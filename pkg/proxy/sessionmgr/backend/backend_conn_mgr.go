package backend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/djshow832/weir/pkg/proxy/driver"
	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

const (
	SQLQueryState = "SHOW SESSION_STATES"
	SQLSetState   = "SET SESSION_STATES '%s'"
)

type signalRedirect struct {
	newAddr string
}

type BackendConnManager struct {
	connectionID   uint64
	authenticator  *Authenticator
	cmdProcessor   *CmdProcessor
	eventReceiver  driver.ConnEventReceiver
	backendConn    BackendConnection
	processLock    sync.Mutex // to make redirecting and command processing exclusive
	signalReceived chan struct{}
	signal         unsafe.Pointer // type *signalRedirect
	cancelFunc     context.CancelFunc
}

func NewBackendConnManager(connectionID uint64) driver.BackendConnManager {
	return &BackendConnManager{
		connectionID:   connectionID,
		cmdProcessor:   NewCmdProcessor(),
		authenticator:  &Authenticator{},
		signalReceived: make(chan struct{}),
	}
}

func (mgr *BackendConnManager) ConnectionID() uint64 {
	return mgr.connectionID
}

func (mgr *BackendConnManager) Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	mgr.backendConn = NewBackendConnectionImpl(serverAddr)
	if err := mgr.backendConn.Connect(); err != nil {
		return err
	}
	backendIO := mgr.backendConn.PacketIO()
	succeed, err := mgr.authenticator.handshakeFirstTime(clientIO, backendIO, serverTLSConfig, backendTLSConfig)
	if err != nil {
		return err
	} else if !succeed {
		return errors.New("server returns auth failure")
	}
	if mgr.authenticator.capability&mysql.ClientProtocol41 == 0 {
		return errors.New("client must support CLIENT_PROTOCOL_41 capability")
	}
	childCtx, cancelFunc := context.WithCancel(ctx)
	go mgr.processSignals(childCtx)
	mgr.cancelFunc = cancelFunc
	return nil
}

func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	waitingRedirect := atomic.LoadPointer(&mgr.signal) != nil
	holdRequest, succeed, err := mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), waitingRedirect)
	if err != nil {
		return err
	}
	switch request[0] {
	case mysql.ComQuit:
		return nil
	case mysql.ComChangeUser:
		if succeed {
			mgr.authenticator.changeUser(request)
		}
		return nil
	}
	if waitingRedirect && mgr.cmdProcessor.canRedirect() {
		if err = mgr.tryRedirect(ctx); err != nil {
			return err
		}
		if holdRequest {
			_, _, err = mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), false)
		}
	}
	return err
}

func (mgr *BackendConnManager) SetEventReceiver(receiver driver.ConnEventReceiver) {
	mgr.eventReceiver = receiver
}

func (mgr *BackendConnManager) initSessionStates(backendIO *pnet.PacketIO, sessionStates string) error {
	// Do not lock here because the caller already locks.
	sessionStates = strings.ReplaceAll(sessionStates, "\\", "\\\\")
	sessionStates = strings.ReplaceAll(sessionStates, "'", "\\'")
	sql := fmt.Sprintf(SQLSetState, sessionStates)
	_, _, err := mgr.cmdProcessor.query(backendIO, sql)
	return err
}

func (mgr *BackendConnManager) querySessionStates() (sessionStates, sessionToken string, err error) {
	// Do not lock here because the caller already locks.
	var result *gomysql.Result
	if result, _, err = mgr.cmdProcessor.query(mgr.backendConn.PacketIO(), SQLQueryState); err != nil {
		return
	}
	if sessionStates, err = result.GetStringByName(0, "Session_states"); err != nil {
		return
	}
	sessionToken, err = result.GetStringByName(0, "Session_token")
	return
}

func (mgr *BackendConnManager) processSignals(ctx context.Context) {
	for {
		select {
		// Redirect the session immediately just in case the session is idle.
		case _, ok := <-mgr.signalReceived:
			if !ok {
				return
			}
			mgr.processLock.Lock()
			if err := mgr.tryRedirect(ctx); err != nil {
				logutil.Logger(ctx).Error("redirect connection failed", zap.Error(err))
			}
			mgr.processLock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// processLock must be held on this function.
func (mgr *BackendConnManager) tryRedirect(ctx context.Context) (err error) {
	signal := (*signalRedirect)(atomic.LoadPointer(&mgr.signal))
	if signal == nil {
		return nil
	}

	if !mgr.cmdProcessor.canRedirect() {
		return
	}
	from := mgr.backendConn.Addr()
	to := signal.newAddr

	defer func() {
		if err != nil {
			mgr.eventReceiver.OnRedirectFail(from, to, mgr)
			logutil.Logger(ctx).Error("redirect connection failed", zap.String("to", signal.newAddr), zap.Error(err))
		} else {
			mgr.eventReceiver.OnRedirectSucceed(from, to, mgr)
			logutil.Logger(ctx).Info("redirect connection succeeds", zap.String("to", signal.newAddr))
		}
	}()

	var sessionStates, sessionToken string
	if sessionStates, sessionToken, err = mgr.querySessionStates(); err != nil {
		return
	}

	newConn := NewBackendConnectionImpl(to)
	if err = newConn.Connect(); err != nil {
		return
	}
	// Retrial may be needed in the future.
	if err = mgr.authenticator.handshakeSecondTime(newConn.PacketIO(), sessionToken); err != nil {
		if ignoredErr := newConn.Close(); ignoredErr != nil {
			logutil.Logger(ctx).Warn("close new backend connection failed", zap.Error(ignoredErr))
		}
		return
	}
	if err = mgr.initSessionStates(newConn.PacketIO(), sessionStates); err != nil {
		if ignoredErr := newConn.Close(); ignoredErr != nil {
			logutil.Logger(ctx).Warn("close new backend connection failed", zap.Error(ignoredErr))
		}
		return
	}
	if ignoredErr := mgr.backendConn.Close(); ignoredErr != nil {
		logutil.Logger(ctx).Warn("close previous backend connection failed", zap.Error(ignoredErr))
	}
	mgr.backendConn = newConn
	// The `mgr` won't be notified again before it calls `OnRedirectSucceed`, so simply `StorePointer` is also fine.
	atomic.CompareAndSwapPointer(&mgr.signal, unsafe.Pointer(signal), nil)
	return
}

func (mgr *BackendConnManager) Redirect(newAddr string) {
	// We do not use `chan signalRedirect` to avoid blocking. We cannot discard the signal when it blocks,
	// because only the latest signal matters.
	atomic.StorePointer(&mgr.signal, unsafe.Pointer(&signalRedirect{
		newAddr: newAddr,
	}))
	logutil.BgLogger().Info("received redirect command", zap.String("to", newAddr))
	select {
	case mgr.signalReceived <- struct{}{}:
	default:
	}
}

func (mgr *BackendConnManager) Close() error {
	if mgr.cancelFunc != nil {
		mgr.cancelFunc()
		mgr.cancelFunc = nil
	}
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	var err error
	if mgr.eventReceiver != nil {
		// Always notify the eventReceiver with the latest address.
		signal := (*signalRedirect)(atomic.LoadPointer(&mgr.signal))
		if signal != nil {
			mgr.eventReceiver.OnConnClosed(signal.newAddr, mgr)
		} else if mgr.backendConn != nil {
			mgr.eventReceiver.OnConnClosed(mgr.backendConn.Addr(), mgr)
		}
	}
	if mgr.backendConn != nil {
		err = mgr.backendConn.Close()
		mgr.backendConn = nil
	}
	return err
}
