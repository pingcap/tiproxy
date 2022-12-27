// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cenkalti/backoff/v4"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

var (
	ErrCloseConnMgr = errors.New("failed to close connection manager")
)

const (
	sqlQueryState    = "SHOW SESSION_STATES"
	sqlSetState      = "SET SESSION_STATES '%s'"
	sessionStatesCol = "Session_states"
	sessionTokenCol  = "Session_token"
	currentDBKey     = "current-db"
)

type signalType int

const (
	signalTypeRedirect signalType = iota
	signalTypeGracefulClose
	signalTypeNums
)

type signalRedirect struct {
	newAddr string
}

type redirectResult struct {
	err  error
	from string
	to   string
}

const (
	statusActive      int32 = iota
	statusNotifyClose       // notified to graceful close
	statusClosing           // really closing
	statusClosed
)

// BackendConnManager migrates a session from one BackendConnection to another.
//
// The signal processing goroutine tries to migrate the session once it receives a signal.
// If the session is not ready at that time, the cmd executing goroutine will try after executing commands.
//
// If redirection fails, it doesn't retry and waits for the next signal, because:
// - If it disconnects immediately: it's even worse than graceful shutdown.
// - If it retries after each command: the latency will be unacceptable afterwards if it always fails.
// - If it stops receiving signals: the previous new backend may be abnormal but the next new backend may be good.
type BackendConnManager struct {
	// processLock makes redirecting and command processing exclusive.
	processLock sync.Mutex
	wg          waitgroup.WaitGroup
	// signalReceived is used to notify the signal processing goroutine.
	signalReceived chan signalType
	authenticator  *Authenticator
	cmdProcessor   *CmdProcessor
	eventReceiver  unsafe.Pointer
	logger         *zap.Logger
	// type *signalRedirect, it saves the last signal if there are multiple signals.
	// It will be set to nil after migration.
	signal unsafe.Pointer
	// redirectResCh is used to notify the event receiver asynchronously.
	redirectResCh chan *redirectResult
	closeStatus   atomic.Int32
	// cancelFunc is used to cancel the signal processing goroutine.
	cancelFunc       context.CancelFunc
	backendConn      *BackendConnection
	handshakeHandler HandshakeHandler
	connectionID     uint64
}

// NewBackendConnManager creates a BackendConnManager.
func NewBackendConnManager(logger *zap.Logger, handshakeHandler HandshakeHandler,
	connectionID uint64, proxyProtocol, requireBackendTLS bool) *BackendConnManager {
	mgr := &BackendConnManager{
		logger:           logger,
		connectionID:     connectionID,
		cmdProcessor:     NewCmdProcessor(),
		handshakeHandler: handshakeHandler,
		authenticator: &Authenticator{
			supportedServerCapabilities: handshakeHandler.GetCapability(),
			proxyProtocol:               proxyProtocol,
			requireBackendTLS:           requireBackendTLS,
			salt:                        GenerateSalt(20),
		},
		// There are 2 types of signals, which may be sent concurrently.
		signalReceived: make(chan signalType, signalTypeNums),
		redirectResCh:  make(chan *redirectResult, 1),
	}
	return mgr
}

// ConnectionID implements RedirectableConn.ConnectionID interface.
// It returns the ID of the frontend connection. The ID stays still after session migration.
func (mgr *BackendConnManager) ConnectionID() uint64 {
	return mgr.connectionID
}

// Connect connects to the first backend and then start watching redirection signals.
func (mgr *BackendConnManager) Connect(ctx context.Context, clientIO *pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	err := mgr.authenticator.handshakeFirstTime(mgr.logger.Named("authenticator"), clientIO, mgr.handshakeHandler, mgr.getBackendIO, frontendTLSConfig, backendTLSConfig)
	mgr.handshakeHandler.OnHandshake(mgr.authenticator, mgr.authenticator.serverAddr, err)
	if err != nil {
		return err
	}

	mgr.cmdProcessor.capability = mgr.authenticator.capability
	childCtx, cancelFunc := context.WithCancel(ctx)
	mgr.cancelFunc = cancelFunc
	mgr.wg.Run(func() {
		mgr.processSignals(childCtx, clientIO)
	})
	return nil
}

func (mgr *BackendConnManager) getBackendIO(ctx ConnContext, auth *Authenticator, resp *pnet.HandshakeResp) (*pnet.PacketIO, error) {
	r, err := mgr.handshakeHandler.GetRouter(auth, resp)
	if err != nil {
		return nil, err
	}
	// wait for initialize
	bctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	addr, err := backoff.RetryNotifyWithData(
		func() (string, error) {
			addr, err := r.Route(mgr)
			if !errors.Is(err, router.ErrNoInstanceToSelect) {
				return addr, backoff.Permanent(err)
			}
			return addr, err
		},
		backoff.WithContext(backoff.NewConstantBackOff(200*time.Millisecond), bctx),
		func(err error, d time.Duration) {
			mgr.handshakeHandler.OnHandshake(auth, "", err)
		},
	)
	cancel()
	if err != nil {
		return nil, err
	}

	mgr.logger.Info("found", zap.String("addr", addr))
	mgr.backendConn = NewBackendConnection(addr)
	if err := mgr.backendConn.Connect(); err != nil {
		mgr.handshakeHandler.OnHandshake(auth, addr, err)
		return nil, err
	}

	auth.serverAddr = addr
	return mgr.backendConn.PacketIO(), nil
}

// ExecuteCmd forwards messages between the client and the backend.
// If it finds that the session is ready for redirection, it migrates the session.
func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	if len(request) < 1 {
		return mysql.ErrMalformPacket
	}
	cmd := request[0]
	startTime := time.Now()
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	switch mgr.closeStatus.Load() {
	case statusClosing, statusClosed:
		return nil
	}
	waitingRedirect := atomic.LoadPointer(&mgr.signal) != nil
	holdRequest, err := mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), waitingRedirect)
	if !holdRequest {
		addCmdMetrics(cmd, mgr.backendConn.Addr(), startTime)
	}
	if err != nil {
		if !IsMySQLError(err) {
			return err
		} else {
			mgr.logger.Debug("got a mysql error", zap.Error(err))
		}
	}
	if err == nil {
		switch cmd {
		case mysql.ComQuit:
			return nil
		case mysql.ComSetOption:
			val := binary.LittleEndian.Uint16(request[1:])
			switch val {
			case 0:
				mgr.authenticator.capability |= mysql.ClientMultiStatements
				mgr.cmdProcessor.capability |= mysql.ClientMultiStatements
			case 1:
				mgr.authenticator.capability &^= mysql.ClientMultiStatements
				mgr.cmdProcessor.capability &^= mysql.ClientMultiStatements
			default:
				return errors.Errorf("unrecognized set_option value:%d", val)
			}
		case mysql.ComChangeUser:
			username, db := pnet.ParseChangeUser(request)
			mgr.authenticator.changeUser(username, db)
			return nil
		}
	}
	// Even if it meets an MySQL error, it may have changed the status, such as when executing multi-statements.
	if mgr.cmdProcessor.finishedTxn() {
		if waitingRedirect && holdRequest {
			mgr.tryRedirect(ctx, clientIO)
			// Execute the held request no matter redirection succeeds or not.
			_, err = mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), false)
			addCmdMetrics(cmd, mgr.backendConn.Addr(), startTime)
			if err != nil && !IsMySQLError(err) {
				return err
			}
		} else if mgr.closeStatus.Load() == statusNotifyClose {
			mgr.tryGracefulClose(ctx, clientIO)
		} else if waitingRedirect {
			mgr.tryRedirect(ctx, clientIO)
		}
	}
	// Ignore MySQL errors, only return unexpected errors.
	return nil
}

// SetEventReceiver implements RedirectableConn.SetEventReceiver interface.
// The receiver sends redirection signals and watches redirecting events.
func (mgr *BackendConnManager) SetEventReceiver(receiver router.ConnEventReceiver) {
	atomic.StorePointer(&mgr.eventReceiver, unsafe.Pointer(&receiver))
}

func (mgr *BackendConnManager) getEventReceiver() router.ConnEventReceiver {
	eventReceiver := (*router.ConnEventReceiver)(atomic.LoadPointer(&mgr.eventReceiver))
	if eventReceiver == nil {
		return nil
	}
	return *eventReceiver
}

func (mgr *BackendConnManager) initSessionStates(backendIO *pnet.PacketIO, sessionStates string) error {
	// Do not lock here because the caller already locks.
	sessionStates = strings.ReplaceAll(sessionStates, "\\", "\\\\")
	sessionStates = strings.ReplaceAll(sessionStates, "'", "\\'")
	sql := fmt.Sprintf(sqlSetState, sessionStates)
	_, _, err := mgr.cmdProcessor.query(backendIO, sql)
	return err
}

func (mgr *BackendConnManager) querySessionStates() (sessionStates, sessionToken string, err error) {
	// Do not lock here because the caller already locks.
	var result *gomysql.Result
	if result, _, err = mgr.cmdProcessor.query(mgr.backendConn.PacketIO(), sqlQueryState); err != nil {
		return
	}
	if sessionStates, err = result.GetStringByName(0, sessionStatesCol); err != nil {
		return
	}
	sessionToken, err = result.GetStringByName(0, sessionTokenCol)
	return
}

// processSignals runs in a goroutine to:
// - Receive redirection signals and then try to migrate the session.
// - Send redirection results to the event receiver.
func (mgr *BackendConnManager) processSignals(ctx context.Context, clientIO *pnet.PacketIO) {
	for {
		select {
		case s := <-mgr.signalReceived:
			// Redirect the session immediately just in case the session is finishedTxn.
			mgr.processLock.Lock()
			switch s {
			case signalTypeGracefulClose:
				mgr.tryGracefulClose(ctx, clientIO)
			case signalTypeRedirect:
				mgr.tryRedirect(ctx, clientIO)
			}
			mgr.processLock.Unlock()
		case rs := <-mgr.redirectResCh:
			mgr.notifyRedirectResult(ctx, rs)
		case <-ctx.Done():
			return
		}
	}
}

// tryRedirect tries to migrate the session if the session is redirect-able.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) tryRedirect(ctx context.Context, clientIO *pnet.PacketIO) {
	switch mgr.closeStatus.Load() {
	case statusNotifyClose, statusClosing, statusClosed:
		return
	}
	signal := (*signalRedirect)(atomic.LoadPointer(&mgr.signal))
	if signal == nil {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}

	rs := &redirectResult{
		from: mgr.backendConn.Addr(),
		to:   signal.newAddr,
	}
	defer func() {
		// The `mgr` won't be notified again before it calls `OnRedirectSucceed`, so simply `StorePointer` is also fine.
		atomic.CompareAndSwapPointer(&mgr.signal, unsafe.Pointer(signal), nil)
		// Notifying may block. Notify the receiver asynchronously to:
		// - Reduce the latency of session migration
		// - Avoid the risk of deadlock
		mgr.redirectResCh <- rs
	}()
	var sessionStates, sessionToken string
	if sessionStates, sessionToken, rs.err = mgr.querySessionStates(); rs.err != nil {
		return
	}
	if rs.err = mgr.updateAuthInfoFromSessionStates(hack.Slice(sessionStates)); rs.err != nil {
		return
	}

	newConn := NewBackendConnection(rs.to)
	if rs.err = newConn.Connect(); rs.err != nil {
		mgr.handshakeHandler.OnHandshake(mgr.authenticator, rs.to, rs.err)
		return
	}
	mgr.authenticator.serverAddr = rs.to
	mgr.authenticator.clientAddr = clientIO.SourceAddr().String()
	if rs.err = mgr.authenticator.handshakeSecondTime(mgr.logger, clientIO, newConn.PacketIO(), sessionToken); rs.err == nil {
		rs.err = mgr.initSessionStates(newConn.PacketIO(), sessionStates)
	} else {
		mgr.handshakeHandler.OnHandshake(mgr.authenticator, mgr.authenticator.serverAddr, rs.err)
	}
	if rs.err != nil {
		if ignoredErr := newConn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			mgr.logger.Error("close new backend connection failed", zap.Error(ignoredErr))
		}
		return
	}
	if ignoredErr := mgr.backendConn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
		mgr.logger.Error("close previous backend connection failed", zap.Error(ignoredErr))
	}
	mgr.backendConn = newConn
	mgr.handshakeHandler.OnHandshake(mgr.authenticator, mgr.authenticator.serverAddr, nil)
}

// The original db in the auth info may be dropped during the session, so we need to authenticate with the current db.
// The user may be renamed during the session, but the session cannot detect it, so this will affect the user.
// TODO: this may be a security problem: a different new user may just be renamed to this user name.
func (mgr *BackendConnManager) updateAuthInfoFromSessionStates(sessionStates []byte) error {
	var statesMap map[string]any
	if err := json.Unmarshal(sessionStates, &statesMap); err != nil {
		return errors.Wrapf(err, "unmarshal session states error")
	}
	// The currentDBKey may be omitted if it's empty. In this case, we still need to update it.
	if currentDB, ok := statesMap[currentDBKey].(string); ok {
		mgr.authenticator.updateCurrentDB(currentDB)
	}
	return nil
}

// Redirect implements RedirectableConn.Redirect interface. It redirects the current session to the newAddr.
// Note that the caller requires the function to be non-blocking.
func (mgr *BackendConnManager) Redirect(newAddr string) {
	// NOTE: BackendConnManager may be closing concurrently because of no lock.
	// The eventReceiver may read the new address even after BackendConnManager is closed.
	atomic.StorePointer(&mgr.signal, unsafe.Pointer(&signalRedirect{
		newAddr: newAddr,
	}))
	switch mgr.closeStatus.Load() {
	case statusNotifyClose, statusClosing, statusClosed:
		return
	}
	// Generally, it won't wait because the caller won't send another signal before the previous one finishes.
	mgr.signalReceived <- signalTypeRedirect
}

// GetRedirectingAddr implements RedirectableConn.GetRedirectingAddr interface.
// It returns the goal backend address to redirect to.
func (mgr *BackendConnManager) GetRedirectingAddr() string {
	signal := (*signalRedirect)(atomic.LoadPointer(&mgr.signal))
	if signal == nil {
		return ""
	}
	return signal.newAddr
}

func (mgr *BackendConnManager) notifyRedirectResult(ctx context.Context, rs *redirectResult) {
	if rs == nil {
		return
	}
	eventReceiver := mgr.getEventReceiver()
	if eventReceiver == nil {
		return
	}
	if rs.err != nil {
		err := eventReceiver.OnRedirectFail(rs.from, rs.to, mgr)
		mgr.logger.Warn("redirect connection failed", zap.String("from", rs.from),
			zap.String("to", rs.to), zap.NamedError("redirect_err", rs.err), zap.NamedError("notify_err", err))
	} else {
		err := eventReceiver.OnRedirectSucceed(rs.from, rs.to, mgr)
		mgr.logger.Info("redirect connection succeeds", zap.String("from", rs.from),
			zap.String("to", rs.to), zap.NamedError("notify_err", err))
	}
}

// GracefulClose waits for the end of the transaction and closes the session.
func (mgr *BackendConnManager) GracefulClose() {
	mgr.closeStatus.Store(statusNotifyClose)
	mgr.signalReceived <- signalTypeGracefulClose
}

func (mgr *BackendConnManager) tryGracefulClose(ctx context.Context, clientIO *pnet.PacketIO) {
	if mgr.closeStatus.Load() != statusNotifyClose {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}
	// Closing clientIO will cause the whole connection to be closed.
	if err := clientIO.GracefulClose(); err != nil {
		mgr.logger.Warn("graceful close client IO error", zap.Stringer("addr", clientIO.SourceAddr()), zap.Error(err))
	}
	mgr.closeStatus.Store(statusClosing)
}

// Close releases all resources.
func (mgr *BackendConnManager) Close() error {
	mgr.closeStatus.Store(statusClosing)
	if mgr.cancelFunc != nil {
		mgr.cancelFunc()
		mgr.cancelFunc = nil
	}
	mgr.wg.Wait()

	var connErr error
	var addr string
	mgr.processLock.Lock()
	if mgr.backendConn != nil {
		addr = mgr.backendConn.address
		connErr = mgr.backendConn.Close()
		mgr.backendConn = nil
	}
	mgr.processLock.Unlock()

	handErr := mgr.handshakeHandler.OnConnClose(mgr.authenticator)

	eventReceiver := mgr.getEventReceiver()
	if eventReceiver != nil {
		// Notify the receiver if there's any event.
		if len(mgr.redirectResCh) > 0 {
			mgr.notifyRedirectResult(context.Background(), <-mgr.redirectResCh)
		}
		// Just notify it with the current address.
		if len(addr) > 0 {
			if err := eventReceiver.OnConnClosed(addr, mgr); err != nil {
				mgr.logger.Error("close connection error", zap.String("addr", addr), zap.NamedError("notify_err", err))
			}
		}
	}
	mgr.closeStatus.Store(statusClosed)
	return errors.Collect(ErrCloseConnMgr, connErr, handErr)
}
