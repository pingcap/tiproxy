package backend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/djshow832/weir/pkg/proxy/driver"
	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	InitBackend = iota
	Command
	WaitFinish
	Redirect
	Disconnect
)

const (
	StatusInTrans            uint32 = 0x01
	StatusAutoCommit         uint32 = 0x02
	StatusPrepareWaitExecute uint32 = 0x04
	StatusPrepareWaitFetch   uint32 = 0x08
)

const (
	SQLQueryState = "SHOW SESSION_STATES"
	SQLSetState   = "SET SESSION_STATES '%s'"
)

type signalRedirect struct {
	newAddr string
}

type BackendConnManager struct {
	authenticator *Authenticator
	querier       *Querier
	backendConn   BackendConnection
	connPhase     uint32
	serverStatus  uint32
	processLock   sync.Mutex
	statusChanged chan int
	signalLock    sync.Mutex
	signal        *signalRedirect
}

func NewBackendConnManager() driver.BackendConnManager {
	return &BackendConnManager{
		connPhase:     InitBackend,
		serverStatus:  StatusAutoCommit,
		authenticator: &Authenticator{},
		statusChanged: make(chan int),
	}
}

func (mgr *BackendConnManager) Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO, tlsConfig *tls.Config) error {
	if mgr.backendConn != nil {
		return errors.New("a backend connection already exists before connecting")
	}
	mgr.backendConn = NewBackendConnectionImpl(serverAddr)
	err := mgr.backendConn.Connect()
	if err != nil {
		return err
	}
	backendIO := mgr.backendConn.PacketIO()
	succeed, err := mgr.authenticator.handshakeWithClient(ctx, clientIO, backendIO, tlsConfig)
	if err != nil {
		return err
	} else if !succeed {
		return errors.New("server returns auth failure")
	}
	mgr.connPhase = Command
	mgr.querier = &Querier{
		capability: mgr.authenticator.capability,
	}
	go mgr.processSignals(ctx)
	return nil
}

func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	backendIO := mgr.backendConn.PacketIO()
	backendIO.ResetSequence()
	err := backendIO.WritePacket(request)
	if err != nil {
		return nil
	}
	err = backendIO.Flush()
	if err != nil {
		return err
	}
	cmd := request[0]
	switch cmd {
	case mysql.ComQuery:
		err = mgr.forwardCommand(ctx, clientIO, backendIO, 2)
	case mysql.ComQuit:
		mgr.connPhase = Disconnect
		return nil
	default:
		err = mgr.forwardCommand(ctx, clientIO, backendIO, 1)
	}
	return err
}

func (mgr *BackendConnManager) forwardCommand(ctx context.Context, clientIO, backendIO *pnet.PacketIO, expectedEOFNum int) error {
	eofHeaders := 0
	okOrErr := false
	for eofHeaders < expectedEOFNum && !okOrErr {
		data, err := backendIO.ReadPacket()
		if err != nil {
			return err
		}
		switch data[0] {
		case mysql.OKHeader:
			mgr.handleOKPacket(data)
			okOrErr = true
		case mysql.ErrHeader:
			okOrErr = true
		case mysql.EOFHeader:
			if len(data) <= 5 {
				mgr.handleEOFPacket(data)
				eofHeaders += 1
			}
		}
		err = clientIO.WritePacket(data)
		if err != nil {
			return err
		}
	}
	return clientIO.Flush()
}

func (mgr *BackendConnManager) handleOKPacket(data []byte) {
}

func (mgr *BackendConnManager) handleEOFPacket(data []byte) {
}

func (mgr *BackendConnManager) initSessionStates(sessionStates string) error {
	// Do not lock here because the caller already locks.
	sessionStates = strings.ReplaceAll(sessionStates, "\\", "\\\\")
	sessionStates = strings.ReplaceAll(sessionStates, "'", "\\'")
	sql := fmt.Sprintf(SQLSetState, sessionStates)
	_, err := mgr.querier.Query(mgr.backendConn.PacketIO(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *BackendConnManager) querySessionStates() (string, error) {
	// Do not lock here because the caller already locks.
	result, err := mgr.querier.Query(mgr.backendConn.PacketIO(), SQLQueryState)
	if err != nil {
		return "", err
	}
	return result.GetString(0, 0)
}

func (mgr *BackendConnManager) disconnect() error {
	return nil
}

func (mgr *BackendConnManager) processSignals(ctx context.Context) {
	finished := false
	for !finished {
		select {
		case <-mgr.statusChanged:
			mgr.signalLock.Lock()
			signal := mgr.signal
			mgr.signalLock.Unlock()
			if signal == nil {
				break
			}
			if err := mgr.tryProcessSignal(ctx, signal); err != nil {
				// TODO: retry / close the client connection
				logutil.Logger(ctx).Error("redirect connection failed", zap.Error(err))
			}
			mgr.signalLock.Lock()
			// Check mgr.signal because it may be updated again.
			if mgr.signal != nil && mgr.signal.newAddr == signal.newAddr {
				mgr.signal = nil
			}
			mgr.signalLock.Unlock()
		case <-ctx.Done():
			finished = true
		}
	}
}

func (mgr *BackendConnManager) tryProcessSignal(ctx context.Context, signal *signalRedirect) (err error) {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	var sessionStates string
	switch mgr.connPhase {
	case Disconnect:
		return
	case WaitFinish:
		// wait for new status
		return
	case InitBackend, Redirect:
		// not possible here
		return
	case Command:
		if mgr.serverStatus&StatusInTrans > 0 || mgr.serverStatus&StatusPrepareWaitExecute > 0 || mgr.serverStatus&StatusPrepareWaitFetch > 0 {
			mgr.connPhase = WaitFinish
			return
		}
		mgr.connPhase = Redirect
		if sessionStates, err = mgr.querySessionStates(); err != nil {
			return
		}
	}

	newConn := NewBackendConnectionImpl(signal.newAddr)
	if err = newConn.Connect(); err != nil {
		mgr.connPhase = Command
		return
	}
	// Retrial may be needed in the future.
	if err = mgr.authenticator.handshakeWithServer(ctx, newConn.PacketIO()); err != nil {
		return
	}
	if err := mgr.backendConn.Close(); err != nil {
		logutil.Logger(ctx).Warn("close previous backend connection failed", zap.Error(err))
	}
	mgr.backendConn = newConn
	if err = mgr.initSessionStates(sessionStates); err != nil {
		return
	}
	logutil.Logger(ctx).Info("redirect connection succeeds", zap.String("to", signal.newAddr))
	mgr.connPhase = Command
	return
}

func (mgr *BackendConnManager) Redirect(newAddr string) error {
	mgr.signalLock.Lock()
	mgr.signal = &signalRedirect{
		newAddr: newAddr,
	}
	mgr.signalLock.Unlock()
	select {
	case mgr.statusChanged <- 1:
	default:
	}
	return nil
}

func (mgr *BackendConnManager) Close() error {
	return mgr.backendConn.Close()
}
