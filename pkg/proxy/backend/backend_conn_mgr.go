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
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cenkalti/backoff/v4"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/TiProxy/lib/config"
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
	DialTimeout          = 1 * time.Second
	CheckBackendInterval = time.Minute
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

type BCConfig struct {
	ProxyProtocol        bool
	RequireBackendTLS    bool
	CheckBackendInterval time.Duration
	HealthyKeepAlive     config.KeepAlive
	UnhealthyKeepAlive   config.KeepAlive
}

func (cfg *BCConfig) check() {
	if cfg.CheckBackendInterval == time.Duration(0) {
		cfg.CheckBackendInterval = CheckBackendInterval
	}
}

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
	config         *BCConfig
	logger         *zap.Logger
	// type *signalRedirect, it saves the last signal if there are multiple signals.
	// It will be set to nil after migration.
	signal unsafe.Pointer
	// redirectResCh is used to notify the event receiver asynchronously.
	redirectResCh      chan *redirectResult
	closeStatus        atomic.Int32
	checkBackendTicker *time.Ticker
	// cancelFunc is used to cancel the signal processing goroutine.
	cancelFunc context.CancelFunc
	clientIO   *pnet.PacketIO
	// backendIO may be written during redirection and be read in ExecuteCmd/Redirect/setKeepalive.
	backendIO        atomic.Pointer[pnet.PacketIO]
	backendTLS       *tls.Config
	handshakeHandler HandshakeHandler
	ctxmap           sync.Map
	connectionID     uint64
	quitSource       ErrorSource
}

// NewBackendConnManager creates a BackendConnManager.
func NewBackendConnManager(logger *zap.Logger, handshakeHandler HandshakeHandler, connectionID uint64, config *BCConfig) *BackendConnManager {
	config.check()
	mgr := &BackendConnManager{
		logger:           logger,
		config:           config,
		connectionID:     connectionID,
		cmdProcessor:     NewCmdProcessor(),
		handshakeHandler: handshakeHandler,
		authenticator: &Authenticator{
			proxyProtocol:     config.ProxyProtocol,
			requireBackendTLS: config.RequireBackendTLS,
			salt:              GenerateSalt(20),
		},
		// There are 2 types of signals, which may be sent concurrently.
		signalReceived: make(chan signalType, signalTypeNums),
		redirectResCh:  make(chan *redirectResult, 1),
		quitSource:     SrcClientQuit,
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

	mgr.backendTLS = backendTLSConfig

	mgr.clientIO = clientIO
	err := mgr.authenticator.handshakeFirstTime(mgr.logger.Named("authenticator"), mgr, clientIO, mgr.handshakeHandler, mgr.getBackendIO, frontendTLSConfig, backendTLSConfig)
	if err != nil {
		mgr.setQuitSourceByErr(err)
		mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), err)
		WriteUserError(clientIO, err, mgr.logger)
		return err
	}
	mgr.resetQuitSource()
	mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), nil)

	mgr.cmdProcessor.capability = mgr.authenticator.capability
	childCtx, cancelFunc := context.WithCancel(ctx)
	mgr.cancelFunc = cancelFunc
	mgr.resetCheckBackendTicker()
	mgr.wg.Run(func() {
		mgr.processSignals(childCtx)
	})
	return nil
}

func (mgr *BackendConnManager) getBackendIO(cctx ConnContext, auth *Authenticator, resp *pnet.HandshakeResp, timeout time.Duration) (*pnet.PacketIO, error) {
	r, err := mgr.handshakeHandler.GetRouter(cctx, resp)
	if err != nil {
		return nil, WrapUserError(err, err.Error())
	}
	// Reasons to wait:
	// - The TiDB instances may not be initialized yet
	// - One TiDB may be just shut down and another is just started but not ready yet
	bctx, cancel := context.WithTimeout(context.Background(), timeout)
	selector := r.GetBackendSelector()
	startTime := time.Now()
	var addr string
	var origErr error
	io, err := backoff.RetryNotifyWithData(
		func() (*pnet.PacketIO, error) {
			// Try to connect to all backup backends one by one.
			addr, err = selector.Next()
			// If all addrs are enumerated, reset and try again.
			if err == nil && addr == "" {
				selector.Reset()
				addr, err = selector.Next()
			}
			if err != nil {
				return nil, backoff.Permanent(WrapUserError(err, err.Error()))
			}
			if addr == "" {
				return nil, router.ErrNoInstanceToSelect
			}

			var cn net.Conn
			cn, err = net.DialTimeout("tcp", addr, DialTimeout)
			if err != nil {
				return nil, errors.Wrapf(err, "dial backend %s error", addr)
			}

			if err = selector.Succeed(mgr); err != nil {
				// Bad luck: the backend has been recycled or shut down just after the selector returns it.
				if ignoredErr := cn.Close(); ignoredErr != nil {
					mgr.logger.Error("close backend connection failed", zap.String("addr", addr), zap.Error(ignoredErr))
				}
				return nil, err
			}

			mgr.logger.Info("connected to backend", zap.String("addr", addr))
			// NOTE: should use DNS name as much as possible
			// Usually certs are signed with domain instead of IP addrs
			// And `RemoteAddr()` will return IP addr
			backendIO := pnet.NewPacketIO(cn, pnet.WithRemoteAddr(addr, cn.RemoteAddr()), pnet.WithWrapError(ErrBackendConn))
			mgr.backendIO.Store(backendIO)
			mgr.setKeepAlive(mgr.config.HealthyKeepAlive)
			return backendIO, nil
		},
		backoff.WithContext(backoff.NewConstantBackOff(200*time.Millisecond), bctx),
		func(err error, d time.Duration) {
			origErr = err
			mgr.setQuitSourceByErr(err)
			mgr.handshakeHandler.OnHandshake(cctx, addr, err)
		},
	)
	cancel()

	duration := time.Since(startTime)
	addGetBackendMetrics(duration)
	if err != nil {
		mgr.logger.Error("get backend failed", zap.Duration("duration", duration), zap.NamedError("last_err", origErr))
	} else if duration >= 3*time.Second {
		mgr.logger.Warn("get backend slow", zap.Duration("duration", duration), zap.NamedError("last_err", origErr),
			zap.String("backend_addr", mgr.ServerAddr()))
	}
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		if origErr != nil {
			err = origErr
		}
	}
	return io, err
}

// ExecuteCmd forwards messages between the client and the backend.
// If it finds that the session is ready for redirection, it migrates the session.
func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte) (err error) {
	defer func() {
		mgr.setQuitSourceByErr(err)
	}()
	if len(request) < 1 {
		err = mysql.ErrMalformPacket
		return
	}
	cmd := request[0]
	startTime := time.Now()
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	switch mgr.closeStatus.Load() {
	case statusClosing, statusClosed:
		return
	}
	defer mgr.resetCheckBackendTicker()
	waitingRedirect := atomic.LoadPointer(&mgr.signal) != nil
	var holdRequest bool
	holdRequest, err = mgr.cmdProcessor.executeCmd(request, mgr.clientIO, mgr.backendIO.Load(), waitingRedirect)
	if !holdRequest {
		addCmdMetrics(cmd, mgr.ServerAddr(), startTime)
	}
	if err != nil {
		if !IsMySQLError(err) {
			return
		} else {
			mgr.logger.Debug("got a mysql error", zap.Error(err))
		}
	}
	if err == nil {
		switch cmd {
		case mysql.ComQuit:
			return
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
				err = errors.Errorf("unrecognized set_option value:%d", val)
				return
			}
		case mysql.ComChangeUser:
			username, db := pnet.ParseChangeUser(request)
			mgr.authenticator.changeUser(username, db)
			return
		}
	}
	// Even if it meets an MySQL error, it may have changed the status, such as when executing multi-statements.
	if mgr.cmdProcessor.finishedTxn() {
		if waitingRedirect && holdRequest {
			mgr.tryRedirect(ctx)
			// Execute the held request no matter redirection succeeds or not.
			_, err = mgr.cmdProcessor.executeCmd(request, mgr.clientIO, mgr.backendIO.Load(), false)
			addCmdMetrics(cmd, mgr.ServerAddr(), startTime)
			if err != nil && !IsMySQLError(err) {
				return
			}
		} else if mgr.closeStatus.Load() == statusNotifyClose {
			mgr.tryGracefulClose(ctx)
		} else if waitingRedirect {
			mgr.tryRedirect(ctx)
		}
	}
	// Ignore MySQL errors, only return unexpected errors.
	err = nil
	return
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

func (mgr *BackendConnManager) querySessionStates(backendIO *pnet.PacketIO) (sessionStates, sessionToken string, err error) {
	// Do not lock here because the caller already locks.
	var result *gomysql.Result
	if result, _, err = mgr.cmdProcessor.query(backendIO, sqlQueryState); err != nil {
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
// - Check if the backend is still alive.
func (mgr *BackendConnManager) processSignals(ctx context.Context) {
	for {
		select {
		case s := <-mgr.signalReceived:
			// Redirect the session immediately just in case the session is finishedTxn.
			mgr.processLock.Lock()
			switch s {
			case signalTypeGracefulClose:
				mgr.tryGracefulClose(ctx)
			case signalTypeRedirect:
				mgr.tryRedirect(ctx)
			}
			mgr.processLock.Unlock()
		case rs := <-mgr.redirectResCh:
			mgr.notifyRedirectResult(ctx, rs)
		case <-mgr.checkBackendTicker.C:
			mgr.checkBackendActive()
		case <-ctx.Done():
			return
		}
	}
}

// tryRedirect tries to migrate the session if the session is redirect-able.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) tryRedirect(ctx context.Context) {
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
		from: mgr.ServerAddr(),
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
	backendIO := mgr.backendIO.Load()
	var sessionStates, sessionToken string
	if sessionStates, sessionToken, rs.err = mgr.querySessionStates(backendIO); rs.err != nil {
		// If the backend connection is closed, also close the client connection.
		// Otherwise, if the client is idle, the mgr will keep retrying.
		if errors.Is(rs.err, net.ErrClosed) || pnet.IsDisconnectError(rs.err) || errors.Is(rs.err, os.ErrDeadlineExceeded) {
			mgr.quitSource = SrcBackendQuit
			if ignoredErr := mgr.clientIO.GracefulClose(); ignoredErr != nil {
				mgr.logger.Warn("graceful close client IO error", zap.Stringer("addr", mgr.clientIO.RemoteAddr()), zap.Error(ignoredErr))
			}
		}
		return
	}
	if rs.err = mgr.updateAuthInfoFromSessionStates(hack.Slice(sessionStates)); rs.err != nil {
		return
	}

	defer mgr.resetQuitSource()
	var cn net.Conn
	cn, rs.err = net.DialTimeout("tcp", rs.to, DialTimeout)
	if rs.err != nil {
		mgr.quitSource = SrcBackendQuit
		mgr.handshakeHandler.OnHandshake(mgr, rs.to, rs.err)
		return
	}
	newBackendIO := pnet.NewPacketIO(cn, pnet.WithRemoteAddr(rs.to, cn.RemoteAddr()), pnet.WithWrapError(ErrBackendConn))

	if rs.err = mgr.authenticator.handshakeSecondTime(mgr.logger, mgr.clientIO, newBackendIO, mgr.backendTLS, sessionToken); rs.err == nil {
		rs.err = mgr.initSessionStates(newBackendIO, sessionStates)
	} else {
		mgr.setQuitSourceByErr(rs.err)
		mgr.handshakeHandler.OnHandshake(mgr, newBackendIO.RemoteAddr().String(), rs.err)
	}
	if rs.err != nil {
		if ignoredErr := newBackendIO.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			mgr.logger.Error("close new backend connection failed", zap.Error(ignoredErr))
		}
		return
	}
	if ignoredErr := backendIO.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
		mgr.logger.Error("close previous backend connection failed", zap.Error(ignoredErr))
	}
	mgr.backendIO.Store(newBackendIO)
	mgr.setKeepAlive(mgr.config.HealthyKeepAlive)
	mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), nil)
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

func (mgr *BackendConnManager) tryGracefulClose(ctx context.Context) {
	if mgr.closeStatus.Load() != statusNotifyClose {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}
	mgr.quitSource = SrcProxyQuit
	// Closing clientIO will cause the whole connection to be closed.
	if err := mgr.clientIO.GracefulClose(); err != nil {
		mgr.logger.Warn("graceful close client IO error", zap.Stringer("addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
	}
	mgr.closeStatus.Store(statusClosing)
}

func (mgr *BackendConnManager) checkBackendActive() {
	switch mgr.closeStatus.Load() {
	case statusClosing, statusClosed:
		return
	}

	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	backendIO := mgr.backendIO.Load()
	if !backendIO.IsPeerActive() {
		mgr.logger.Info("backend connection is closed, close client connection", zap.Stringer("client", mgr.clientIO.RemoteAddr()),
			zap.Stringer("backend", backendIO.RemoteAddr()))
		mgr.quitSource = SrcBackendQuit
		if err := mgr.clientIO.GracefulClose(); err != nil {
			mgr.logger.Warn("graceful close client IO error", zap.Stringer("addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
		}
		mgr.closeStatus.Store(statusClosing)
	}
}

// Checking backend is expensive, so only check it when the client is idle for some time.
// This function should be called within the lock.
func (mgr *BackendConnManager) resetCheckBackendTicker() {
	if mgr.checkBackendTicker == nil {
		mgr.checkBackendTicker = time.NewTicker(mgr.config.CheckBackendInterval)
	} else {
		mgr.checkBackendTicker.Reset(mgr.config.CheckBackendInterval)
	}
}

func (mgr *BackendConnManager) ClientAddr() string {
	if mgr.clientIO == nil {
		return ""
	}
	return mgr.clientIO.RemoteAddr().String()
}

func (mgr *BackendConnManager) ServerAddr() string {
	if backendIO := mgr.backendIO.Load(); backendIO != nil {
		return backendIO.RemoteAddr().String()
	}
	return ""
}

func (mgr *BackendConnManager) ClientInBytes() uint64 {
	if mgr.clientIO == nil {
		return 0
	}
	return mgr.clientIO.InBytes()
}

func (mgr *BackendConnManager) ClientOutBytes() uint64 {
	if mgr.clientIO == nil {
		return 0
	}
	return mgr.clientIO.OutBytes()
}

func (mgr *BackendConnManager) QuitSource() ErrorSource {
	return mgr.quitSource
}

func (mgr *BackendConnManager) SetValue(key, val any) {
	mgr.ctxmap.Store(key, val)
}

func (mgr *BackendConnManager) Value(key any) any {
	v, ok := mgr.ctxmap.Load(key)
	if !ok {
		return nil
	}
	return v
}

// Close releases all resources.
func (mgr *BackendConnManager) Close() error {
	mgr.closeStatus.Store(statusClosing)
	if mgr.checkBackendTicker != nil {
		mgr.checkBackendTicker.Stop()
	}
	if mgr.cancelFunc != nil {
		mgr.cancelFunc()
		mgr.cancelFunc = nil
	}
	mgr.wg.Wait()

	var connErr error
	var addr string
	mgr.processLock.Lock()
	if backendIO := mgr.backendIO.Swap(nil); backendIO != nil {
		addr = backendIO.RemoteAddr().String()
		connErr = backendIO.Close()
	}
	mgr.processLock.Unlock()

	handErr := mgr.handshakeHandler.OnConnClose(mgr)

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

// NotifyBackendStatus notifies the backend status to mgr.
// The request to the unhealthy backend may block sometimes, instead of fail immediately.
// So we set a shorter keep alive timeout for the unhealthy backends.
func (mgr *BackendConnManager) NotifyBackendStatus(status router.BackendStatus) {
	switch status {
	case router.StatusHealthy:
		mgr.setKeepAlive(mgr.config.HealthyKeepAlive)
	default:
		mgr.setKeepAlive(mgr.config.UnhealthyKeepAlive)
	}
}

func (mgr *BackendConnManager) setKeepAlive(cfg config.KeepAlive) {
	backendIO := mgr.backendIO.Load()
	if backendIO == nil {
		return
	}
	if err := backendIO.SetKeepalive(cfg); err != nil {
		mgr.logger.Warn("failed to set keepalive", zap.Error(err), zap.Stringer("backend", backendIO.RemoteAddr()))
	}
}

// quitSource will be read by OnHandshake and OnConnClose, so setQuitSourceByErr should be called before them.
func (mgr *BackendConnManager) setQuitSourceByErr(err error) {
	// Do not update the source if err is nil. It may be already be set.
	if err == nil {
		return
	}
	if errors.Is(err, ErrBackendConn) {
		mgr.quitSource = SrcBackendQuit
	} else if IsMySQLError(err) {
		mgr.quitSource = SrcClientErr
	} else if !errors.Is(err, ErrClientConn) {
		mgr.quitSource = SrcProxyErr
	}
}

func (mgr *BackendConnManager) resetQuitSource() {
	// SrcClientQuit is by default.
	// Sometimes ErrClientConn is caused by GracefulClose and the quitSource is already set.
	// Error maybe set during handshake for OnHandshake. If handshake finally succeeds, we reset it.
	mgr.quitSource = SrcClientQuit
}
