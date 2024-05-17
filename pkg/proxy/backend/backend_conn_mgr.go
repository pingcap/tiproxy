// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/router"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

var (
	ErrCloseConnMgr    = errors.New("failed to close connection manager")
	ErrTargetUnhealthy = errors.New("target backend becomes unhealthy")
)

const (
	// DialTimeout is the timeout for each dial.
	DialTimeout = 1 * time.Second
	// ConnectTimeout is the timeout for choosing and connecting to an available backend.
	ConnectTimeout = 15 * time.Second
	// CheckBackendInterval is the interval for checking if the backend is still connected.
	CheckBackendInterval = time.Minute
	// TickerInterval is the interval for checking backend status.
	TickerInterval = 5 * time.Second
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
	signalTypeSaveSession
)

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

// Default values for ExponentialBackOff
// See https://pkg.go.dev/github.com/cenkalti/backoff/v4
const (
	defaultExponentialBackOffInitialInterval     = 100 * time.Millisecond
	defaultExponentialBackOffRandomizationFactor = 0.5
	defaultExponentialBackOffMultiplier          = 2
	defaultExponentionBackOffMaxInterval         = 4 * time.Second
)

type BCConfig struct {
	HealthyKeepAlive     config.KeepAlive
	UnhealthyKeepAlive   config.KeepAlive
	TickerInterval       time.Duration
	CheckBackendInterval time.Duration
	ConnectTimeout       time.Duration
	ConnBufferSize       int
	ProxyProtocol        bool
	RequireBackendTLS    bool
}

func (cfg *BCConfig) check() {
	if cfg.TickerInterval == time.Duration(0) {
		cfg.TickerInterval = TickerInterval
	}
	if cfg.CheckBackendInterval == time.Duration(0) {
		cfg.CheckBackendInterval = CheckBackendInterval
	}
	if cfg.ConnectTimeout == time.Duration(0) {
		cfg.ConnectTimeout = ConnectTimeout
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
	// processLock makes all processes exclusive.
	processLock sync.Mutex
	wg          waitgroup.WaitGroup
	// signalReceived is used to notify the signal processing goroutine.
	signalReceived chan signalType
	authenticator  *Authenticator
	cmdProcessor   *CmdProcessor
	eventReceiver  unsafe.Pointer
	config         *BCConfig
	logger         *zap.Logger
	curBackend     router.BackendInst
	// Redirect() sets it without lock. It will be set to nil after migration.
	redirectInfo atomic.Pointer[router.BackendInst]
	// redirectResCh is used to notify the event receiver asynchronously.
	redirectResCh chan *redirectResult
	// GracefulClose() sets it without lock.
	closeStatus atomic.Int32
	// The last time when the backend is active.
	lastActiveTime monotime.Time
	// The traffic recorded last time.
	inBytes, inPackets, outBytes, outPackets uint64
	// cancelFunc is used to cancel the signal processing goroutine.
	cancelFunc context.CancelFunc
	clientIO   *pnet.PacketIO
	// backendIO may be written during redirection and be read in ExecuteCmd/Redirect/setKeepalive.
	backendIO atomic.Pointer[pnet.PacketIO]
	// sessionState is used to restore session from zero backend mode
	sessionState     atomic.Pointer[SessionState]
	backendTLS       *tls.Config
	handshakeHandler HandshakeHandler
	ctxmap           struct {
		sync.Mutex
		m map[any]any
	}
	connectionID uint64
	quitSource   ErrorSource
}

// NewBackendConnManager creates a BackendConnManager.
func NewBackendConnManager(logger *zap.Logger, handshakeHandler HandshakeHandler, connectionID uint64, config *BCConfig) *BackendConnManager {
	config.check()
	mgr := &BackendConnManager{
		logger:           logger,
		config:           config,
		connectionID:     connectionID,
		cmdProcessor:     NewCmdProcessor(logger.Named("cp")),
		handshakeHandler: handshakeHandler,
		authenticator:    NewAuthenticator(config),
		// There are 2 types of signals, which may be sent concurrently.
		signalReceived: make(chan signalType, signalTypeSaveSession),
		redirectResCh:  make(chan *redirectResult, 1),
		quitSource:     SrcNone,
	}
	mgr.ctxmap.m = make(map[any]any)
	mgr.SetValue(ConnContextKeyConnID, connectionID)
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

	if mgr.closeStatus.Load() >= statusNotifyClose {
		mgr.quitSource = SrcProxyQuit
		return errors.New("graceful shutdown before connecting")
	}
	startTime := monotime.Now()
	err := mgr.authenticator.handshakeFirstTime(ctx, mgr.logger.Named("authenticator"), mgr, clientIO, mgr.handshakeHandler, mgr.getBackendIO, frontendTLSConfig, backendTLSConfig)
	if err != nil {
		src := Error2Source(err)
		mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), err, src)
		// For some errors, convert them to MySQL errors and send them to the client.
		if clientErr := ErrToClient(err); clientErr != nil {
			clientIO.WriteUserError(clientErr)
		}
		mgr.quitSource = src
		return err
	}
	mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), nil, SrcNone)
	endTime := monotime.Now()
	addHandshakeMetrics(mgr.ServerAddr(), time.Duration(endTime-startTime))
	mgr.updateTraffic(mgr.backendIO.Load())

	mgr.cmdProcessor.capability = mgr.authenticator.capability
	childCtx, cancelFunc := context.WithCancel(ctx)
	mgr.cancelFunc = cancelFunc
	mgr.lastActiveTime = endTime
	mgr.wg.RunWithRecover(func() {
		mgr.processSignals(childCtx)
	}, func(_ any) {
		// If we do not clean up, the router may retain the connection forever and the TiDB won't be released in the
		// Serverless Tier.
		_ = mgr.Close()
	}, mgr.logger)
	return nil
}

func (mgr *BackendConnManager) newExponentialBackOff() *backoff.ExponentialBackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     defaultExponentialBackOffInitialInterval,
		RandomizationFactor: defaultExponentialBackOffRandomizationFactor,
		Multiplier:          defaultExponentialBackOffMultiplier,
		MaxInterval:         defaultExponentionBackOffMaxInterval,
		MaxElapsedTime:      mgr.config.ConnectTimeout,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

func (mgr *BackendConnManager) getBackendIO(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (*pnet.PacketIO, error) {
	r, err := mgr.handshakeHandler.GetRouter(cctx, resp)
	if err != nil {
		return nil, errors.Wrap(ErrProxyErr, err)
	}
	// Reasons to wait:
	// - The TiDB instances may not be initialized yet
	// - One TiDB may be just shut down and another is just started but not ready yet
	bctx, cancel := context.WithTimeout(ctx, mgr.config.ConnectTimeout)
	selector := r.GetBackendSelector()
	startTime := monotime.Now()
	var addr string
	var backend router.BackendInst
	var origErr error
	io, err := backoff.RetryNotifyWithData(
		func() (*pnet.PacketIO, error) {
			addr = ""
			// Try to connect to all backup backends one by one.
			if backend, err = selector.Next(); err == router.ErrNoBackend {
				return nil, ErrProxyNoBackend
			} else if err != nil {
				return nil, backoff.Permanent(errors.Wrap(ErrProxyErr, err))
			}

			var cn net.Conn
			addr = backend.Addr()
			cn, err = net.DialTimeout("tcp", addr, DialTimeout)
			selector.Finish(mgr, err == nil)
			if err != nil {
				return nil, errors.Wrap(ErrBackendHandshake, errors.Wrapf(err, "dial backend %s error", addr))
			}

			// NOTE: should use DNS name as much as possible
			// Usually certs are signed with domain instead of IP addrs
			// And `RemoteAddr()` will return IP addr
			backendIO := pnet.NewPacketIO(cn, mgr.logger, mgr.config.ConnBufferSize, pnet.WithRemoteAddr(addr, cn.RemoteAddr()), pnet.WithWrapError(ErrBackendConn))
			mgr.backendIO.Store(backendIO)
			mgr.curBackend = backend
			mgr.setKeepAlive()
			return backendIO, nil
		},
		backoff.WithContext(mgr.newExponentialBackOff(), bctx),
		func(err error, d time.Duration) {
			origErr = err
			mgr.handshakeHandler.OnHandshake(cctx, addr, err, Error2Source(err))
		},
	)
	cancel()

	duration := monotime.Since(startTime)
	addGetBackendMetrics(duration, err == nil)
	if err != nil {
		mgr.logger.Error("get backend failed", zap.Duration("duration", duration), zap.NamedError("last_err", origErr))
	} else if duration >= time.Second {
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
	startTime := monotime.Now()
	mgr.processLock.Lock()
	defer func() {
		mgr.setQuitSourceByErr(err)
		mgr.handshakeHandler.OnTraffic(mgr)
		now := monotime.Now()
		if err != nil && errors.Is(err, ErrBackendConn) {
			cmd, data := pnet.Command(request[0]), request[1:]
			var query string
			if cmd == pnet.ComQuery {
				query = parser.Normalize(pnet.ParseQueryPacket(data))
				if len(query) > 256 {
					query = query[:256]
				}
			}
			// idle_time: maybe the idle time exceeds wait_timeout?
			// execute_time and query: maybe this query causes TiDB OOM?
			mgr.logger.Info("backend disconnects", zap.Duration("idle_time", time.Duration(now-mgr.lastActiveTime)),
				zap.Duration("execute_time", time.Duration(now-startTime)), zap.Stringer("cmd", cmd), zap.String("query", query))
		}
		mgr.lastActiveTime = now
		mgr.processLock.Unlock()
	}()
	if len(request) < 1 {
		err = mysql.ErrMalformPacket
		return
	}
	cmd := pnet.Command(request[0])

	// Once the request is accepted, it's treated in the transaction, so we don't check graceful shutdown here.
	if mgr.closeStatus.Load() >= statusClosing {
		return
	}
	if mgr.backendIO.Load() == nil {
		err = mgr.reconnect(ctx)
		if err != nil {
			mgr.logger.Info("reconnect failed", zap.Error(err))
			return
		}
	}
	waitingRedirect := mgr.redirectInfo.Load() != nil
	var holdRequest bool
	backendIO := mgr.backendIO.Load()
	holdRequest, err = mgr.cmdProcessor.executeCmd(request, mgr.clientIO, backendIO, waitingRedirect)
	if !holdRequest {
		addCmdMetrics(cmd, backendIO.RemoteAddr().String(), startTime)
		mgr.updateTraffic(backendIO)
	}
	if err != nil {
		if !pnet.IsMySQLError(err) {
			return
		} else {
			mgr.logger.Debug("got a mysql error", zap.Error(err), zap.Stringer("cmd", cmd))
		}
	}
	if err == nil {
		switch cmd {
		case pnet.ComQuit:
			return
		case pnet.ComSetOption:
			val := binary.LittleEndian.Uint16(request[1:])
			switch val {
			case 0:
				mgr.authenticator.capability |= pnet.ClientMultiStatements
				mgr.cmdProcessor.capability |= pnet.ClientMultiStatements
			case 1:
				mgr.authenticator.capability &^= pnet.ClientMultiStatements
				mgr.cmdProcessor.capability &^= pnet.ClientMultiStatements
			default:
				err = errors.Wrapf(mysql.ErrMalformPacket, "unrecognized set_option value:%d", val)
				return
			}
		case pnet.ComChangeUser:
			// Critical errors should not happen because CmdProcessor has parsed it already.
			req, _ := pnet.ParseChangeUser(request, mgr.authenticator.capability)
			mgr.authenticator.changeUser(req)
		}
	}
	// Even if it meets an MySQL error, it may have changed the status, such as when executing multi-statements.
	if mgr.cmdProcessor.finishedTxn() {
		if mgr.closeStatus.Load() == statusNotifyClose {
			mgr.tryGracefulClose(ctx)
		} else if waitingRedirect {
			mgr.tryRedirect(ctx)
		}
	}
	// Execute the held request no matter redirection succeeds or not.
	if holdRequest && mgr.closeStatus.Load() < statusNotifyClose {
		backendIO = mgr.backendIO.Load()
		_, err = mgr.cmdProcessor.executeCmd(request, mgr.clientIO, backendIO, false)
		addCmdMetrics(cmd, backendIO.RemoteAddr().String(), startTime)
		mgr.updateTraffic(backendIO)
		if err != nil && !pnet.IsMySQLError(err) {
			return
		}
	}
	// Ignore MySQL errors, only return unexpected errors.
	err = nil
	return
}

func (mgr *BackendConnManager) updateTraffic(backendIO *pnet.PacketIO) {
	inBytes, inPackets, outBytes, outPackets := backendIO.InBytes(), backendIO.InPackets(), backendIO.OutBytes(), backendIO.OutPackets()
	addTraffic(backendIO.RemoteAddr().String(), inBytes-mgr.inBytes, inPackets-mgr.inPackets, outBytes-mgr.outBytes, outPackets-mgr.outPackets)
	mgr.inBytes, mgr.inPackets, mgr.outBytes, mgr.outPackets = inBytes, inPackets, outBytes, outPackets
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
	var result *mysql.Resultset
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
	checkBackendTicker := time.NewTicker(mgr.config.TickerInterval)
	for {
		select {
		case s := <-mgr.signalReceived:
			func() {
				// Redirect the session immediately just in case the session is finishedTxn.
				mgr.processLock.Lock()
				defer mgr.processLock.Unlock()
				switch s {
				case signalTypeGracefulClose:
					mgr.tryGracefulClose(ctx)
				case signalTypeRedirect:
					mgr.tryRedirect(ctx)
				case signalTypeSaveSession:
					mgr.trySaveSession(ctx)
				}
			}()
		case rs := <-mgr.redirectResCh:
			mgr.notifyRedirectResult(ctx, rs)
		case <-checkBackendTicker.C:
			func() {
				mgr.checkBackendActive()
				mgr.processLock.Lock()
				defer mgr.processLock.Unlock()
				mgr.setKeepAlive()
			}()
		case <-ctx.Done():
			checkBackendTicker.Stop()
			return
		}
	}
}

// tryRedirect tries to migrate the session if the session is redirect-able.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) tryRedirect(ctx context.Context) {
	if mgr.closeStatus.Load() >= statusNotifyClose || ctx.Err() != nil {
		return
	}
	backendInst := mgr.redirectInfo.Load()
	if backendInst == nil {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}

	rs := &redirectResult{
		from: mgr.ServerAddr(),
		to:   (*backendInst).Addr(),
	}
	defer func() {
		// The `mgr` won't be notified again before it calls `OnRedirectSucceed`, so simply `StorePointer` is also fine.
		mgr.redirectInfo.Store(nil)
		// Notifying may block. Notify the receiver asynchronously to:
		// - Reduce the latency of session migration
		// - Avoid the risk of deadlock
		mgr.redirectResCh <- rs
	}()
	// It may have been too long since the redirection signal was sent, and the target backend may be unhealthy now.
	if !(*backendInst).Healthy() {
		rs.err = ErrTargetUnhealthy
		return
	}
	backendIO := mgr.backendIO.Load()
	var sessionStates, sessionToken string
	if backendIO == nil {
		mgr.logger.Info("session saved, no need to redirect")
		return
	} else {
		if sessionStates, sessionToken, rs.err = mgr.querySessionStates(backendIO); rs.err != nil {
			// If the backend connection is closed, also close the client connection.
			// Otherwise, if the client is idle, the mgr will keep retrying.
			if errors.Is(rs.err, net.ErrClosed) || pnet.IsDisconnectError(rs.err) || errors.Is(rs.err, os.ErrDeadlineExceeded) {
				mgr.quitSource = SrcBackendNetwork
				if ignoredErr := mgr.clientIO.GracefulClose(); ignoredErr != nil {
					mgr.logger.Warn("graceful close client IO error", zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Error(ignoredErr))
				}
			}
			return
		}
		if ctx.Err() != nil {
			rs.err = ctx.Err()
			return
		}
	}
	if rs.err = mgr.updateAuthInfoFromSessionStates(hack.Slice(sessionStates)); rs.err != nil {
		return
	}

	var cn net.Conn
	cn, rs.err = net.DialTimeout("tcp", rs.to, DialTimeout)
	if rs.err != nil {
		mgr.handshakeHandler.OnHandshake(mgr, rs.to, rs.err, SrcBackendNetwork)
		return
	}
	newBackendIO := pnet.NewPacketIO(cn, mgr.logger, mgr.config.ConnBufferSize, pnet.WithRemoteAddr(rs.to, cn.RemoteAddr()), pnet.WithWrapError(ErrBackendConn))

	if rs.err = mgr.authenticator.handshakeSecondTime(mgr.logger, mgr.clientIO, newBackendIO, mgr.backendTLS, sessionToken); rs.err == nil {
		rs.err = mgr.initSessionStates(newBackendIO, sessionStates)
	} else {
		mgr.handshakeHandler.OnHandshake(mgr, newBackendIO.RemoteAddr().String(), rs.err, Error2Source(rs.err))
	}
	if rs.err != nil {
		if ignoredErr := newBackendIO.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			mgr.logger.Error("close new backend connection failed", zap.Error(ignoredErr))
		}
		return
	}
	mgr.updateTraffic(backendIO)
	mgr.inBytes, mgr.inPackets, mgr.outBytes, mgr.outPackets = 0, 0, 0, 0
	mgr.updateTraffic(newBackendIO)
	if ignoredErr := backendIO.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
		mgr.logger.Error("close previous backend connection failed", zap.Error(ignoredErr))
	}
	mgr.backendIO.Store(newBackendIO)
	mgr.curBackend = *backendInst
	mgr.setKeepAlive()
	mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), nil, SrcNone)
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
func (mgr *BackendConnManager) Redirect(backendInst router.BackendInst) bool {
	// NOTE: BackendConnManager may be closing concurrently because of no lock.
	if mgr.closeStatus.Load() >= statusNotifyClose {
		return false
	}
	if mgr.backendIO.Load() == nil {
		return false
	}
	mgr.redirectInfo.Store(&backendInst)
	// Generally, it won't wait because the caller won't send another signal before the previous one finishes.
	mgr.signalReceived <- signalTypeRedirect
	return true
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
		mgr.logger.Debug("redirect connection succeeds", zap.String("from", rs.from),
			zap.String("to", rs.to), zap.NamedError("notify_err", err))
	}
}

// GracefulClose waits for the end of the transaction and closes the session.
func (mgr *BackendConnManager) GracefulClose() {
	if mgr.closeStatus.CompareAndSwap(statusActive, statusNotifyClose) {
		mgr.signalReceived <- signalTypeGracefulClose
	}
}

func (mgr *BackendConnManager) tryGracefulClose(ctx context.Context) {
	if mgr.closeStatus.Load() != statusNotifyClose || ctx.Err() != nil {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}
	mgr.quitSource = SrcProxyQuit
	// Closing clientIO will cause the whole connection to be closed.
	if err := mgr.clientIO.GracefulClose(); err != nil {
		mgr.logger.Warn("graceful close client IO error", zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
	}
	mgr.closeStatus.CompareAndSwap(statusNotifyClose, statusClosing)
}

func (mgr *BackendConnManager) checkBackendActive() {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	if mgr.closeStatus.Load() >= statusNotifyClose {
		return
	}
	now := monotime.Now()
	if mgr.lastActiveTime.Add(mgr.config.CheckBackendInterval).After(now) {
		return
	}
	backendIO := mgr.backendIO.Load()
	if backendIO == nil {
		return
	}
	if !backendIO.IsPeerActive() {
		mgr.logger.Info("backend connection is closed, close client connection",
			zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Stringer("backend_addr", backendIO.RemoteAddr()),
			zap.Bool("backend_healthy", mgr.curBackend.Healthy()))
		mgr.quitSource = SrcBackendNetwork
		if err := mgr.clientIO.GracefulClose(); err != nil {
			mgr.logger.Warn("graceful close client IO error", zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
		}
		mgr.closeStatus.CompareAndSwap(statusActive, statusClosing)
	} else {
		mgr.lastActiveTime = now
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
	mgr.ctxmap.Lock()
	mgr.ctxmap.m[key] = val
	mgr.ctxmap.Unlock()
}

func (mgr *BackendConnManager) Value(key any) any {
	mgr.ctxmap.Lock()
	v := mgr.ctxmap.m[key]
	mgr.ctxmap.Unlock()
	return v
}

// Close releases all resources.
func (mgr *BackendConnManager) Close() error {
	// BackendConnMgr may close even before connecting, so protect the members with a lock.
	mgr.processLock.Lock()
	defer func() {
		mgr.processLock.Unlock()
		// Wait out of the lock to avoid deadlock.
		mgr.wg.Wait()
	}()
	if mgr.closeStatus.Load() >= statusClosed {
		return nil
	}

	mgr.closeStatus.Store(statusClosing)
	if mgr.cancelFunc != nil {
		mgr.cancelFunc()
		mgr.cancelFunc = nil
	}

	// OnConnClose may read ServerAddr(), so call it before closing backendIO.
	handErr := mgr.handshakeHandler.OnConnClose(mgr, mgr.quitSource)

	var connErr error
	var addr string
	if backendIO := mgr.backendIO.Swap(nil); backendIO != nil {
		addr = backendIO.RemoteAddr().String()
		connErr = backendIO.Close()
	}

	eventReceiver := mgr.getEventReceiver()
	if eventReceiver != nil {
		// Notify the receiver if there's any event.
		if len(mgr.redirectResCh) > 0 {
			mgr.notifyRedirectResult(context.Background(), <-mgr.redirectResCh)
		}
		// Just notify it with the current address.
		if len(addr) > 0 {
			if err := eventReceiver.OnConnClosed(addr, mgr); err != nil {
				mgr.logger.Error("close connection error", zap.String("backend_addr", addr), zap.NamedError("notify_err", err))
			}
		}
	}
	mgr.closeStatus.Store(statusClosed)
	return errors.Collect(ErrCloseConnMgr, connErr, handErr)
}

// setKeepAlive sets keepalive on the backend connection based on the health status.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) setKeepAlive() {
	if mgr.closeStatus.Load() >= statusClosing {
		return
	}
	backendIO := mgr.backendIO.Load()
	if backendIO == nil {
		return
	}

	// The request to the unhealthy backend may block, instead of fail immediately.
	// So we set a shorter keep alive timeout for the unhealthy backends.
	cfg := mgr.config.HealthyKeepAlive
	curHealthy := mgr.curBackend.Healthy()
	if !curHealthy {
		cfg = mgr.config.UnhealthyKeepAlive
	}
	if err := backendIO.SetKeepalive(cfg); err != nil {
		mgr.logger.Warn("failed to set keepalive", zap.Stringer("backend_addr", backendIO.RemoteAddr()),
			zap.Bool("backend_healthy", curHealthy), zap.Error(err))
	}
}

func (mgr *BackendConnManager) setQuitSourceByErr(err error) {
	if err == nil {
		return
	}
	// The source may be already be set.
	// E.g. quitSource is set before TiProxy shuts down and client connection error is caused by shutdown instead of network.
	if mgr.quitSource != SrcNone {
		return
	}
	mgr.quitSource = Error2Source(err)
}

// UpdateLogger add fields to the logger.
// Note: it should be called within the lock.
func (mgr *BackendConnManager) UpdateLogger(fields ...zap.Field) {
	mgr.logger = mgr.logger.With(fields...)
}

// ConnInfo returns detailed info of the connection, which should not be logged too many times.
func (mgr *BackendConnManager) ConnInfo() []zap.Field {
	mgr.processLock.Lock()
	var fields []zap.Field
	if mgr.authenticator != nil {
		fields = mgr.authenticator.ConnInfo()
	}
	mgr.processLock.Unlock()
	fields = append(fields, zap.String("backend_addr", mgr.ServerAddr()))
	return fields
}

func (mgr *BackendConnManager) SaveSession() bool {
	if mgr.closeStatus.Load() >= statusNotifyClose {
		return false
	}

	if mgr.backendIO.Load() == nil {
		return false
	}

	mgr.signalReceived <- signalTypeSaveSession
	return true
}

func (mgr *BackendConnManager) trySaveSession(ctx context.Context) {
	backendIO := mgr.backendIO.Load()
	if backendIO == nil {
		mgr.logger.Error("failed to get backend when save session")
		return
	}
	sessionStates, sessionToken, err := mgr.querySessionStates(backendIO)
	if err != nil {
		mgr.logger.Error("query session failed when save session", zap.Error(err))
		return
	}
	mgr.sessionState.Store(&SessionState{
		sessionStates: sessionStates,
		sessionToken:  sessionToken,
	})
	mgr.backendIO.Store(nil)
}

// reconnect to a new backend when in zero backend mode, then restore the saved session to the backend.
func (mgr *BackendConnManager) reconnect(ctx context.Context) error {
	token := new(SessionToken)
	state := mgr.sessionState.Load()
	if state == nil {
		err := errors.New("session state is nil")
		return err
	}
	err := json.Unmarshal([]byte(state.sessionToken), token)
	if err != nil {
		return err
	}

	clientResp := pnet.HandshakeResp{User: token.Username}
	mgr.logger.Info("start to getBackendIO")
	newBackendIO, err := mgr.getBackendIO(ctx, mgr, &clientResp)
	if err != nil {
		return err
	}
	mgr.logger.Info("start to handshakeSecondTime")
	if err = mgr.authenticator.handshakeSecondTime(mgr.logger, mgr.clientIO, newBackendIO, mgr.backendTLS, state.sessionToken); err == nil {
		err = mgr.initSessionStates(newBackendIO, state.sessionStates)
	} else {
		mgr.handshakeHandler.OnHandshake(mgr, newBackendIO.RemoteAddr().String(), err, Error2Source(err))
	}
	mgr.logger.Info("finish to handshakeSecondTime")
	if err != nil {
		if ignoredErr := newBackendIO.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			mgr.logger.Error("close new backend connection failed", zap.Error(ignoredErr))
		}
		return err
	}
	mgr.backendIO.Store(newBackendIO)
	mgr.sessionState.Store(nil)

	return nil
}
