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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	sqlQueryState    = "SHOW SESSION_STATES"
	sqlSetState      = "SET SESSION_STATES '%s'"
	sessionStatesCol = "Session_states"
	sessionTokenCol  = "Session_token"
	currentDBKey     = "current-db"
)

type signalRedirect struct {
	newAddr string
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
	connectionID  uint64
	authenticator *Authenticator
	cmdProcessor  *CmdProcessor
	eventReceiver router.ConnEventReceiver
	backendConn   *BackendConnection
	// processLock makes redirecting and command processing exclusive.
	processLock sync.Mutex
	// signalReceived is used to notify the signal processing goroutine.
	signalReceived chan struct{}
	// type *signalRedirect, it saves the last signal if there are multiple signals.
	// It will be set to nil after migration.
	signal unsafe.Pointer
	// cancelFunc is used to cancel the signal processing goroutine.
	cancelFunc context.CancelFunc
}

// NewBackendConnManager creates a BackendConnManager.
func NewBackendConnManager(connectionID uint64) *BackendConnManager {
	return &BackendConnManager{
		connectionID:   connectionID,
		cmdProcessor:   NewCmdProcessor(),
		authenticator:  &Authenticator{},
		signalReceived: make(chan struct{}),
	}
}

// ConnectionID implements RedirectableConn.ConnectionID interface.
// It returns the ID of the frontend connection. The ID stays still after session migration.
func (mgr *BackendConnManager) ConnectionID() uint64 {
	return mgr.connectionID
}

// Connect connects to the first backend and then start watching redirection signals.
func (mgr *BackendConnManager) Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	mgr.backendConn = NewBackendConnection(serverAddr)
	if err := mgr.backendConn.Connect(); err != nil {
		return err
	}
	backendIO := mgr.backendConn.PacketIO()
	if err := mgr.authenticator.handshakeFirstTime(clientIO, backendIO, serverTLSConfig, backendTLSConfig); err != nil {
		return err
	}
	childCtx, cancelFunc := context.WithCancel(ctx)
	go mgr.processSignals(childCtx)
	mgr.cancelFunc = cancelFunc
	return nil
}

// ExecuteCmd forwards messages between the client and the backend.
// If it finds that the session is ready for redirection, it migrates the session.
func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()
	waitingRedirect := atomic.LoadPointer(&mgr.signal) != nil
	holdRequest, err := mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), waitingRedirect)
	if err != nil && !IsMySQLError(err) {
		return err
	}
	if err == nil {
		switch request[0] {
		case mysql.ComQuit:
			return nil
		case mysql.ComChangeUser:
			username, db := pnet.ParseChangeUser(request)
			mgr.authenticator.changeUser(username, db)
			return nil
		}
	}
	// Even if it meets an MySQL error, it may have changed the status, such as when executing multi-statements.
	if waitingRedirect && mgr.cmdProcessor.canRedirect() {
		_ = mgr.tryRedirect(ctx)
		// Execute the held request no matter redirection succeeds or not.
		if holdRequest {
			_, err = mgr.cmdProcessor.executeCmd(request, clientIO, mgr.backendConn.PacketIO(), false)
		}
		if err != nil && !IsMySQLError(err) {
			return err
		}
	}
	// Ignore MySQL errors, only return unexpected errors.
	return nil
}

// SetEventReceiver implements RedirectableConn.SetEventReceiver interface.
// The receiver sends redirection signals and watches redirecting events.
func (mgr *BackendConnManager) SetEventReceiver(receiver router.ConnEventReceiver) {
	mgr.eventReceiver = receiver
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

// processSignals runs in a goroutine to receive redirection signals.
// It will then try to migrate the session.
func (mgr *BackendConnManager) processSignals(ctx context.Context) {
	for {
		select {
		// Redirect the session immediately just in case the session is idle.
		case _, ok := <-mgr.signalReceived:
			if !ok {
				return
			}
			mgr.processLock.Lock()
			_ = mgr.tryRedirect(ctx)
			mgr.processLock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// tryRedirect tries to migrate the session if the session is redirect-able.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) tryRedirect(ctx context.Context) error {
	signal := (*signalRedirect)(atomic.LoadPointer(&mgr.signal))
	if signal == nil {
		return nil
	}

	if !mgr.cmdProcessor.canRedirect() {
		return nil
	}
	from := mgr.backendConn.Addr()
	to := signal.newAddr

	var err error
	defer func() {
		if err != nil {
			mgr.eventReceiver.OnRedirectFail(from, to, mgr)
			logutil.Logger(ctx).Warn("redirect connection failed", zap.String("from", from), zap.String("to", to), zap.Error(err))
		} else {
			mgr.eventReceiver.OnRedirectSucceed(from, to, mgr)
			logutil.Logger(ctx).Info("redirect connection succeeds", zap.String("from", from), zap.String("to", to))
		}
	}()

	var sessionStates, sessionToken string
	if sessionStates, sessionToken, err = mgr.querySessionStates(); err != nil {
		return err
	}

	newConn := NewBackendConnection(to)
	if err = newConn.Connect(); err != nil {
		return err
	}
	if err = mgr.authenticator.handshakeSecondTime(newConn.PacketIO(), sessionToken); err == nil {
		err = mgr.initSessionStates(newConn.PacketIO(), sessionStates)
	}
	if err != nil {
		if ignoredErr := newConn.Close(); ignoredErr != nil {
			logutil.Logger(ctx).Warn("close new backend connection failed", zap.Error(ignoredErr))
		}
		return err
	}
	if ignoredErr := mgr.backendConn.Close(); ignoredErr != nil {
		logutil.Logger(ctx).Warn("close previous backend connection failed", zap.Error(ignoredErr))
	}
	mgr.backendConn = newConn
	// The `mgr` won't be notified again before it calls `OnRedirectSucceed`, so simply `StorePointer` is also fine.
	atomic.CompareAndSwapPointer(&mgr.signal, unsafe.Pointer(signal), nil)
	return nil
}

// The original db in the auth info may be dropped during the session, so we need to authenticate with the current db.
// The user may be renamed during the session, but the session cannot detect it, so this will affect the user.
// TODO: this may be a security problem: a different new user may just be renamed to this user name.
func (mgr *BackendConnManager) updateAuthInfoFromSessionStates(sessionStates []byte) error {
	var statesMap map[string]string
	if err := json.Unmarshal(sessionStates, &statesMap); err != nil {
		return errors.Wrapf(err, "unmarshal session states error")
	}
	// The currentDBKey may be omitted if it's empty. In this case, we still need to update it.
	currentDB := statesMap[currentDBKey]
	mgr.authenticator.updateCurrentDB(currentDB)
	return nil
}

// Redirect implements RedirectableConn.Redirect interface. It redirects the current session to the newAddr.
// Note that the caller requires the function to be non-blocking.
func (mgr *BackendConnManager) Redirect(newAddr string) {
	// We do not use `chan signalRedirect` to avoid blocking. We cannot discard the signal when it blocks,
	// because only the latest signal matters.
	atomic.StorePointer(&mgr.signal, unsafe.Pointer(&signalRedirect{
		newAddr: newAddr,
	}))
	logutil.BgLogger().Info("received redirect command", zap.String("from", mgr.backendConn.Addr()), zap.String("to", newAddr))
	select {
	case mgr.signalReceived <- struct{}{}:
	default:
	}
}

// Close releases all resources.
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
