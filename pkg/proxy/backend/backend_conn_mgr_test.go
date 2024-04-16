// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	eventSucceed = iota
	eventFail
	eventClose
)

type event struct {
	from, to  string
	eventName int
}

type mockEventReceiver struct {
	eventCh chan event
}

func newMockEventReceiver() *mockEventReceiver {
	return &mockEventReceiver{
		eventCh: make(chan event, 1),
	}
}

func (mer *mockEventReceiver) OnRedirectSucceed(from, to string, conn router.RedirectableConn) error {
	mer.eventCh <- event{
		from:      from,
		to:        to,
		eventName: eventSucceed,
	}
	return nil
}

func (mer *mockEventReceiver) OnRedirectFail(from, to string, conn router.RedirectableConn) error {
	mer.eventCh <- event{
		from:      from,
		to:        to,
		eventName: eventFail,
	}
	return nil
}

func (mer *mockEventReceiver) OnConnClosed(from string, conn router.RedirectableConn) error {
	mer.eventCh <- event{
		from:      from,
		eventName: eventClose,
	}
	return nil
}

func (mer *mockEventReceiver) checkEvent(t *testing.T, eventName int) {
	e := <-mer.eventCh
	require.Equal(t, eventName, e.eventName)
}

type mockBackendInst struct {
	addr    string
	healthy atomic.Bool
}

func newMockBackendInst(ts *backendMgrTester) *mockBackendInst {
	mbi := &mockBackendInst{
		addr: ts.tc.backendListener.Addr().String(),
	}
	mbi.setHealthy(true)
	return mbi
}

func (mbi *mockBackendInst) Addr() string {
	return mbi.addr
}

func (mbi *mockBackendInst) Healthy() bool {
	return mbi.healthy.Load()
}

func (mbi *mockBackendInst) setHealthy(healthy bool) {
	mbi.healthy.Store(healthy)
}

type runner struct {
	client  func(packetIO *pnet.PacketIO) error
	proxy   func(clientIO, backendIO *pnet.PacketIO) error
	backend func(packetIO *pnet.PacketIO) error
}

// backendMgrTester encapsulates testSuite but is dedicated for BackendConnMgr.
type backendMgrTester struct {
	*testSuite
	t      *testing.T
	lg     *zap.Logger
	closed bool
}

func newBackendMgrTester(t *testing.T, cfg ...cfgOverrider) *backendMgrTester {
	tc := newTCPConnSuite(t)
	cfg = append(cfg, func(cfg *testConfig) {
		cfg.testSuiteConfig.enableRouteLogic = true
	})
	ts, clean := newTestSuite(t, tc, cfg...)
	lg, _ := logger.CreateLoggerForTest(t)
	tester := &backendMgrTester{
		testSuite: ts,
		t:         t,
		lg:        lg,
	}
	t.Cleanup(func() {
		clean()
		if tester.closed {
			return
		}
		err := ts.mp.Close()
		require.NoError(t, err)
		eventReceiver := ts.mp.getEventReceiver()
		if eventReceiver != nil {
			eventReceiver.(*mockEventReceiver).checkEvent(t, eventClose)
		}
	})
	return tester
}

// Define some common runners here to reduce code redundancy.
func (ts *backendMgrTester) firstHandshake4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
	require.NoError(ts.t, err)
	mer := newMockEventReceiver()
	ts.mp.SetEventReceiver(mer)
	return nil
}

func (ts *backendMgrTester) handshake4Backend(packetIO *pnet.PacketIO) error {
	conn, err := ts.tc.backendListener.Accept()
	require.NoError(ts.t, err)
	ts.tc.backendIO = pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
	return ts.mb.authenticate(ts.tc.backendIO)
}

func (ts *backendMgrTester) redirectSucceed4Backend(packetIO *pnet.PacketIO) error {
	// respond to `SHOW SESSION STATES`
	ts.mb.respondType = responseTypeResultSet
	err := ts.mb.respond(packetIO)
	require.NoError(ts.t, err)
	err = ts.handshake4Backend(ts.tc.backendIO)
	require.NoError(ts.t, err)
	// respond to `SET SESSION STATES`
	err = ts.respondWithNoTxn4Backend(ts.tc.backendIO)
	require.NoError(ts.t, err)
	// previous connection is closed
	_, err = packetIO.ReadPacket()
	require.True(ts.t, pnet.IsDisconnectError(err))
	return nil
}

func (ts *backendMgrTester) redirectSucceed4Proxy(_, _ *pnet.PacketIO) error {
	backend1 := ts.mp.backendIO.Load()
	ts.mp.Redirect(newMockBackendInst(ts))
	ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventSucceed)
	require.NotEqual(ts.t, backend1, ts.mp.backendIO.Load())
	require.Equal(ts.t, SrcNone, ts.mp.QuitSource())
	return nil
}

func (ts *backendMgrTester) forwardCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	require.NoError(ts.t, err)
	prevCounter, err := readCmdCounter(pnet.Command(request[0]), ts.tc.backendListener.Addr().String())
	require.NoError(ts.t, err)
	rsErr := ts.mp.ExecuteCmd(context.Background(), request)
	curCounter, err := readCmdCounter(pnet.Command(request[0]), ts.tc.backendListener.Addr().String())
	require.NoError(ts.t, err)
	require.Equal(ts.t, prevCounter+1, curCounter)
	return rsErr
}

func (ts *backendMgrTester) respondWithNoTxn4Backend(packetIO *pnet.PacketIO) error {
	ts.mb.respondType = responseTypeOK
	ts.mb.status = 0
	return ts.mb.respond(packetIO)
}

func (ts *backendMgrTester) startTxn4Backend(packetIO *pnet.PacketIO) error {
	ts.mb.respondType = responseTypeOK
	ts.mb.status = pnet.ServerStatusInTrans
	return ts.mb.respond(packetIO)
}

func (ts *backendMgrTester) checkNotRedirected4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	redirInfo := ts.mp.redirectInfo.Load()
	require.Nil(ts.t, redirInfo)
	backend1 := ts.mp.backendIO.Load()
	// There is no other way to verify it's not redirected.
	// The buffer size of channel signalReceived is 0, so after the second redirect signal is sent,
	// we can ensure that the first signal is already processed.
	ts.mp.Redirect(newMockBackendInst(ts))
	ts.mp.signalReceived <- signalTypeRedirect
	// The backend connection is still the same.
	require.Equal(ts.t, backend1, ts.mp.backendIO.Load())
	return nil
}

func (ts *backendMgrTester) redirectAfterCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendIO.Load()
	err := ts.forwardCmd4Proxy(clientIO, backendIO)
	require.NoError(ts.t, err)
	ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventSucceed)
	require.NotEqual(ts.t, backend1, ts.mp.backendIO.Load())
	return nil
}

func (ts *backendMgrTester) redirectFail4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendIO.Load()
	ts.mp.Redirect(newMockBackendInst(ts))
	ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventFail)
	require.Equal(ts.t, backend1, ts.mp.backendIO.Load())
	return nil
}

func (ts *backendMgrTester) checkConnClosed4Proxy(_, _ *pnet.PacketIO) error {
	require.Eventually(ts.t, func() bool {
		switch ts.mp.closeStatus.Load() {
		case statusClosing, statusClosed:
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
	return nil
}

func (ts *backendMgrTester) runTests(runners []runner) {
	for _, runner := range runners {
		ts.runAndCheck(ts.t, nil, runner.client, runner.backend, runner.proxy)
		require.Equal(ts.t, ts.tc.clientIO.InBytes(), ts.mp.ClientOutBytes())
		require.Equal(ts.t, ts.tc.clientIO.OutBytes(), ts.mp.ClientInBytes())
	}
}

// Test that redirection succeeds immediately if the session is redirect-able.
func TestNormalRedirect(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// 2nd handshake: redirect immediately after connection
		{
			client:  nil,
			proxy:   ts.redirectSucceed4Proxy,
			backend: ts.redirectSucceed4Backend,
		},
	}
	ts.runTests(runners)
}

// Test redirection when the session has a transaction.
func TestRedirectInTxn(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// start a transaction to make it unredirect-able
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to redirect but it doesn't redirect
		{
			proxy: ts.checkNotRedirected4Proxy,
		},
		// finish the transaction and it will then automatically redirect
		{
			client: ts.mc.request,
			proxy:  ts.redirectAfterCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to the client request
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				return ts.redirectSucceed4Backend(packetIO)
			},
		},
		// start a transaction to make it unredirect-able
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to redirect but it doesn't redirect
		{
			proxy: ts.checkNotRedirected4Proxy,
		},
		// hold the request and then make it redirect-able
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "begin"
				return ts.mc.request(packetIO)
			},
			proxy: ts.redirectAfterCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to internal COMMIT
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				// redirect
				err = ts.redirectSucceed4Backend(packetIO)
				require.NoError(t, err)
				// respond to `begin`
				return ts.startTxn4Backend(ts.tc.backendIO)
			},
		},
		// start a transaction to make it unredirect-able
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to redirect but it doesn't redirect
		{
			proxy: ts.checkNotRedirected4Proxy,
		},
		// CHANGE_USER clears the txn
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComChangeUser
				return ts.mc.request(packetIO)
			},
			proxy: ts.redirectAfterCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to the client request
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				return ts.redirectSucceed4Backend(packetIO)
			},
		},
		// start a transaction to make it unredirect-able
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComQuery
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to redirect but it doesn't redirect
		{
			proxy: ts.checkNotRedirected4Proxy,
		},
		// internal COMMIT fails and the `begin` is not sent
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "begin"
				return ts.mc.request(packetIO)
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				return ts.forwardCmd4Proxy(clientIO, backendIO)
			},
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to internal COMMIT
				ts.mb.respondType = responseTypeErr
				return ts.mb.respond(packetIO)
			},
		},
		// show session states fails and the `begin` is sent to the old backend
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "begin"
				return ts.mc.request(packetIO)
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				backend1 := ts.mp.backendIO.Load()
				err := ts.forwardCmd4Proxy(clientIO, backendIO)
				require.NoError(t, err)
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventFail)
				require.Equal(t, backend1, ts.mp.backendIO.Load())
				require.Equal(t, SrcNone, ts.mp.QuitSource())
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to internal COMMIT
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				// respond to `SHOW SESSION_STATES`
				ts.mb.respondType = responseTypeErr
				err = ts.mb.respond(packetIO)
				require.NoError(t, err)
				// respond to `begin`
				return ts.startTxn4Backend(packetIO)
			},
		},
	}

	ts.runTests(runners)
}

// Test that the client handshake fails.
func TestConnectFail(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				return ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
			},
			backend: func(_ *pnet.PacketIO) error {
				conn, err := ts.tc.backendListener.Accept()
				require.NoError(ts.t, err)
				ts.tc.backendIO = pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
				ts.mb.authSucceed = false
				return ts.mb.authenticate(ts.tc.backendIO)
			},
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, SrcClientAuthFail, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

// Test that the proxy works fine when redirection fails.
func TestRedirectFail(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// show session states fails
		{
			proxy: ts.redirectFail4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to `SHOW SESSION_STATES`
				ts.mb.respondType = responseTypeErr
				return ts.mb.respond(packetIO)
			},
		},
		// 2nd handshake fails
		{
			proxy: ts.redirectFail4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to `SHOW SESSION_STATES`
				ts.mb.respondType = responseTypeResultSet
				err := ts.mb.respondOnce(packetIO)
				require.NoError(t, err)
				conn, err := ts.tc.backendListener.Accept()
				require.NoError(t, err)
				tmpBackendIO := pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
				// auth fails
				ts.mb.authSucceed = false
				err = ts.mb.authenticate(tmpBackendIO)
				require.NoError(t, err)
				// the new connection is closed
				_, err = tmpBackendIO.ReadPacket()
				require.True(ts.t, pnet.IsDisconnectError(err))
				return tmpBackendIO.Close()
			},
		},
		// set session states fails
		{
			proxy: ts.redirectFail4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				// respond to `SHOW SESSION STATES`
				ts.mb.respondType = responseTypeResultSet
				err := ts.mb.respond(packetIO)
				require.NoError(ts.t, err)
				conn, err := ts.tc.backendListener.Accept()
				require.NoError(ts.t, err)
				tmpBackendIO := pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
				ts.mb.authSucceed = true
				err = ts.mb.authenticate(tmpBackendIO)
				require.NoError(t, err)
				// respond to `SET SESSION STATES`
				ts.mb.respondType = responseTypeErr
				err = ts.mb.respond(tmpBackendIO)
				require.NoError(t, err)
				// the new connection is closed
				_, err = tmpBackendIO.ReadPacket()
				require.True(ts.t, pnet.IsDisconnectError(err))
				return tmpBackendIO.Close()
			},
		},
	}
	ts.runTests(runners)
}

// Test that the proxy sends the right handshake info after COM_CHANGE_USER and COM_SET_OPTION.
func TestSpecialCmds(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// change user
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComChangeUser
				ts.mc.username = "another_user"
				ts.mc.dbName = "another_db"
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// disable multi-stmts
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComSetOption
				ts.mc.dataBytes = []byte{1, 0}
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// 2nd handshake
		{
			client: nil,
			proxy:  ts.redirectSucceed4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.sessionStates = "{\"current-db\":\"session_db\"}"
				require.NoError(t, ts.redirectSucceed4Backend(packetIO))
				require.Equal(t, "another_user", ts.mb.username)
				require.Equal(t, "session_db", ts.mb.db)
				expectCap := ts.mp.handshakeHandler.GetCapability() & defaultTestClientCapability &^ (pnet.ClientMultiStatements | pnet.ClientPluginAuthLenencClientData)
				gotCap := ts.mb.capability &^ pnet.ClientPluginAuthLenencClientData
				require.Equal(t, expectCap, gotCap, "expected=%s,got=%s", expectCap, gotCap)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

// Test that closing the BackendConnMgr while it's receiving a redirection signal is OK.
func TestCloseWhileRedirect(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// close and redirect concurrently
		{
			proxy: func(_, _ *pnet.PacketIO) error {
				// Send an event to make Close() block at notifying.
				addr := ts.tc.backendListener.Addr().String()
				eventReceiver := ts.mp.getEventReceiver().(*mockEventReceiver)
				err := eventReceiver.OnRedirectSucceed(addr, addr, ts.mp)
				require.NoError(t, err)
				var wg waitgroup.WaitGroup
				wg.Run(func() {
					_ = ts.mp.Close()
					ts.closed = true
				})
				// Make sure the process goroutine finishes.
				ts.mp.wg.Wait()
				// Redirect() should not panic after Close().
				ts.mp.Redirect(newMockBackendInst(ts))
				eventReceiver.checkEvent(t, eventSucceed)
				wg.Wait()
				eventReceiver.checkEvent(t, eventClose)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestCustomHandshake(t *testing.T) {
	ts := newBackendMgrTester(t, func(cfg *testConfig) {
		handler := cfg.proxyConfig.handler
		handler.handleHandshakeResp = func(ctx ConnContext, resp *pnet.HandshakeResp) error {
			resp.User = "rewritten_user"
			resp.Attrs = map[string]string{"key": "value"}
			return nil
		}
		handler.getCapability = func() pnet.Capability {
			return SupportedServerCapabilities & ^pnet.ClientDeprecateEOF
		}
		handler.getServerVersion = func() string {
			return "test_server_version"
		}
	})
	runners := []runner{
		// 1st handshake
		{
			client: func(packetIO *pnet.PacketIO) error {
				if err := ts.mc.authenticate(packetIO); err != nil {
					return err
				}
				require.Equal(t, "test_server_version", ts.mc.serverVersion)
				return nil
			},
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// query
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "select 1"
				return ts.mc.request(packetIO)
			},
			proxy: ts.forwardCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.respondType = responseTypeResultSet
				ts.mb.columns = 1
				ts.mb.rows = 1
				return ts.mb.respond(packetIO)
			},
		},
		// 2nd handshake
		{
			client:  nil,
			proxy:   ts.redirectSucceed4Proxy,
			backend: ts.redirectSucceed4Backend,
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, SrcNone, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseWhenIdle(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// graceful close
		{
			proxy: func(_, _ *pnet.PacketIO) error {
				ts.mp.GracefulClose()
				return nil
			},
		},
		// really closed
		{
			proxy: ts.checkConnClosed4Proxy,
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseWhenActive(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// start a transaction to make it active
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to gracefully close but it doesn't close
		{
			proxy: func(_, _ *pnet.PacketIO) error {
				ts.mp.GracefulClose()
				time.Sleep(300 * time.Millisecond)
				require.Equal(t, statusNotifyClose, ts.mp.closeStatus.Load())
				return nil
			},
		},
		// finish the transaction
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// it will then automatically close
		{
			proxy: ts.checkConnClosed4Proxy,
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseBeforeHandshake(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// try to gracefully close before handshake
		{
			proxy: func(_, _ *pnet.PacketIO) error {
				ts.mp.GracefulClose()
				return nil
			},
		},
		// connect fails
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
				require.Error(ts.t, err)
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestHandlerReturnError(t *testing.T) {
	tests := []struct {
		cfg        cfgOverrider
		errMsg     string
		quitSource ErrorSource
	}{
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.handler.getRouter = func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
					return nil, errors.New("mocked error")
				}
			},
			errMsg:     "mocked error",
			quitSource: SrcProxyErr,
		},
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.handler.handleHandshakeResp = func(ctx ConnContext, resp *pnet.HandshakeResp) error {
					return errors.New("mocked error")
				}
			},
			errMsg:     "mocked error",
			quitSource: SrcProxyErr,
		},
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.bcConfig.ConnectTimeout = time.Second
				config.proxyConfig.handler.getRouter = func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
					return router.NewStaticRouter(nil), nil
				}
			},
			errMsg:     ErrProxyNoBackend.Error(),
			quitSource: SrcProxyNoBackend,
		},
	}
	for _, test := range tests {
		ts := newBackendMgrTester(t, test.cfg)
		rn := runner{
			client: func(packetIO *pnet.PacketIO) error {
				err := ts.mc.authenticate(packetIO)
				require.NoError(t, err)
				require.ErrorContains(t, ts.mc.mysqlErr, test.errMsg)
				return nil
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
				require.Error(t, err)
				require.Equal(t, test.quitSource, ts.mp.QuitSource())
				return nil
			},
			backend: nil,
		}
		ts.runAndCheck(ts.t, func(t *testing.T, ts *testSuite) {}, rn.client, rn.backend, rn.proxy)
	}
}

func TestOnTraffic(t *testing.T) {
	var inBytes, outBytes uint64
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.handler.onTraffic = func(cc ConnContext) {
			require.Greater(t, cc.ClientInBytes(), uint64(0))
			require.GreaterOrEqual(t, cc.ClientInBytes(), inBytes)
			inBytes = cc.ClientInBytes()
			require.Greater(t, cc.ClientOutBytes(), uint64(0))
			require.GreaterOrEqual(t, cc.ClientOutBytes(), outBytes)
			outBytes = cc.ClientOutBytes()
		}
	})
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// query
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "select 1"
				return ts.mc.request(packetIO)
			},
			proxy: ts.forwardCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.respondType = responseTypeResultSet
				ts.mb.columns = 1
				ts.mb.rows = 1
				return ts.mb.respond(packetIO)
			},
		},
	}
	ts.runTests(runners)
}

func TestGetBackendIO(t *testing.T) {
	addrs := make([]string, 0, 3)
	listeners := make([]net.Listener, 0, cap(addrs))

	for i := 0; i < cap(addrs); i++ {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(t, err)
		listeners = append(listeners, listener)
		addrs = append(addrs, listener.Addr().String())
	}

	rt := router.NewStaticRouter(addrs)
	badAddrs := make(map[string]struct{}, 3)
	handler := &CustomHandshakeHandler{
		getRouter: func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
			return rt, nil
		},
		onHandshake: func(connContext ConnContext, s string, err error, src ErrorSource) {
			if err != nil && len(s) > 0 {
				badAddrs[s] = struct{}{}
			}
			if err != nil {
				require.Equal(t, SrcBackendHandshake, src)
			}
		},
	}
	lg, _ := logger.CreateLoggerForTest(t)
	mgr := NewBackendConnManager(lg, handler, 0, &BCConfig{ConnectTimeout: time.Second})
	var wg waitgroup.WaitGroup
	for i := 0; i <= len(listeners); i++ {
		wg.Run(func() {
			if i < len(listeners) {
				cn, err := listeners[i].Accept()
				require.NoError(t, err)
				require.NoError(t, cn.Close())
			}
		})
		io, err := mgr.getBackendIO(context.Background(), mgr, nil)
		if err == nil {
			require.NoError(t, io.Close())
		}
		message := fmt.Sprintf("%d: %s, %+v\n", i, badAddrs, err)
		if i < len(listeners) {
			require.NoError(t, err, message)
			err = listeners[i].Close()
			require.NoError(t, err, message)
		} else {
			require.Error(t, err, message)
		}
		require.True(t, len(badAddrs) <= i, message)
		badAddrs = make(map[string]struct{}, 3)
		wg.Wait()
	}
}

func TestBackendInactive(t *testing.T) {
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.bcConfig.TickerInterval = time.Millisecond
		config.proxyConfig.bcConfig.CheckBackendInterval = 10 * time.Millisecond
	})
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// do some queries and the interval is less than checkBackendInterval
		{
			client: func(packetIO *pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					time.Sleep(5 * time.Millisecond)
					if err := ts.mc.request(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					if err := ts.forwardCmd4Proxy(clientIO, backendIO); err != nil {
						return err
					}
				}
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					if err := ts.respondWithNoTxn4Backend(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
		},
		// do some queries and the interval is longer than checkBackendInterval
		{
			client: func(packetIO *pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					time.Sleep(30 * time.Millisecond)
					if err := ts.mc.request(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					if err := ts.forwardCmd4Proxy(clientIO, backendIO); err != nil {
						return err
					}
				}
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					if err := ts.respondWithNoTxn4Backend(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
		},
		// close the backend and the client connection will close
		{
			proxy: ts.checkConnClosed4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				return packetIO.Close()
			},
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, SrcBackendNetwork, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestKeepAlive(t *testing.T) {
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.bcConfig.TickerInterval = time.Millisecond
		config.proxyConfig.bcConfig.HealthyKeepAlive.Idle = time.Minute
		config.proxyConfig.bcConfig.UnhealthyKeepAlive.Idle = time.Second
	})
	runners := []runner{
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.NoError(t, ts.firstHandshake4Proxy(clientIO, backendIO))
				require.Equal(t, time.Minute, ts.mp.backendIO.Load().LastKeepAlive().Idle)
				return nil
			},
			backend: ts.handshake4Backend,
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				require.Equal(t, time.Minute, ts.mp.backendIO.Load().LastKeepAlive().Idle)
				ts.mp.curBackend.(*router.StaticBackend).SetHealthy(false)
				require.Eventually(t, func() bool {
					return ts.mp.backendIO.Load().LastKeepAlive().Idle == time.Second
				}, 3*time.Second, 10*time.Millisecond)
				ts.mp.curBackend.(*router.StaticBackend).SetHealthy(true)
				require.Eventually(t, func() bool {
					return ts.mp.backendIO.Load().LastKeepAlive().Idle == time.Minute
				}, 3*time.Second, 10*time.Millisecond)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestConnID(t *testing.T) {
	ids := []uint64{0, 4, 9}
	for _, id := range ids {
		ts := newBackendMgrTester(t, func(config *testConfig) {
			config.proxyConfig.connectionID = id
		})
		runners := []runner{{
			client: func(packetIO *pnet.PacketIO) error {
				err := ts.mc.authenticate(packetIO)
				require.Equal(t, ts.mc.connid, id)
				return err
			},
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		}}
		ts.runTests(runners)
	}
}

func TestConnAttrs(t *testing.T) {
	ts := newBackendMgrTester(t)
	attr1 := map[string]string{"k1": "v1"}
	attr2 := map[string]string{"k2": "v2"}
	runners := []runner{
		// 1st handshake
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.attrs = attr1
				return ts.mc.authenticate(packetIO)
			},
			proxy: ts.firstHandshake4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				err := ts.handshake4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr1, ts.mb.attrs)
				return nil
			},
		},
		// 2nd handshake
		{
			proxy: ts.redirectSucceed4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				err := ts.redirectSucceed4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr1, ts.mb.attrs)
				return nil
			},
		},
		// CHANGE_USER updates attrs
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComChangeUser
				ts.mc.attrs = attr2
				return ts.mc.request(packetIO)
			},
			proxy: ts.forwardCmd4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr2, ts.mb.attrs)
				return nil
			},
		},
		// 2nd handshake
		{
			proxy: ts.redirectSucceed4Proxy,
			backend: func(packetIO *pnet.PacketIO) error {
				err := ts.redirectSucceed4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr2, ts.mb.attrs)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

// Test the target backend becomes unhealthy during redirection.
func TestBackendStatusChange(t *testing.T) {
	ts := newBackendMgrTester(t)
	mbi := newMockBackendInst(ts)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// start a transaction to make it unredirect-able
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to redirect but it doesn't redirect
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				ts.mp.Redirect(mbi)
				ts.mp.signalReceived <- signalTypeRedirect
				// the target backend becomes unhealthy now
				mbi.healthy.Store(false)
				return nil
			},
		},
		// finish the transaction and the redirection fails
		{
			client: ts.mc.request,
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				backend1 := ts.mp.backendIO.Load()
				err := ts.forwardCmd4Proxy(clientIO, backendIO)
				require.NoError(ts.t, err)
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventFail)
				require.Equal(ts.t, backend1, ts.mp.backendIO.Load())
				require.Eventually(ts.t, func() bool {
					return strings.Contains(ts.mp.text.String(), ErrTargetUnhealthy.Error())
				}, time.Second, 10*time.Millisecond)
				return nil
			},
			backend: ts.respondWithNoTxn4Backend,
		},
	}

	ts.runTests(runners)
}

func TestCloseWhileConnect(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake while force close
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
				if err == nil {
					mer := newMockEventReceiver()
					ts.mp.SetEventReceiver(mer)
				}
				return err
			},
			backend: ts.handshake4Backend,
		},
	}

	ts.runTests(runners)
}

func TestCloseWhileExecute(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// execute cmd while force close
		{
			client: ts.mc.request,
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				clientIO.ResetSequence()
				request, err := clientIO.ReadPacket()
				if err != nil {
					return err
				}
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				return ts.mp.ExecuteCmd(context.Background(), request)
			},
			backend: ts.startTxn4Backend,
		},
	}

	ts.runTests(runners)
}

func TestCloseWhileGracefulClose(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// graceful close while force close
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				ts.mp.GracefulClose()
				return nil
			},
		},
	}

	ts.runTests(runners)
}

func TestTrafficMetrics(t *testing.T) {
	ts := newBackendMgrTester(t)
	var inBytes, inPackets, outBytes, outPackets int
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// receive at least 1000 packets
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "select * from t"
				return ts.mc.request(packetIO)
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				addr := ts.tc.backendListener.Addr().String()
				var err error
				inBytes, inPackets, outBytes, outPackets, err = readTraffic(addr)
				require.NoError(t, err)
				require.True(t, inBytes > 0 && inPackets > 0 && outBytes > 0 && outPackets > 0)
				require.NoError(t, ts.forwardCmd4Proxy(clientIO, backendIO))
				inBytes2, inPackets2, outBytes2, outPackets2, err := readTraffic(addr)
				require.NoError(t, err)
				require.True(t, inBytes2 > inBytes && inPackets2 > inPackets && outBytes2 > outBytes && outPackets2 > outPackets)
				require.True(t, inBytes2 > 4096 && inPackets2 > 1000)
				inBytes, inPackets, outBytes, outPackets = inBytes2, inPackets2, outBytes2, outPackets2
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.respondType = responseTypeResultSet
				ts.mb.columns = 1
				ts.mb.rows = 1000
				return ts.mb.respond(packetIO)
			},
		},
		// 2nd handshake: redirect
		{
			client:  nil,
			proxy:   ts.redirectSucceed4Proxy,
			backend: ts.redirectSucceed4Backend,
		},
		// the traffic should still increase after redirection
		{
			client: func(packetIO *pnet.PacketIO) error {
				ts.mc.sql = "select 1"
				return ts.mc.request(packetIO)
			},
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				addr := ts.tc.backendListener.Addr().String()
				inBytes1, inPackets1, outBytes1, outPackets1, err := readTraffic(addr)
				require.NoError(t, err)
				require.True(t, inBytes1 > inBytes && inPackets1 > inPackets && outBytes1 > outBytes && outPackets1 > outPackets)
				require.NoError(t, ts.forwardCmd4Proxy(clientIO, backendIO))
				inBytes2, inPackets2, outBytes2, outPackets2, err := readTraffic(addr)
				require.NoError(t, err)
				require.True(t, inBytes2 > inBytes1 && inPackets2 > inPackets1 && outBytes2 > outBytes1 && outPackets2 > outPackets1)
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.respondType = responseTypeResultSet
				ts.mb.columns = 1
				ts.mb.rows = 1
				return ts.mb.respond(packetIO)
			},
		},
	}
	ts.runTests(runners)
}

func TestDisconnectLog(t *testing.T) {
	ts := newBackendMgrTester(t)
	tests := []struct {
		runner  runner
		checker checker
	}{
		{
			// 1st handshake
			runner: runner{
				client:  ts.mc.authenticate,
				proxy:   ts.firstHandshake4Proxy,
				backend: ts.handshake4Backend,
			},
		},
		{
			// proxy logs SQL when the backend disconnects
			runner: runner{
				client: func(packetIO *pnet.PacketIO) error {
					ts.mc.sql = "select 1"
					return ts.mc.request(packetIO)
				},
				proxy: func(clientIO, backendIO *pnet.PacketIO) error {
					err := ts.forwardCmd4Proxy(clientIO, backendIO)
					_ = clientIO.Close()
					return err
				},
				backend: func(packetIO *pnet.PacketIO) error {
					return packetIO.Close()
				},
			},
			checker: func(t *testing.T, ts *testSuite) {
				require.True(t, pnet.IsDisconnectError(ts.mc.err))
				require.ErrorIs(t, ts.mp.err, ErrBackendConn)
				require.True(t, strings.Contains(ts.mp.text.String(), "select ?"))
			},
		},
	}
	// Do not run ts.runTests(runners) to skip the general checker.
	for _, test := range tests {
		ts.runAndCheck(ts.t, test.checker, test.runner.client, test.runner.backend, test.runner.proxy)
	}
}

func TestProcessSignalsPanic(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		{
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				// Mock panic in `mgr.processSignals()`.
				ts.mp.handler.onHandshake = func(connContext ConnContext, s string, err error, source ErrorSource) {
					panic("mock panic")
				}
				ts.mp.Redirect(newMockBackendInst(ts))
				// Panic won't set error so it's still treated as success. It's fine because it will close anyway.
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventSucceed)
				// Do not wait for eventClose because `clean` will wait for it.
				return nil
			},
			backend: ts.redirectSucceed4Backend,
		},
	}
	ts.runTests(runners)
}

func BenchmarkSyncMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var m sync.Map
		m.Store("1", "1")
		m.Load("1")
	}
}

func BenchmarkLockedMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := make(map[string]string)
		var lock sync.Mutex
		lock.Lock()
		m["1"] = "1"
		_ = m["1"]
		lock.Unlock()
	}
}
