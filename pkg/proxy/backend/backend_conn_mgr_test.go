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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
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

type runner struct {
	client  func(packetIO *pnet.PacketIO) error
	proxy   func(clientIO, backendIO *pnet.PacketIO) error
	backend func(packetIO *pnet.PacketIO) error
}

// backendMgrTester encapsulates testSuite but is dedicated for BackendConnMgr.
type backendMgrTester struct {
	*testSuite
	t      *testing.T
	closed bool
}

func newBackendMgrTester(t *testing.T, cfg ...cfgOverrider) *backendMgrTester {
	tc := newTCPConnSuite(t)
	cfg = append(cfg, func(cfg *testConfig) {
		cfg.testSuiteConfig.enableRouteLogic = true
	})
	ts, clean := newTestSuite(t, tc, cfg...)
	tester := &backendMgrTester{
		testSuite: ts,
		t:         t,
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
	ts.tc.backendIO = pnet.NewPacketIO(conn)
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

func (ts *backendMgrTester) forwardCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	require.NoError(ts.t, err)
	prevCounter, err := readCmdCounter(request[0], ts.tc.backendListener.Addr().String())
	require.NoError(ts.t, err)
	rsErr := ts.mp.ExecuteCmd(context.Background(), request, clientIO)
	curCounter, err := readCmdCounter(request[0], ts.tc.backendListener.Addr().String())
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
	ts.mb.status = mysql.ServerStatusInTrans
	return ts.mb.respond(packetIO)
}

func (ts *backendMgrTester) checkNotRedirected4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	signal := (*signalRedirect)(atomic.LoadPointer(&ts.mp.signal))
	require.Nil(ts.t, signal)
	backend1 := ts.mp.backendConn
	// There is no other way to verify it's not redirected.
	// The buffer size of channel signalReceived is 0, so after the second redirect signal is sent,
	// we can ensure that the first signal is already processed.
	ts.mp.Redirect(ts.tc.backendListener.Addr().String())
	ts.mp.signalReceived <- signalTypeRedirect
	// The backend connection is still the same.
	require.Equal(ts.t, backend1, ts.mp.backendConn)
	return nil
}

func (ts *backendMgrTester) redirectAfterCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendConn
	err := ts.forwardCmd4Proxy(clientIO, backendIO)
	require.NoError(ts.t, err)
	ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventSucceed)
	require.NotEqual(ts.t, backend1, ts.mp.backendConn)
	require.Len(ts.t, ts.mp.GetRedirectingAddr(), 0)
	return nil
}

func (ts *backendMgrTester) redirectFail4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendConn
	ts.mp.Redirect(ts.tc.backendListener.Addr().String())
	ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(ts.t, eventFail)
	require.Equal(ts.t, backend1, ts.mp.backendConn)
	require.Len(ts.t, ts.mp.GetRedirectingAddr(), 0)
	return nil
}

func (ts *backendMgrTester) checkConnClosed(_, _ *pnet.PacketIO) error {
	for i := 0; i < 30; i++ {
		switch ts.mp.closeStatus.Load() {
		case statusClosing, statusClosed:
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timeout")
}

func (ts *backendMgrTester) runTests(runners []runner) {
	for _, runner := range runners {
		ts.runAndCheck(ts.t, nil, runner.client, runner.backend, runner.proxy)
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
			client: nil,
			proxy: func(_, _ *pnet.PacketIO) error {
				backend1 := ts.mp.backendConn
				ts.mp.Redirect(ts.tc.backendListener.Addr().String())
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventSucceed)
				require.NotEqual(t, backend1, ts.mp.backendConn)
				return nil
			},
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
				backend1 := ts.mp.backendConn
				err := ts.forwardCmd4Proxy(clientIO, backendIO)
				require.NoError(t, err)
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventFail)
				require.Equal(t, backend1, ts.mp.backendConn)
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
				ts.tc.backendIO = pnet.NewPacketIO(conn)
				ts.mb.authSucceed = false
				return ts.mb.authenticate(ts.tc.backendIO)
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
				tmpBackendIO := pnet.NewPacketIO(conn)
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
				tmpBackendIO := pnet.NewPacketIO(conn)
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
				ts.mc.cmd = mysql.ComChangeUser
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
				ts.mc.cmd = mysql.ComSetOption
				ts.mc.dataBytes = []byte{1, 0}
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// 2nd handshake
		{
			client: nil,
			proxy: func(_, _ *pnet.PacketIO) error {
				backend1 := ts.mp.backendConn
				ts.mp.Redirect(ts.tc.backendListener.Addr().String())
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventSucceed)
				require.NotEqual(t, backend1, ts.mp.backendConn)
				return nil
			},
			backend: func(packetIO *pnet.PacketIO) error {
				ts.mb.sessionStates = "{\"current-db\":\"session_db\"}"
				require.NoError(t, ts.redirectSucceed4Backend(packetIO))
				require.Equal(t, "another_user", ts.mb.username)
				require.Equal(t, "session_db", ts.mb.db)
				expectCap := pnet.Capability(ts.mp.authenticator.supportedServerCapabilities.Uint32() &^ (mysql.ClientMultiStatements | mysql.ClientPluginAuthLenencClientData))
				gotCap := pnet.Capability(ts.mb.capability &^ mysql.ClientPluginAuthLenencClientData)
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
				ts.mp.Redirect(addr)
				eventReceiver.checkEvent(t, eventSucceed)
				require.Equal(t, addr, ts.mp.GetRedirectingAddr())
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
		// 2nd handshake
		{
			client: nil,
			proxy: func(_, _ *pnet.PacketIO) error {
				backend1 := ts.mp.backendConn
				ts.mp.Redirect(ts.tc.backendListener.Addr().String())
				ts.mp.getEventReceiver().(*mockEventReceiver).checkEvent(t, eventSucceed)
				require.NotEqual(t, backend1, ts.mp.backendConn)
				return nil
			},
			backend: ts.redirectSucceed4Backend,
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
			proxy: ts.checkConnClosed,
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
			proxy: ts.checkConnClosed,
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
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// it will then automatically close
		{
			proxy: ts.checkConnClosed,
		},
	}
	ts.runTests(runners)
}

func TestGetBackendIO(t *testing.T) {
	listeners := make([]net.Listener, 0, 3)
	addrs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
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
		onHandshake: func(connContext ConnContext, s string, err error) {
			if err != nil && len(s) > 0 {
				badAddrs[s] = struct{}{}
			}
		},
	}
	mgr := NewBackendConnManager(logger.CreateLoggerForTest(t), handler, 0, false, false)
	for i := 0; i <= len(listeners); i++ {
		io, err := mgr.getBackendIO(mgr.authenticator, mgr.authenticator, nil, time.Second)
		if err == nil {
			require.NoError(t, io.Close())
		}
		if i < len(listeners) {
			require.NoError(t, err)
			err = listeners[i].Close()
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.True(t, len(badAddrs) <= i)
		badAddrs = make(map[string]struct{}, 3)
	}
}
