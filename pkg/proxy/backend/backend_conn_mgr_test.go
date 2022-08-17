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
	"io"
	"sync/atomic"
	"testing"

	"github.com/pingcap/TiProxy/pkg/manager/router"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
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

func (mer *mockEventReceiver) OnRedirectSucceed(from, to string, conn router.RedirectableConn) {
	mer.eventCh <- event{
		from:      from,
		to:        to,
		eventName: eventSucceed,
	}
}

func (mer *mockEventReceiver) OnRedirectFail(from, to string, conn router.RedirectableConn) {
	mer.eventCh <- event{
		from:      from,
		to:        to,
		eventName: eventFail,
	}
}

func (mer *mockEventReceiver) OnConnClosed(from string, conn router.RedirectableConn) {
	mer.eventCh <- event{
		from:      from,
		eventName: eventClose,
	}
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
	clean func()
	t     *testing.T
}

func newBackendMgrTester(t *testing.T) *backendMgrTester {
	tc := newTCPConnSuite(t)
	ts, clean := newTestSuiteWithoutBackendConn(t, tc)
	return &backendMgrTester{
		testSuite: ts,
		clean:     clean,
		t:         t,
	}
}

// Define some common runners here to reduce code redundancy.
func (ts *backendMgrTester) firstHandshake4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	err := ts.mp.Connect(context.Background(), ts.tc.backendListener.Addr().String(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
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
	require.True(ts.t, errors.Is(err, io.EOF))
	return nil
}

func (ts *backendMgrTester) forwardCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	require.NoError(ts.t, err)
	return ts.mp.ExecuteCmd(context.Background(), request, clientIO)
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
	ts.mp.signalReceived <- struct{}{}
	// The backend connection is still the same.
	require.Equal(ts.t, backend1, ts.mp.backendConn)
	return nil
}

func (ts *backendMgrTester) redirectAfterCmd4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendConn
	err := ts.forwardCmd4Proxy(clientIO, backendIO)
	require.NoError(ts.t, err)
	ts.mp.eventReceiver.(*mockEventReceiver).checkEvent(ts.t, eventSucceed)
	require.NotEqual(ts.t, backend1, ts.mp.backendConn)
	return nil
}

func (ts *backendMgrTester) redirectFail4Proxy(clientIO, backendIO *pnet.PacketIO) error {
	backend1 := ts.mp.backendConn
	ts.mp.Redirect(ts.tc.backendListener.Addr().String())
	ts.mp.eventReceiver.(*mockEventReceiver).checkEvent(ts.t, eventFail)
	require.Equal(ts.t, backend1, ts.mp.backendConn)
	return nil
}

func (ts *backendMgrTester) runTests(runners []runner) {
	for _, runner := range runners {
		ts.runAndCheck(ts.t, nil, runner.client, runner.backend, runner.proxy)
	}
	ts.clean()
	err := ts.mp.Close()
	require.NoError(ts.t, err)
	if ts.mp.eventReceiver != nil {
		ts.mp.eventReceiver.(*mockEventReceiver).checkEvent(ts.t, eventClose)
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
			proxy: func(clientIO, backendIO *pnet.PacketIO) error {
				backend1 := ts.mp.backendConn
				ts.mp.Redirect(ts.tc.backendListener.Addr().String())
				ts.mp.eventReceiver.(*mockEventReceiver).checkEvent(t, eventSucceed)
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
				ts.mp.eventReceiver.(*mockEventReceiver).checkEvent(t, eventFail)
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
				return ts.mp.Connect(context.Background(), ts.tc.backendListener.Addr().String(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig)
			},
			backend: func(packetIO *pnet.PacketIO) error {
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
				require.True(ts.t, errors.Is(err, io.EOF))
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
				require.True(ts.t, errors.Is(err, io.EOF))
				return tmpBackendIO.Close()
			},
		},
	}
	ts.runTests(runners)
}
