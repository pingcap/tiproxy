// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"database/sql"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/router"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/proxy/client"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdown(t *testing.T) {
	// Graceful shutdown finishes immediately if there's no connection.
	lg, _ := logger.CreateLoggerForTest(t)
	hsHandler := backend.NewDefaultHandshakeHandler(nil, "")
	server, err := NewSQLServer(lg, config.ProxyServer{
		ProxyServerOnline: config.ProxyServerOnline{
			GracefulWaitBeforeShutdown: 10,
		},
	}, nil, hsHandler)
	require.NoError(t, err)
	finish := make(chan struct{})
	go func() {
		err = server.Close()
		require.NoError(t, err)
		finish <- struct{}{}
	}()
	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case <-finish:
	}

	createClientConn := func() *client.ClientConnection {
		server.mu.Lock()
		go func() {
			conn, err := net.Dial("tcp", server.listener.Addr().String())
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		}()
		conn, err := server.listener.Accept()
		require.NoError(t, err)
		clientConn := client.NewClientConnection(lg, conn, nil, nil, hsHandler, 0, &backend.BCConfig{})
		server.mu.clients[1] = clientConn
		server.mu.Unlock()
		return clientConn
	}

	// Graceful shutdown will be blocked if there are alive connections.
	server, err = NewSQLServer(lg, config.ProxyServer{
		ProxyServerOnline: config.ProxyServerOnline{
			GracefulWaitBeforeShutdown: 10,
		},
	}, nil, hsHandler)
	require.NoError(t, err)
	clientConn := createClientConn()
	go func() {
		require.NoError(t, server.Close())
		finish <- struct{}{}
	}()
	select {
	case <-time.After(300 * time.Millisecond):
	case <-finish:
		t.Fatal("should be blocked")
	}
	// Graceful shutdown finishes immediately when the connection finishes.
	server.mu.Lock()
	require.NoError(t, clientConn.Close())
	delete(server.mu.clients, 1)
	server.mu.Unlock()
	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case <-finish:
	}

	// Graceful shutdown will shut down after GracefulWaitBeforeShutdown.
	server, err = NewSQLServer(lg, config.ProxyServer{
		ProxyServerOnline: config.ProxyServerOnline{
			GracefulWaitBeforeShutdown: 1,
		},
	}, nil, hsHandler)
	require.NoError(t, err)
	createClientConn()
	go func() {
		require.NoError(t, server.Close())
		finish <- struct{}{}
	}()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	case <-finish:
	}
}

func TestRecoverPanic(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	server, err := NewSQLServer(lg, config.ProxyServer{
		Addr: "0.0.0.0:6000",
	}, certManager, &panicHsHandler{})
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	mdb, err := sql.Open("mysql", "root@tcp(localhost:6000)/test")
	require.NoError(t, err)
	// The first connection encounters panic.
	require.ErrorContains(t, mdb.Ping(), "invalid connection")
	require.Eventually(t, func() bool {
		return strings.Contains(text.String(), "panic")
	}, 3*time.Second, 10*time.Millisecond)
	// The second connection gets a server error, which means the server is still running.
	require.ErrorContains(t, mdb.Ping(), "no router")
	require.NoError(t, mdb.Close())
	require.NoError(t, server.Close())
	certManager.Close()
}

type panicHsHandler struct {
	backend.DefaultHandshakeHandler
}

// HandleHandshakeResp only panics for the first connections.
func (handler *panicHsHandler) HandleHandshakeResp(ctx backend.ConnContext, _ *pnet.HandshakeResp) error {
	if ctx.Value(backend.ConnContextKeyConnID).(uint64) == 0 {
		panic("HandleHandshakeResp panic")
	}
	return nil
}

func (handler *panicHsHandler) GetServerVersion() string {
	return "5.7"
}

// GetRouter returns an error for the second connection.
func (handler *panicHsHandler) GetRouter(backend.ConnContext, *pnet.HandshakeResp) (router.Router, error) {
	return nil, errors.New("no router")
}
