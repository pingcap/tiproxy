// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/proxy/client"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestCreateConn(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := &config.Config{}
	certManager := cert.NewCertManager()
	require.NoError(t, certManager.Init(cfg, lg, nil))
	server, err := NewSQLServer(lg, cfg, certManager, &mockHsHandler{})
	require.NoError(t, err)
	server.Run(context.Background(), nil)
	defer func() {
		require.NoError(t, server.Close())
	}()

	createConn := func() net.Conn {
		conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
		require.NoError(t, err)
		return conn
	}
	checkMetrics := func(totalConns, createConns int) {
		require.Eventually(t, func() bool {
			connGauge, err := metrics.ReadGauge(metrics.ConnGauge)
			require.NoError(t, err)
			if totalConns != int(connGauge) {
				return false
			}
			connCounter, err := metrics.ReadCounter(metrics.CreateConnCounter)
			require.NoError(t, err)
			return createConns == connCounter
		}, time.Second, 10*time.Millisecond)
	}

	checkMetrics(0, 0)
	conn1 := createConn()
	checkMetrics(1, 1)
	conn2 := createConn()
	checkMetrics(2, 2)
	require.NoError(t, conn1.Close())
	checkMetrics(1, 2)
	require.NoError(t, conn2.Close())
	checkMetrics(0, 2)
}

func TestGracefulCloseConn(t *testing.T) {
	// Graceful shutdown finishes immediately if there's no connection.
	lg, _ := logger.CreateLoggerForTest(t)
	hsHandler := backend.NewDefaultHandshakeHandler(nil)
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			ProxyServerOnline: config.ProxyServerOnline{
				GracefulCloseConnTimeout: 10,
			},
		},
	}
	server, err := NewSQLServer(lg, cfg, nil, hsHandler)
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
			conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		}()
		conn, err := server.listeners[0].Accept()
		require.NoError(t, err)
		clientConn := client.NewClientConnection(lg, conn, nil, nil, hsHandler, 0, "", &backend.BCConfig{})
		server.mu.clients[1] = clientConn
		server.mu.Unlock()
		return clientConn
	}

	// Graceful shutdown will be blocked if there are alive connections.
	server, err = NewSQLServer(lg, cfg, nil, hsHandler)
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

	// Graceful shutdown will shut down after GracefulCloseConnTimeout.
	cfg.Proxy.GracefulCloseConnTimeout = 1
	server, err = NewSQLServer(lg, cfg, nil, hsHandler)
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

func TestGracefulShutDown(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			ProxyServerOnline: config.ProxyServerOnline{
				GracefulWaitBeforeShutdown: 2,
				GracefulCloseConnTimeout:   10,
			},
		},
	}
	server, err := NewSQLServer(lg, cfg, certManager, &mockHsHandler{})
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	var wg waitgroup.WaitGroup
	wg.Run(func() {
		// Wait until the server begins to shut down.
		require.Eventually(t, server.IsClosing, 500*time.Millisecond, 10*time.Millisecond)
		// The listener should be open and handshake should proceed.
		_, port, err := net.SplitHostPort(server.listeners[0].Addr().String())
		require.NoError(t, err)
		mdb, err := sql.Open("mysql", fmt.Sprintf("root@tcp(localhost:%s)/test", port))
		require.NoError(t, err)
		require.ErrorContains(t, mdb.Ping(), "no router")
		// The listener should be closed after GracefulWaitBeforeShutdown.
		require.Eventually(t, func() bool {
			conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
			if err == nil {
				require.NoError(t, conn.Close())
			}
			return err != nil
		}, 3*time.Second, 100*time.Millisecond)
	})
	require.NoError(t, server.Close())
	wg.Wait()
}

func TestMultiAddr(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	server, err := NewSQLServer(lg, &config.Config{
		Proxy: config.ProxyServer{
			Addr: "0.0.0.0:0,0.0.0.0:0",
		},
	}, certManager, &mockHsHandler{})
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	require.Len(t, server.listeners, 2)
	for _, listener := range server.listeners {
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	require.NoError(t, server.Close())
	certManager.Close()
}

func TestWatchCfg(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hsHandler := backend.NewDefaultHandshakeHandler(nil)
	cfgch := make(chan *config.Config)
	server, err := NewSQLServer(lg, &config.Config{}, nil, hsHandler)
	require.NoError(t, err)
	server.Run(context.Background(), cfgch)
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			ProxyServerOnline: config.ProxyServerOnline{
				MaxConnections:           100,
				ConnBufferSize:           1024 * 1024,
				ProxyProtocol:            "v2",
				GracefulCloseConnTimeout: 100,
			},
		},
		Security: config.Security{
			RequireBackendTLS: true,
		},
	}
	cfgch <- cfg
	require.Eventually(t, func() bool {
		server.mu.RLock()
		defer server.mu.RUnlock()
		return server.mu.requireBackendTLS == cfg.Security.RequireBackendTLS &&
			server.mu.maxConnections == cfg.Proxy.MaxConnections &&
			server.mu.connBufferSize == cfg.Proxy.ConnBufferSize &&
			server.mu.proxyProtocol == (cfg.Proxy.ProxyProtocol != "") &&
			server.mu.gracefulWait == cfg.Proxy.GracefulWaitBeforeShutdown
	}, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, server.Close())
}

func TestRecoverPanic(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	server, err := NewSQLServer(lg, &config.Config{}, certManager, &mockHsHandler{
		handshakeResp: func(ctx backend.ConnContext, _ *pnet.HandshakeResp) error {
			if ctx.Value(backend.ConnContextKeyConnID).(uint64) == 0 {
				panic("HandleHandshakeResp panic")
			}
			return nil
		},
	})
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	_, port, err := net.SplitHostPort(server.listeners[0].Addr().String())
	require.NoError(t, err)
	mdb, err := sql.Open("mysql", fmt.Sprintf("root@tcp(localhost:%s)/test", port))
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

type mockHsHandler struct {
	backend.DefaultHandshakeHandler
	handshakeResp func(ctx backend.ConnContext, _ *pnet.HandshakeResp) error
}

// HandleHandshakeResp only panics for the first connections.
func (handler *mockHsHandler) HandleHandshakeResp(ctx backend.ConnContext, resp *pnet.HandshakeResp) error {
	if handler.handshakeResp != nil {
		return handler.handshakeResp(ctx, resp)
	}
	return nil
}

func (handler *mockHsHandler) GetServerVersion() string {
	return "5.7"
}

// GetRouter returns an error for the second connection.
func (handler *mockHsHandler) GetRouter(backend.ConnContext, *pnet.HandshakeResp) (router.Router, error) {
	return nil, errors.New("no router")
}
