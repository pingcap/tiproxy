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
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/router"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/proxy/client"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestGracefulCloseConn(t *testing.T) {
	// Graceful shutdown finishes immediately if there's no connection.
	lg, _ := logger.CreateLoggerForTest(t)
	hsHandler := backend.NewDefaultHandshakeHandler(nil)
	cfg := config.ProxyServer{
		ProxyServerOnline: config.ProxyServerOnline{
			GracefulCloseConnTimeout: 10,
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
	cfg.GracefulCloseConnTimeout = 1
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
	hsHandler := backend.NewDefaultHandshakeHandler(nil)
	cfg := config.ProxyServer{
		ProxyServerOnline: config.ProxyServerOnline{
			GracefulWaitBeforeShutdown: 2,
			GracefulCloseConnTimeout:   10,
		},
	}
	server, err := NewSQLServer(lg, cfg, nil, hsHandler)
	require.NoError(t, err)

	var wg waitgroup.WaitGroup
	wg.Run(func() {
		// Wait until the server begins to shut down.
		for i := 0; ; i++ {
			if server.IsClosing() {
				break
			}
			if i >= 50 {
				t.Fatal("timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
		// The listener should be open.
		conn1, err := net.Dial("tcp", server.listeners[0].Addr().String())
		require.NoError(t, err)
		// The listener should be closed after GracefulWaitBeforeShutdown.
		require.Eventually(t, func() bool {
			conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
			if err == nil {
				require.NoError(t, conn.Close())
			} else {
				require.ErrorContains(t, err, "connection refused")
			}
			return err != nil
		}, 3*time.Second, 100*time.Millisecond)
		require.NoError(t, conn1.Close())
	})
	require.NoError(t, server.Close())
	wg.Wait()
}

func TestMultiAddr(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	server, err := NewSQLServer(lg, config.ProxyServer{
		Addr: "0.0.0.0:0,0.0.0.0:0",
	}, certManager, &panicHsHandler{})
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
	server, err := NewSQLServer(lg, config.ProxyServer{}, nil, hsHandler)
	require.NoError(t, err)
	server.Run(context.Background(), cfgch)
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			ProxyServerOnline: config.ProxyServerOnline{
				RequireBackendTLS:        true,
				MaxConnections:           100,
				ConnBufferSize:           1024 * 1024,
				ProxyProtocol:            "v2",
				GracefulCloseConnTimeout: 100,
			},
		},
	}
	cfgch <- cfg
	require.Eventually(t, func() bool {
		server.mu.RLock()
		defer server.mu.RUnlock()
		return server.mu.requireBackendTLS == cfg.Proxy.RequireBackendTLS &&
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
	server, err := NewSQLServer(lg, config.ProxyServer{}, certManager, &panicHsHandler{})
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
