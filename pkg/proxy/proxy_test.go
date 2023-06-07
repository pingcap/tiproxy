// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	"github.com/pingcap/TiProxy/pkg/proxy/client"
	"github.com/stretchr/testify/require"
)

func TestGracefulShutdown(t *testing.T) {
	// Graceful shutdown finishes immediately if there's no connection.
	lg := logger.CreateLoggerForTest(t)
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
