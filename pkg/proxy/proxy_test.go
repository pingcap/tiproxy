// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	mgrmem "github.com/pingcap/tiproxy/pkg/manager/memory"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/proxy/client"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCreateConn(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := &config.Config{}
	certManager := cert.NewCertManager()
	require.NoError(t, certManager.Init(cfg, lg, nil))
	server, err := NewSQLServer(lg, cfg, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, nil)
	require.NoError(t, err)
	server.Run(context.Background(), nil)
	defer func() {
		server.PreClose()
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

func TestRejectConnByMemory(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	require.NoError(t, certManager.Init(&config.Config{}, lg, nil))
	server, err := NewSQLServer(lg, &config.Config{}, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, &mockMemUsageProvider{
		reject: true,
		snapshot: mgrmem.UsageSnapshot{
			Used:       9 * (1 << 30),
			Limit:      10 * (1 << 30),
			Usage:      0.9,
			UpdateTime: time.Now(),
			Valid:      true,
		},
		threshold: 0.9,
	})
	require.NoError(t, err)
	server.Run(context.Background(), nil)
	defer func() {
		server.PreClose()
		require.NoError(t, server.Close())
		certManager.Close()
	}()

	rejectBefore, err := metrics.ReadCounter(metrics.RejectConnCounter.WithLabelValues("memory"))
	require.NoError(t, err)
	createBefore, err := metrics.ReadCounter(metrics.CreateConnCounter)
	require.NoError(t, err)
	connGaugeBefore, err := metrics.ReadGauge(metrics.ConnGauge)
	require.NoError(t, err)

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	require.Eventually(t, func() bool {
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		var buf [1]byte
		_, err := conn.Read(buf[:])
		return err != nil
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		rejectAfter, err := metrics.ReadCounter(metrics.RejectConnCounter.WithLabelValues("memory"))
		require.NoError(t, err)
		createAfter, err := metrics.ReadCounter(metrics.CreateConnCounter)
		require.NoError(t, err)
		connGaugeAfter, err := metrics.ReadGauge(metrics.ConnGauge)
		require.NoError(t, err)
		return rejectAfter == rejectBefore+1 && createAfter == createBefore && connGaugeAfter == connGaugeBefore
	}, time.Second, 10*time.Millisecond)
}

func TestTrackConnBufferMemDelta(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			ProxyServerOnline: config.ProxyServerOnline{
				ConnBufferSize: 4096,
			},
		},
	}
	require.NoError(t, certManager.Init(cfg, lg, nil))
	memUsage := &mockMemUsageProvider{}
	server, err := NewSQLServer(lg, cfg, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, memUsage)
	require.NoError(t, err)
	server.Run(context.Background(), nil)
	defer func() {
		server.PreClose()
		require.NoError(t, server.Close())
		certManager.Close()
	}()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return memUsage.connBufferMemDelta.Load() == int64(cfg.Proxy.ConnBufferSize*2)
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		return memUsage.connBufferMemDelta.Load() == 0
	}, time.Second, 10*time.Millisecond)
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
	server, err := NewSQLServer(lg, cfg, nil, id.NewIDManager(), nil, nil, hsHandler, nil)
	require.NoError(t, err)
	finish := make(chan struct{})
	go func() {
		server.PreClose()
		require.NoError(t, server.Close())
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
		clientConn := client.NewClientConnection(lg, conn, nil, nil, hsHandler, nil, 0, "", &backend.BCConfig{}, nil)
		server.mu.clients[1] = clientConn
		server.mu.Unlock()
		return clientConn
	}

	// Graceful shutdown will be blocked if there are alive connections.
	server, err = NewSQLServer(lg, cfg, nil, id.NewIDManager(), nil, nil, hsHandler, nil)
	require.NoError(t, err)
	clientConn := createClientConn()
	go func() {
		server.PreClose()
		finish <- struct{}{}
		require.NoError(t, server.Close())
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
	server, err = NewSQLServer(lg, cfg, nil, id.NewIDManager(), nil, nil, hsHandler, nil)
	require.NoError(t, err)
	createClientConn()
	go func() {
		server.PreClose()
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
	server, err := NewSQLServer(lg, cfg, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, nil)
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	var wg waitgroup.WaitGroup
	wg.Run(func() {
		// The listener should be open and handshake should proceed.
		_, port, err := net.SplitHostPort(server.listeners[0].Addr().String())
		require.NoError(t, err)
		mdb, err := sql.Open("mysql", fmt.Sprintf("root@tcp(localhost:%s)/test", port))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return mdb.Ping() != nil
		}, 3*time.Second, 10*time.Millisecond)
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
	server.PreClose()
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
	}, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, nil)
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	require.Len(t, server.listeners, 2)
	for _, listener := range server.listeners {
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	server.PreClose()
	require.NoError(t, server.Close())
	certManager.Close()
}

func TestPortRange(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	start, end := findFreePortRange(t, 3)
	server, err := NewSQLServer(lg, &config.Config{
		Proxy: config.ProxyServer{
			Addr:      fmt.Sprintf("127.0.0.1:%d", start),
			PortRange: []int{start, end},
		},
	}, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{}, nil)
	require.NoError(t, err)
	server.Run(context.Background(), nil)

	require.Len(t, server.listeners, 3)
	ports := make([]int, 0, len(server.listeners))
	for _, listener := range server.listeners {
		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		ports = append(ports, tcpAddr.Port)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	slices.Sort(ports)
	require.Equal(t, []int{start, start + 1, end}, ports)

	server.PreClose()
	require.NoError(t, server.Close())
	certManager.Close()
}

func TestConnAddrUsesActualListenerAddr(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	require.NoError(t, certManager.Init(&config.Config{}, lg, nil))

	var (
		addrMu   sync.Mutex
		connAddr string
	)
	handler := &mockHsHandler{
		getRouter: func(ctx backend.ConnContext, _ *pnet.HandshakeResp) (router.Router, error) {
			addrMu.Lock()
			connAddr, _ = ctx.Value(backend.ConnContextKeyConnAddr).(string)
			addrMu.Unlock()
			return nil, errors.New("no router")
		},
	}
	server, err := NewSQLServer(lg, &config.Config{
		Proxy: config.ProxyServer{
			Addr: "127.0.0.1:0",
		},
	}, certManager, id.NewIDManager(), nil, nil, handler, nil)
	require.NoError(t, err)
	server.Run(context.Background(), nil)
	defer func() {
		server.PreClose()
		require.NoError(t, server.Close())
		certManager.Close()
	}()

	_, port, err := net.SplitHostPort(server.listeners[0].Addr().String())
	require.NoError(t, err)
	mdb, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%s)/test", port))
	require.NoError(t, err)
	defer func() { require.NoError(t, mdb.Close()) }()

	require.ErrorContains(t, mdb.Ping(), "no router")
	require.Eventually(t, func() bool {
		addrMu.Lock()
		defer addrMu.Unlock()
		return connAddr == server.listeners[0].Addr().String()
	}, 3*time.Second, 10*time.Millisecond)
}

func TestWatchCfg(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hsHandler := backend.NewDefaultHandshakeHandler(nil)
	cfgch := make(chan *config.Config)
	server, err := NewSQLServer(lg, &config.Config{}, nil, id.NewIDManager(), nil, nil, hsHandler, nil)
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
	server.PreClose()
	require.NoError(t, server.Close())
}

func findFreePortRange(t *testing.T, size int) (start, end int) {
	t.Helper()
	for range 128 {
		probe, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		start = probe.Addr().(*net.TCPAddr).Port
		require.NoError(t, probe.Close())

		listeners := make([]net.Listener, 0, size)
		ok := true
		for port := start; port < start+size; port++ {
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				ok = false
				break
			}
			listeners = append(listeners, listener)
		}
		for _, listener := range listeners {
			require.NoError(t, listener.Close())
		}
		if ok {
			return start, start + size - 1
		}
	}
	t.Fatal("failed to find free contiguous ports")
	return 0, 0
}

func TestRecoverPanic(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	certManager := cert.NewCertManager()
	err := certManager.Init(&config.Config{}, lg, nil)
	require.NoError(t, err)
	server, err := NewSQLServer(lg, &config.Config{}, certManager, id.NewIDManager(), nil, nil, &mockHsHandler{
		handshakeResp: func(ctx backend.ConnContext, _ *pnet.HandshakeResp) error {
			if ctx.Value(backend.ConnContextKeyConnID).(uint64) == 1 {
				panic("HandleHandshakeResp panic")
			}
			return nil
		},
	}, nil)
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
	server.PreClose()
	require.NoError(t, server.Close())
	certManager.Close()
}

func TestPublicEndpoint(t *testing.T) {
	tests := []struct {
		publicEndpoints []string
		publicIps       []string
		privateIps      []string
	}{
		{
			publicIps:  []string{"137.84.2.178"},
			privateIps: []string{"10.10.10.10"},
		},
		{
			publicEndpoints: []string{"10.10.10.0/24"},
			publicIps:       []string{"137.84.2.178", "10.10.10.10"},
			privateIps:      []string{"10.10.20.10"},
		},
		{
			publicEndpoints: []string{"10.10.10.0/24", "10.10.20.10"},
			publicIps:       []string{"137.84.2.178", "10.10.10.10", "10.10.20.10"},
			privateIps:      []string{"10.10.20.11"},
		},
	}

	server, err := NewSQLServer(zap.NewNop(), &config.Config{}, nil, id.NewIDManager(), nil, nil, backend.NewDefaultHandshakeHandler(nil), nil)
	require.NoError(t, err)
	for i, test := range tests {
		cfg := &config.Config{}
		cfg.Proxy.PublicEndpoints = test.publicEndpoints
		server.reset(cfg)
		for j, ip := range test.publicIps {
			require.True(t, server.fromPublicEndpoint(&net.TCPAddr{IP: net.ParseIP(ip), Port: 1000}), "test %d %d", i, j)
		}
		for j, ip := range test.privateIps {
			require.False(t, server.fromPublicEndpoint(&net.TCPAddr{IP: net.ParseIP(ip), Port: 1000}), "test %d %d", i, j)
		}
		require.False(t, server.fromPublicEndpoint(nil))
	}
}

type mockHsHandler struct {
	backend.DefaultHandshakeHandler
	handshakeResp func(ctx backend.ConnContext, _ *pnet.HandshakeResp) error
	getRouter     func(ctx backend.ConnContext, _ *pnet.HandshakeResp) (router.Router, error)
}

type mockMemUsageProvider struct {
	reject             bool
	snapshot           mgrmem.UsageSnapshot
	threshold          float64
	connBufferMemDelta atomic.Int64
}

func (m *mockMemUsageProvider) ShouldRejectNewConn() (bool, mgrmem.UsageSnapshot, float64) {
	return m.reject, m.snapshot, m.threshold
}

func (m *mockMemUsageProvider) UpdateConnBufferMemory(delta int64) {
	m.connBufferMemDelta.Add(delta)
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
func (handler *mockHsHandler) GetRouter(ctx backend.ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	if handler.getRouter != nil {
		return handler.getRouter(ctx, resp)
	}
	return nil, errors.New("no router")
}
