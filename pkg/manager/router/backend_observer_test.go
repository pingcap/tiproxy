// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockEventReceiver struct {
	backendChan chan map[string]*backendHealth
	errChan     chan error
}

func (mer *mockEventReceiver) OnBackendChanged(backends map[string]*backendHealth, err error) {
	if err != nil {
		mer.errChan <- err
	} else if len(backends) > 0 {
		mer.backendChan <- backends
	}
}

func newMockEventReceiver(backendChan chan map[string]*backendHealth, errChan chan error) *mockEventReceiver {
	return &mockEventReceiver{
		backendChan: backendChan,
		errChan:     errChan,
	}
}

func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:        true,
		Interval:      300 * time.Millisecond,
		MaxRetries:    3,
		RetryInterval: 30 * time.Millisecond,
		DialTimeout:   100 * time.Millisecond,
	}
}

// Test that the notified backend status is correct when the backend starts or shuts down.
func TestObserveBackends(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)
	ts.bo.Start()

	backend1 := ts.addBackend()
	ts.checkStatus(backend1, StatusHealthy)
	backend1.stopSQLServer()
	ts.checkStatus(backend1, StatusCannotConnect)
	backend1.startSQLServer()
	ts.checkStatus(backend1, StatusHealthy)
	backend1.setHTTPResp(false)
	ts.checkStatus(backend1, StatusCannotConnect)
	backend1.setHTTPResp(true)
	ts.checkStatus(backend1, StatusHealthy)
	backend1.setHTTPWait(ts.bo.healthCheckConfig.DialTimeout + time.Second)
	ts.checkStatus(backend1, StatusCannotConnect)
	backend1.setHTTPWait(time.Duration(0))
	ts.checkStatus(backend1, StatusHealthy)
	backend1.stopHTTPServer()
	ts.checkStatus(backend1, StatusCannotConnect)
	backend1.startHTTPServer()
	ts.checkStatus(backend1, StatusHealthy)

	backend2 := ts.addBackend()
	ts.checkStatus(backend2, StatusHealthy)
	ts.removeBackend(backend2)
	ts.checkStatus(backend2, StatusCannotConnect)

	backend1.close()
	ts.checkStatus(backend1, StatusCannotConnect)
	backend2.close()
}

// Test that the health check can exit when the context is cancelled.
func TestCancelObserver(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	backends := make([]*backendServer, 0, 3)
	for i := 0; i < 3; i++ {
		backends = append(backends, ts.addBackend())
	}
	info, err := ts.fetcher.GetBackendList(context.Background())
	require.NoError(t, err)
	require.Len(t, info, 3)

	// Try 10 times.
	for i := 0; i < 10; i++ {
		childCtx, cancelFunc := context.WithCancel(context.Background())
		var wg waitgroup.WaitGroup
		wg.Run(func() {
			for childCtx.Err() != nil {
				ts.bo.checkHealth(childCtx, info)
			}
		})
		time.Sleep(10 * time.Millisecond)
		cancelFunc()
		wg.Wait()
	}

	for _, backend := range backends {
		backend.close()
	}
}

func TestReadServerVersion(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	backend1 := ts.addBackend()
	backend1.serverVersion.Store("1.0")
	ts.bo.Start()
	backends := ts.getBackendsFromCh()
	require.Equal(t, "1.0", backends[backend1.sqlAddr].serverVersion)
	backend1.stopSQLServer()
	ts.checkStatus(backend1, StatusCannotConnect)
	backend1.serverVersion.Store("2.0")
	backend1.startSQLServer()
	backends = ts.getBackendsFromCh()
	require.Equal(t, "2.0", backends[backend1.sqlAddr].serverVersion)
	backend1.close()
}

type observerTestSuite struct {
	t           *testing.T
	bo          *BackendObserver
	fetcher     *mockBackendFetcher
	backendChan chan map[string]*backendHealth
}

func newObserverTestSuite(t *testing.T) *observerTestSuite {
	backendChan := make(chan map[string]*backendHealth, 1)
	mer := newMockEventReceiver(backendChan, make(chan error, 1))
	fetcher := &mockBackendFetcher{
		backends: make(map[string]*BackendInfo),
	}
	bo, err := NewBackendObserver(logger.CreateLoggerForTest(t), mer, nil, newHealthCheckConfigForTest(), fetcher)
	require.NoError(t, err)
	return &observerTestSuite{
		t:           t,
		bo:          bo,
		fetcher:     fetcher,
		backendChan: backendChan,
	}
}

func (ts *observerTestSuite) close() {
	if ts.bo != nil {
		ts.bo.Close()
		ts.bo = nil
	}
}

func (ts *observerTestSuite) checkStatus(backend *backendServer, expectedStatus BackendStatus) {
	backends := ts.getBackendsFromCh()
	require.Equal(ts.t, 1, len(backends))
	health, ok := backends[backend.sqlAddr]
	require.True(ts.t, ok)
	require.Equal(ts.t, expectedStatus, health.status)
	require.True(ts.t, checkBackendStatusMetrics(backend.sqlAddr, health.status))
}

func (ts *observerTestSuite) getBackendsFromCh() map[string]*backendHealth {
	var backends map[string]*backendHealth
	select {
	case backends = <-ts.backendChan:
	case <-time.After(3 * time.Second):
		ts.t.Fatal("timeout")
	}
	return backends
}

func (ts *observerTestSuite) addBackend() *backendServer {
	backend := newBackendServer(ts.t)
	ts.fetcher.backends[backend.sqlAddr] = &BackendInfo{
		IP:         backend.ip,
		StatusPort: backend.statusPort,
	}
	return backend
}

func (ts *observerTestSuite) removeBackend(backend *backendServer) {
	delete(ts.fetcher.backends, backend.sqlAddr)
}

type backendServer struct {
	t             *testing.T
	sqlListener   net.Listener
	sqlAddr       string
	statusServer  *http.Server
	statusAddr    string
	serverVersion atomic.String
	*mockHttpHandler
	wg         waitgroup.WaitGroup
	ip         string
	statusPort uint
}

func newBackendServer(t *testing.T) *backendServer {
	backend := &backendServer{
		t: t,
	}
	backend.startHTTPServer()
	backend.setHTTPResp(true)
	backend.startSQLServer()
	return backend
}

type mockHttpHandler struct {
	t      *testing.T
	httpOK atomic.Bool
	wait   atomic.Int64
}

func (handler *mockHttpHandler) setHTTPResp(succeed bool) {
	handler.httpOK.Store(succeed)
}

func (handler *mockHttpHandler) setHTTPWait(wait time.Duration) {
	handler.wait.Store(int64(wait))
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wait := handler.wait.Load()
	if wait > 0 {
		time.Sleep(time.Duration(wait))
	}
	if handler.httpOK.Load() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (srv *backendServer) startHTTPServer() {
	if srv.mockHttpHandler == nil {
		srv.mockHttpHandler = &mockHttpHandler{
			t: srv.t,
		}
	}
	var statusListener net.Listener
	statusListener, srv.statusAddr = startListener(srv.t, srv.statusAddr)
	srv.ip, srv.statusPort = parseHostPort(srv.t, srv.statusAddr)
	srv.statusServer = &http.Server{Addr: srv.statusAddr, Handler: srv.mockHttpHandler}
	srv.wg.Run(func() {
		_ = srv.statusServer.Serve(statusListener)
	})
}

func (srv *backendServer) stopHTTPServer() {
	err := srv.statusServer.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) startSQLServer() {
	srv.sqlListener, srv.sqlAddr = startListener(srv.t, srv.sqlAddr)
	srv.wg.Run(func() {
		for {
			conn, err := srv.sqlListener.Accept()
			if err != nil {
				// listener is closed
				break
			}
			if err = pnet.WriteServerVersion(conn, srv.serverVersion.Load()); err != nil {
				break
			}
			_ = conn.Close()
		}
	})
}

func (srv *backendServer) stopSQLServer() {
	err := srv.sqlListener.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) close() {
	srv.stopHTTPServer()
	srv.stopSQLServer()
	srv.wg.Wait()
}

func startListener(t *testing.T, addr string) (net.Listener, string) {
	if len(addr) == 0 {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return listener, listener.Addr().String()
}

func parseHostPort(t *testing.T, addr string) (string, uint) {
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.ParseUint(port, 10, 32)
	require.NoError(t, err)
	return host, uint(p)
}

type mockBackendFetcher struct {
	backends map[string]*BackendInfo
}

func (mbf *mockBackendFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	return mbf.backends, nil
}

// ExternalFetcher fetches backend list from a given callback.
type ExternalFetcher struct {
	backendGetter func() ([]string, error)
}

func NewExternalFetcher(backendGetter func() ([]string, error)) *ExternalFetcher {
	return &ExternalFetcher{
		backendGetter: backendGetter,
	}
}

func (ef *ExternalFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	addrs, err := ef.backendGetter()
	return backendListToMap(addrs), err
}
