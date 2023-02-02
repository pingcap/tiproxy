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

package router

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
)

type mockEventReceiver struct {
	backendChan chan map[string]BackendStatus
	errChan     chan error
}

func (mer *mockEventReceiver) OnBackendChanged(backends map[string]BackendStatus, err error) {
	if err != nil {
		mer.errChan <- err
	} else if len(backends) > 0 {
		mer.backendChan <- backends
	}
}

func newMockEventReceiver(backendChan chan map[string]BackendStatus, errChan chan error) *mockEventReceiver {
	return &mockEventReceiver{
		backendChan: backendChan,
		errChan:     errChan,
	}
}

func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:             true,
		Interval:           500 * time.Millisecond,
		MaxRetries:         3,
		RetryInterval:      100 * time.Millisecond,
		DialTimeout:        100 * time.Millisecond,
		TombstoneThreshold: 5 * time.Minute,
	}
}

// Test that the notified backend status is correct when the backend starts or shuts down.
func TestObserveBackends(t *testing.T) {
	runETCDTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		bo.Start()

		backend1 := addBackend(t, kv)
		checkStatus(t, backendChan, backend1, StatusHealthy)
		addFakeTopology(t, kv, backend1.sqlAddr)
		backend1.stopSQLServer()
		checkStatus(t, backendChan, backend1, StatusCannotConnect)
		backend1.startSQLServer()
		checkStatus(t, backendChan, backend1, StatusHealthy)
		backend1.setHTTPResp(false)
		checkStatus(t, backendChan, backend1, StatusCannotConnect)
		backend1.setHTTPResp(true)
		checkStatus(t, backendChan, backend1, StatusHealthy)
		backend1.stopHTTPServer()
		checkStatus(t, backendChan, backend1, StatusCannotConnect)
		backend1.startHTTPServer()
		checkStatus(t, backendChan, backend1, StatusHealthy)

		backend2 := addBackend(t, kv)
		checkStatus(t, backendChan, backend2, StatusHealthy)
		removeBackend(t, kv, backend2)
		checkStatus(t, backendChan, backend2, StatusCannotConnect)

		backend1.close()
		checkStatus(t, backendChan, backend1, StatusCannotConnect)
		backend2.close()
	})
}

// Test that the observer can exit during health check.
func TestCancelObserver(t *testing.T) {
	runETCDTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		backends := make([]*backendServer, 0, 3)
		for i := 0; i < 3; i++ {
			backends = append(backends, addBackend(t, kv))
		}
		backendInfo, err := bo.fetcher.GetBackendList(context.Background())
		require.NoError(t, err)
		require.Equal(t, 3, len(backendInfo))

		// Try 10 times.
		for i := 0; i < 10; i++ {
			childCtx, cancelFunc := context.WithCancel(context.Background())
			var wg waitgroup.WaitGroup
			wg.Run(func() {
				for childCtx.Err() != nil {
					bo.checkHealth(childCtx, backendInfo)
				}
			})
			time.Sleep(10 * time.Millisecond)
			cancelFunc()
			wg.Wait()
		}

		for _, backend := range backends {
			backend.close()
		}
	})
}

// Test that it won't hang or panic when the Etcd server fails.
func TestEtcdUnavailable(t *testing.T) {
	runETCDTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		bo.Start()
		etcd.Server.Stop()
		<-etcd.Server.StopNotify()
		// The observer will retry indefinitely. We verify it won't panic or hang here.
		// There's no other way than modifying the code or just sleeping.
		time.Sleep(bo.healthCheckConfig.Interval + bo.healthCheckConfig.DialTimeout + bo.healthCheckConfig.RetryInterval)
		require.Equal(t, 0, len(backendChan))
	})
}

// Test that the notified backend status is correct for external fetcher.
func TestExternalFetcher(t *testing.T) {
	backendAddrs := make([]string, 0)
	var observeError error
	var mutex sync.Mutex
	backendGetter := func() ([]string, error) {
		mutex.Lock()
		defer mutex.Unlock()
		return backendAddrs, observeError
	}
	addBackend := func() *backendServer {
		backend := newBackendServer(t)
		mutex.Lock()
		backendAddrs = append(backendAddrs, backend.sqlAddr)
		mutex.Unlock()
		return backend
	}
	removeBackend := func(backend *backendServer) {
		mutex.Lock()
		idx := slices.Index(backendAddrs, backend.sqlAddr)
		backendAddrs = append(backendAddrs[:idx], backendAddrs[idx+1:]...)
		mutex.Unlock()
	}
	mockError := func() {
		mutex.Lock()
		observeError = errors.New("mock observe error")
		mutex.Unlock()
	}

	backendChan := make(chan map[string]BackendStatus, 1)
	errChan := make(chan error, 1)
	mer := newMockEventReceiver(backendChan, errChan)
	fetcher := NewExternalFetcher(backendGetter)
	bo, err := NewBackendObserver(logger.CreateLoggerForTest(t), mer, nil, newHealthCheckConfigForTest(), fetcher)
	require.NoError(t, err)
	bo.Start()
	defer bo.Close()

	backend1 := addBackend()
	checkStatus(t, backendChan, backend1, StatusHealthy)
	backend2 := addBackend()
	checkStatus(t, backendChan, backend2, StatusHealthy)

	// remove from the list but it's still alive
	removeBackend(backend1)
	checkStatus(t, backendChan, backend1, StatusCannotConnect)
	backend1.close()

	// kill it but it's still in the list
	backend2.close()
	checkStatus(t, backendChan, backend2, StatusCannotConnect)
	removeBackend(backend2)

	// returns observe error
	require.Len(t, errChan, 0)
	mockError()
	err = <-errChan
	require.Error(t, err)
}

func runETCDTest(t *testing.T, f func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus)) {
	etcd := createEtcdServer(t, "127.0.0.1:0")
	client := createEtcdClient(t, etcd)
	kv := clientv3.NewKV(client)
	backendChan := make(chan map[string]BackendStatus, 1)
	mer := newMockEventReceiver(backendChan, make(chan error, 1))
	fetcher := NewPDFetcher(client, logger.CreateLoggerForTest(t), newHealthCheckConfigForTest())
	bo, err := NewBackendObserver(logger.CreateLoggerForTest(t), mer, nil, newHealthCheckConfigForTest(), fetcher)
	require.NoError(t, err)
	f(etcd, kv, bo, backendChan)
	bo.Close()
}

func checkStatus(t *testing.T, backendChan chan map[string]BackendStatus, backend *backendServer, expectedStatus BackendStatus) {
	backends := <-backendChan
	require.Equal(t, 1, len(backends))
	status, ok := backends[backend.sqlAddr]
	require.True(t, ok)
	require.Equal(t, expectedStatus, status)
	require.True(t, checkBackendStatusMetrics(backend.sqlAddr, status))
}

type backendServer struct {
	t            *testing.T
	sqlListener  net.Listener
	sqlAddr      string
	statusServer *http.Server
	statusAddr   string
	httpHandler  *mockHttpHandler
	wg           waitgroup.WaitGroup
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
}

func (handler *mockHttpHandler) setHTTPResp(succeed bool) {
	handler.httpOK.Store(succeed)
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !handler.httpOK.Load() {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (srv *backendServer) startHTTPServer() {
	if srv.httpHandler == nil {
		srv.httpHandler = &mockHttpHandler{
			t: srv.t,
		}
	}
	var statusListener net.Listener
	statusListener, srv.statusAddr = startListener(srv.t, srv.statusAddr)
	srv.statusServer = &http.Server{Addr: srv.statusAddr, Handler: srv.httpHandler}
	srv.wg.Run(func() {
		_ = srv.statusServer.Serve(statusListener)
	})
}

func (srv *backendServer) setHTTPResp(succeed bool) {
	srv.httpHandler.setHTTPResp(succeed)
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
