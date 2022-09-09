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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"
)

type mockEventReceiver struct {
	backendChan chan map[string]BackendStatus
}

func (mer *mockEventReceiver) OnBackendChanged(backends map[string]BackendStatus) {
	mer.backendChan <- backends
}

func newMockEventReceiver(backendChan chan map[string]BackendStatus) *mockEventReceiver {
	return &mockEventReceiver{
		backendChan: backendChan,
	}
}

func newHealthCheckConfigForTest() *HealthCheckConfig {
	return &HealthCheckConfig{
		healthCheckInterval:      500 * time.Millisecond,
		healthCheckMaxRetries:    healthCheckMaxRetries,
		healthCheckRetryInterval: 100 * time.Millisecond,
		healthCheckTimeout:       100 * time.Millisecond,
		tombstoneThreshold:       tombstoneThreshold,
	}
}

// Test that the notified backend status is correct when the backend starts or shuts down.
func TestObserveBackends(t *testing.T) {
	runTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		bo.Start()

		backend1 := addBackend(t, kv, backendChan)
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

		backend2 := addBackend(t, kv, backendChan)
		checkStatus(t, backendChan, backend2, StatusHealthy)
		removeBackend(t, kv, backend2)
		checkStatus(t, backendChan, backend2, StatusCannotConnect)

		backend1.close()
		checkStatus(t, backendChan, backend1, StatusCannotConnect)
		backend2.close()
	})
}

// Test that the health of tombstone backends won't be checked.
func TestTombstoneBackends(t *testing.T) {
	runTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		// Do not start observer to avoid data race, just mock data.
		now := time.Now()
		oldTTL, newTTL := []byte("123456789"), []byte("999999999")
		bo.allBackendInfo = map[string]*BackendInfo{
			"dead_addr": {
				TopologyInfo: &infosync.TopologyInfo{},
				ttl:          oldTTL,
				lastUpdate:   now.Add(-bo.config.tombstoneThreshold * 2),
			},
			"restart_addr": {
				TopologyInfo: &infosync.TopologyInfo{},
				ttl:          oldTTL,
				lastUpdate:   now.Add(-bo.config.tombstoneThreshold * 2),
			},
			"removed_addr": {
				TopologyInfo: &infosync.TopologyInfo{},
				ttl:          oldTTL,
				lastUpdate:   now,
			},
			"alive_addr": {
				TopologyInfo: &infosync.TopologyInfo{},
				ttl:          oldTTL,
				lastUpdate:   now,
			},
		}
		bo.curBackendInfo = map[string]BackendStatus{
			"dead_addr":    StatusCannotConnect,
			"removed_addr": StatusHealthy,
			"alive_addr":   StatusHealthy,
		}

		updateTTL(t, kv, "dead_addr", oldTTL)
		updateTopologyInfo(t, kv, "dead_addr", "dead_addr:0")
		updateTTL(t, kv, "restart_addr", newTTL)
		updateTopologyInfo(t, kv, "restart_addr", "restart_addr:0")
		updateTTL(t, kv, "alive_addr", newTTL)
		updateTopologyInfo(t, kv, "alive_addr", "alive_addr:0")
		updateTTL(t, kv, "new_addr", newTTL)
		updateTopologyInfo(t, kv, "new_addr", "new_addr:0")
		updateTTL(t, kv, "no_tp_addr", newTTL)
		updateTopologyInfo(t, kv, "no_ttl_addr", "no_ttl_addr:0")

		err := bo.fetchBackendList(context.Background())
		require.NoError(t, err)
		// removed_addr doesn't exist.
		require.Equal(t, 6, len(bo.allBackendInfo))
		checkAddrAndTTL := func(addr string, ttl []byte) {
			info, ok := bo.allBackendInfo[addr]
			require.True(t, ok)
			require.Equal(t, ttl, info.ttl)
		}
		checkAddrAndTTL("dead_addr", oldTTL)
		checkAddrAndTTL("restart_addr", newTTL)
		checkAddrAndTTL("alive_addr", newTTL)
		checkAddrAndTTL("new_addr", newTTL)
		checkAddrAndTTL("no_tp_addr", newTTL)
		checkAddrAndTTL("no_ttl_addr", nil)

		// dead_addr and removed_addr won't be checked.
		backendStatus := bo.filterTombstoneBackends()
		checkAddr := func(addr string) {
			_, ok := backendStatus[addr]
			require.True(t, ok)
		}
		checkAddr("restart_addr")
		checkAddr("alive_addr")
		checkAddr("new_addr")
	})
}

// Test that the observer can exit during health check.
func TestCancelObserver(t *testing.T) {
	runTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		backends := make([]*backendServer, 0, 3)
		for i := 0; i < 3; i++ {
			backends = append(backends, addBackend(t, kv, backendChan))
		}
		err := bo.fetchBackendList(context.Background())
		require.NoError(t, err)

		// Try 10 times.
		for i := 0; i < 10; i++ {
			childCtx, cancelFunc := context.WithCancel(context.Background())
			var wg waitgroup.WaitGroup
			wg.Run(func() {
				for childCtx.Err() != nil {
					bo.checkHealth(childCtx, bo.allBackendInfo)
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
	runTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		bo.Start()
		etcd.Server.Stop()
		<-etcd.Server.StopNotify()
		// The observer will retry indefinitely. We verify it won't panic or hang here.
		// There's no other way than modifying the code or just sleeping.
		time.Sleep(bo.config.healthCheckInterval + bo.config.healthCheckTimeout + bo.config.healthCheckRetryInterval)
		require.Equal(t, 0, len(backendChan))
	})
}

func runTest(t *testing.T, f func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus)) {
	etcd := createEtcdServer(t, "127.0.0.1:0")
	client := createEtcdClient(t, etcd)
	kv := clientv3.NewKV(client)
	backendChan := make(chan map[string]BackendStatus, 1)
	mer := newMockEventReceiver(backendChan)
	bo, err := NewBackendObserver(mer, client, newHealthCheckConfigForTest(), nil)
	require.NoError(t, err)
	f(etcd, kv, bo, backendChan)
	bo.Close()
}

func createEtcdServer(t *testing.T, addr string) *embed.Etcd {
	serverURL, err := url.Parse(fmt.Sprintf("http://%s", addr))
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LCUrls = []url.URL{*serverURL}
	cfg.LPUrls = []url.URL{*serverURL}
	cfg.LogLevel = "fatal"
	etcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	<-etcd.Server.ReadyNotify()
	t.Cleanup(etcd.Close)
	return etcd
}

func createEtcdClient(t *testing.T, etcd *embed.Etcd) *clientv3.Client {
	cfg := &config.Config{
		Proxy: config.ProxyServer{
			PDAddrs: etcd.Clients[0].Addr().String(),
		},
	}
	client, err := InitEtcdClient(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
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

// A new healthy backend is added.
func addBackend(t *testing.T, kv clientv3.KV, backendChan chan map[string]BackendStatus) *backendServer {
	backend := &backendServer{
		t: t,
	}
	backend.startHTTPServer()
	backend.setHTTPResp(true)
	backend.startSQLServer()
	updateTTL(t, kv, backend.sqlAddr, []byte("123456789"))
	updateTopologyInfo(t, kv, backend.sqlAddr, backend.statusAddr)
	return backend
}

// Update the path prefix but it doesn't affect the topology.
func addFakeTopology(t *testing.T, kv clientv3.KV, addr string) {
	_, err := kv.Put(context.Background(), path.Join(infosync.TopologyInformationPath, addr, "/fake"), "{}")
	require.NoError(t, err)
}

// A backend is removed from Etcd.
func removeBackend(t *testing.T, kv clientv3.KV, backend *backendServer) {
	_, err := kv.Delete(context.Background(), path.Join(infosync.TopologyInformationPath, backend.sqlAddr, ttlPathSuffix))
	require.NoError(t, err)
	_, err = kv.Delete(context.Background(), path.Join(infosync.TopologyInformationPath, backend.sqlAddr, infoPathSuffix))
	require.NoError(t, err)
}

func checkStatus(t *testing.T, backendChan chan map[string]BackendStatus, backend *backendServer, expectedStatus BackendStatus) {
	backends := <-backendChan
	require.Equal(t, 1, len(backends))
	status, ok := backends[backend.sqlAddr]
	require.True(t, ok)
	require.Equal(t, expectedStatus, status)
	require.True(t, checkBackendStatusMetrics(backend.sqlAddr, status))
}

// Update the TTL for a backend.
func updateTTL(t *testing.T, kv clientv3.KV, addr string, ttl []byte) {
	_, err := kv.Put(context.Background(), path.Join(infosync.TopologyInformationPath, addr, ttlPathSuffix), string(ttl))
	require.NoError(t, err)
}

// Update the TopologyInfo for a backend.
func updateTopologyInfo(t *testing.T, kv clientv3.KV, sqlAddr, statusAddr string) {
	hostAndPort := strings.Split(statusAddr, ":")
	require.Equal(t, 2, len(hostAndPort))
	port, err := strconv.ParseUint(hostAndPort[1], 10, 32)
	require.NoError(t, err)
	topology := &infosync.TopologyInfo{
		IP:         hostAndPort[0],
		StatusPort: uint(port),
	}
	data, err := json.Marshal(topology)
	require.NoError(t, err)
	_, err = kv.Put(context.Background(), path.Join(infosync.TopologyInformationPath, sqlAddr, infoPathSuffix), string(data))
	require.NoError(t, err)
}
