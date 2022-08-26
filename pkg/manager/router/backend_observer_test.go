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
	"fmt"
	"net"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
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
		listener1, addr1 := checkAddBackend(t, kv, backendChan)
		addFakeTopology(t, kv, addr1)
		listener2, addr2 := checkAddBackend(t, kv, backendChan)
		checkBackendUnavailable(t, backendChan, listener1, addr1)
		checkRemoveBackend(t, kv, backendChan, listener2, addr2)
		listener1 = checkRestart(t, backendChan, addr1)
	})
}

// Test that the health of tombstone backends won't be checked.
func TestTombstoneBackends(t *testing.T) {
	runTest(t, func(etcd *embed.Etcd, kv clientv3.KV, bo *BackendObserver, backendChan chan map[string]BackendStatus) {
		// Do not start observer to avoid data race, just mock data.
		now := time.Now()
		bo.allBackendInfo = map[string]*BackendTTLInfo{
			"dead_addr": {
				ttl:        []byte("123456789"),
				lastUpdate: now.Add(-bo.config.tombstoneThreshold * 2),
			},
			"restart_addr": {
				ttl:        []byte("123456789"),
				lastUpdate: now.Add(-bo.config.tombstoneThreshold * 2),
			},
			"removed_addr": {
				ttl:        []byte("123456789"),
				lastUpdate: now,
			},
			"alive_addr": {
				ttl:        []byte("123456789"),
				lastUpdate: now,
			},
		}
		bo.curBackendInfo = map[string]BackendStatus{
			"dead_addr":    StatusCannotConnect,
			"removed_addr": StatusHealthy,
			"alive_addr":   StatusHealthy,
		}

		updateTTL(t, kv, "dead_addr", "123456789")
		updateTTL(t, kv, "restart_addr", "999999999")
		updateTTL(t, kv, "alive_addr", "999999999")
		updateTTL(t, kv, "new_addr", "999999999")

		backendsToBeChecked, err := bo.fetchBackendList(context.Background())
		require.NoError(t, err)
		// dead_addr and removed_addr won't be checked.
		require.Equal(t, map[string]BackendStatus{
			"restart_addr": StatusHealthy,
			"alive_addr":   StatusHealthy,
			"new_addr":     StatusHealthy,
		}, backendsToBeChecked)
		// All backends exist except removed_addr.
		require.Equal(t, 4, len(bo.allBackendInfo))
		checkAddrAndTTL := func(addr, ttl string) {
			info, ok := bo.allBackendInfo[addr]
			require.True(t, ok)
			require.Equal(t, []byte(ttl), info.ttl)
		}
		checkAddrAndTTL("dead_addr", "123456789")
		checkAddrAndTTL("restart_addr", "999999999")
		checkAddrAndTTL("alive_addr", "999999999")
		checkAddrAndTTL("new_addr", "999999999")
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
	backendChan := make(chan map[string]BackendStatus, 1)
	mer := newMockEventReceiver(backendChan)
	bo, err := NewBackendObserver(mer, client, newHealthCheckConfigForTest(), nil)
	require.NoError(t, err)
	kv := clientv3.NewKV(client)
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

func newListener(t *testing.T, addr string) (net.Listener, string) {
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	addr = listener.Addr().String()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// listener is closed
				break
			}
			_ = conn.Close()
		}
	}()
	return listener, addr
}

// A new healthy backend is added.
func checkAddBackend(t *testing.T, kv clientv3.KV, backendChan chan map[string]BackendStatus) (net.Listener, string) {
	listener, addr := newListener(t, "127.0.0.1:0")
	updateTTL(t, kv, addr, "123456789")
	checkStatus(t, backendChan, addr, StatusHealthy)
	return listener, addr
}

// Update the path prefix but it doesn't affect the topology.
func addFakeTopology(t *testing.T, kv clientv3.KV, addr string) {
	_, err := kv.Put(context.Background(), path.Join(infosync.TopologyInformationPath, addr, "/info"), "{}")
	require.NoError(t, err)
}

// The backend is still in Etcd but it's actually unavailable.
func checkBackendUnavailable(t *testing.T, backendChan chan map[string]BackendStatus, listener net.Listener, addr string) {
	require.NoError(t, listener.Close())
	checkStatus(t, backendChan, addr, StatusCannotConnect)
}

// Restart a backend that was alive.
func checkRestart(t *testing.T, backendChan chan map[string]BackendStatus, addr string) net.Listener {
	listener, _ := newListener(t, addr)
	checkStatus(t, backendChan, addr, StatusHealthy)
	return listener
}

// A backend is removed from Etcd.
func checkRemoveBackend(t *testing.T, kv clientv3.KV, backendChan chan map[string]BackendStatus, listener net.Listener, addr string) {
	_, err := kv.Delete(context.Background(), path.Join(infosync.TopologyInformationPath, addr, topologyPathSuffix))
	require.NoError(t, err)
	checkStatus(t, backendChan, addr, StatusCannotConnect)
	require.NoError(t, listener.Close())
}

func checkStatus(t *testing.T, backendChan chan map[string]BackendStatus, addr string, expectedStatus BackendStatus) {
	backends := <-backendChan
	require.Equal(t, 1, len(backends))
	status, ok := backends[addr]
	require.True(t, ok)
	require.Equal(t, expectedStatus, status)
}

// Update the TTL for a backend.
func updateTTL(t *testing.T, kv clientv3.KV, addr, ttl string) {
	_, err := kv.Put(context.Background(), path.Join(infosync.TopologyInformationPath, addr, topologyPathSuffix), ttl)
	require.NoError(t, err)
}
