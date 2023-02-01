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
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// Test that the health of tombstone backends won't be checked.
func TestTombstoneBackends(t *testing.T) {
	etcd := createEtcdServer(t, "127.0.0.1:0")
	client := createEtcdClient(t, etcd)
	kv := clientv3.NewKV(client)
	pf := NewPDFetcher(client, logger.CreateLoggerForTest(t), newHealthCheckConfigForTest())

	now := time.Now()
	oldTTL, newTTL := []byte("123456789"), []byte("999999999")
	pf.backendInfo = map[string]*pdBackendInfo{
		"dead_addr": {
			TopologyInfo: &infosync.TopologyInfo{},
			ttl:          oldTTL,
			lastUpdate:   now.Add(-pf.config.TombstoneThreshold * 2),
		},
		"restart_addr": {
			TopologyInfo: &infosync.TopologyInfo{},
			ttl:          oldTTL,
			lastUpdate:   now.Add(-pf.config.TombstoneThreshold * 2),
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

	pf.fetchBackendList(context.Background())
	// removed_addr doesn't exist.
	require.Equal(t, 6, len(pf.backendInfo))
	checkAddrAndTTL := func(addr string, ttl []byte) {
		info, ok := pf.backendInfo[addr]
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
	backendStatus := pf.filterTombstoneBackends()
	checkAddr := func(addr string) {
		_, ok := backendStatus[addr]
		require.True(t, ok)
	}
	checkAddr("restart_addr")
	checkAddr("alive_addr")
	checkAddr("new_addr")
}

func createEtcdServer(t *testing.T, addr string) *embed.Etcd {
	serverURL, err := url.Parse(fmt.Sprintf("http://%s", addr))
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LCUrls = []url.URL{*serverURL}
	cfg.LPUrls = []url.URL{*serverURL}
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger.CreateLoggerForTest(t))
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
	certMgr := cert.NewCertManager()
	err := certMgr.Init(cfg, logger.CreateLoggerForTest(t))
	require.NoError(t, err)
	client, err := InitEtcdClient(logger.CreateLoggerForTest(t), cfg, certMgr)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
}

// A new healthy backend is added to etcd.
func addBackend(t *testing.T, kv clientv3.KV) *backendServer {
	backend := newBackendServer(t)
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
