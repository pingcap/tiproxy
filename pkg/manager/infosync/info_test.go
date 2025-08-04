// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"encoding/json"
	"net"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/sys"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// TTL is refreshed periodically and info stays the same.
func TestTTLRefresh(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	var ttl, info string
	for i := 0; i < 10; i++ {
		require.Eventually(t, func() bool {
			newTTL, newInfo := ts.getTTLAndInfo(tiproxyTopologyPath)
			satisfied := newTTL != ttl && len(newInfo) > 0
			if satisfied {
				ttl = newTTL
				if len(info) > 0 {
					require.Equal(ts.t, info, newInfo)
				} else {
					info = newInfo
				}
			}
			return satisfied
		}, 10*time.Second, 100*time.Millisecond)
	}
}

// InfoSyncer continues refreshing even after etcd server is down.
func TestEtcdServerDown4Sync(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	var ttl string
	// Make the server down for some time.
	addr := ts.shutdownServer()
	time.Sleep(time.Second)
	ts.startServer(addr)
	require.Eventually(t, func() bool {
		newTTL, info := ts.getTTLAndInfo(tiproxyTopologyPath)
		satisfied := newTTL != ttl && len(info) > 0
		if satisfied {
			ttl = newTTL
		}
		return satisfied
	}, 10*time.Second, 100*time.Millisecond)
}

// TTL and info are erased after the client shuts down normally.
func TestClientShutDown4Sync(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	require.Eventually(t, func() bool {
		ttl, info := ts.getTTLAndInfo(tiproxyTopologyPath)
		return len(ttl) > 0 && len(info) > 0
	}, 10*time.Second, 100*time.Millisecond)
	ts.closeInfoSyncer()
	ttl, info := ts.getTTLAndInfo(tiproxyTopologyPath)
	require.True(t, len(ttl) == 0 && len(info) == 0)
}

// TTL and info are erased after the client is down accidentally.
func TestClientDown4Sync(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	require.Eventually(t, func() bool {
		ttl, info := ts.getTTLAndInfo(tiproxyTopologyPath)
		return len(ttl) > 0 && len(info) > 0
	}, 10*time.Second, 100*time.Millisecond)
	ts.stopInfoSyncer()
	require.Eventually(t, func() bool {
		ttl, info := ts.getTTLAndInfo(tiproxyTopologyPath)
		return len(ttl) == 0 && len(info) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

// Test that the result of GetTiDBTopology is right.
func TestFetchTiDBTopology(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)

	tests := []struct {
		update func()
		check  func(info map[string]*TiDBTopologyInfo)
	}{
		{
			// No backends.
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Empty(t, info)
			},
		},
		{
			// Only update TTL.
			update: func() {
				ts.updateTTL("1.1.1.1:4000", "", []byte("123456789"))
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 0)
			},
		},
		{
			// Then update info.
			update: func() {
				ts.updateInfo("1.1.1.1:4000", "", &TiDBTopologyInfo{
					IP:         "1.1.1.1",
					StatusPort: 10080,
				})
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 1)
				require.NotNil(ts.t, info["1.1.1.1:4000"])
				require.Equal(ts.t, "1.1.1.1", info["1.1.1.1:4000"].IP)
				require.Equal(ts.t, uint(10080), info["1.1.1.1:4000"].StatusPort)
			},
		},
		{
			// Add another backend.
			update: func() {
				ts.updateTTL("2.2.2.2:4000", "", []byte("123456789"))
				ts.updateInfo("2.2.2.2:4000", "", &TiDBTopologyInfo{
					IP:         "2.2.2.2",
					StatusPort: 10080,
				})
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 2)
				require.NotNil(ts.t, info["2.2.2.2:4000"])
				require.Equal(ts.t, "2.2.2.2", info["2.2.2.2:4000"].IP)
				require.Equal(ts.t, uint(10080), info["2.2.2.2:4000"].StatusPort)
			},
		},
		{
			// Remove the backend TTL.
			update: func() {
				ts.deleteTTL("2.2.2.2:4000", "")
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 1)
				require.Contains(ts.t, info, "1.1.1.1:4000")
			},
		},
		{
			// Add another keyspace backend.
			update: func() {
				ts.updateTTL("3.3.3.3:4000", "test", []byte("123456789"))
				ts.updateInfo("3.3.3.3:4000", "test", &TiDBTopologyInfo{
					IP:         "3.3.3.3",
					StatusPort: 10080,
				})
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 2)
				require.NotNil(ts.t, info["3.3.3.3:4000"])
				require.Equal(ts.t, "3.3.3.3", info["3.3.3.3:4000"].IP)
				require.Equal(ts.t, uint(10080), info["3.3.3.3:4000"].StatusPort)
				require.Equal(ts.t, "test", info["3.3.3.3:4000"].Keyspace)
			},
		},
		{
			// Remove the keyspace backend TTL.
			update: func() {
				ts.deleteTTL("3.3.3.3:4000", "test")
			},
			check: func(info map[string]*TiDBTopologyInfo) {
				require.Len(ts.t, info, 1)
				require.Contains(ts.t, info, "1.1.1.1:4000")
			},
		},
	}

	for i, test := range tests {
		if test.update != nil {
			test.update()
		}
		info, err := ts.is.GetTiDBTopology(context.Background())
		require.NoError(t, err, "case %d", i)
		test.check(info)
	}
}

func TestGetTopology(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	for _, cas := range []struct {
		addr          string
		advertiseAddr string
		port          string
		nonUnicast    bool
	}{
		{":34", "", "34", true},
		{"0.0.0.0:34", "", "34", true},
		{"255.255.255.255:34", "", "34", true},
		{"239.255.255.255:34", "", "34", true},
		{"[FF02::1:FF47]:34", "", "34", true},
		{"127.0.0.1:34", "", "34", false},
		{"[F02::1:FF47]:34", "", "34", false},
		{"192.0.0.1:6049", "", "6049", false},
		{"0.0.0.0:1000", "tc-tiproxy-0.tc-tiproxy-peer.ns.svc", "1000", false},
	} {
		is, err := ts.is.getTopologyInfo(&config.Config{
			Proxy: config.ProxyServer{
				Addr:          cas.addr,
				AdvertiseAddr: cas.advertiseAddr,
			},
			API: config.API{
				Addr: cas.addr,
			},
		})
		require.NoError(t, err)
		ip := cas.advertiseAddr
		if len(ip) == 0 {
			ip, _, err = net.SplitHostPort(cas.addr)
			require.NoError(t, err)
			if cas.nonUnicast {
				ip = sys.GetGlobalUnicastIP()
			}
		}
		require.Equal(t, ip, is.IP)
		require.Equal(t, cas.port, is.Port)
		require.Equal(t, cas.port, is.StatusPort)
	}
}

func TestGetPromInfo(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	info, err := ts.is.GetPromInfo(context.Background())
	require.ErrorIs(t, err, ErrNoProm)
	require.Nil(t, info)

	pInfo := &PrometheusInfo{
		IP:         "111.111.111.111",
		BinaryPath: "/bin",
		Port:       9090,
	}
	ts.setPromInfo(pInfo)
	info, err = ts.is.GetPromInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, pInfo, info)

	ts.shutdownServer()
	_, err = ts.is.GetPromInfo(context.Background())
	require.Error(t, err)
}

// Test that fetching retries when etcd server is down until the server is up again.
func TestEtcdServerDown4Fetch(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	addr := ts.shutdownServer()

	var wg waitgroup.WaitGroup
	wg.Run(func() {
		info, err := ts.is.GetTiDBTopology(context.Background())
		require.NoError(t, err)
		require.Len(ts.t, info, 0)
	})

	time.Sleep(time.Second)
	ts.startServer(addr)
	wg.Wait()
}

type etcdTestSuite struct {
	t      *testing.T
	lg     *zap.Logger
	server *embed.Etcd
	client *clientv3.Client
	kv     clientv3.KV
	is     *InfoSyncer
	cancel context.CancelFunc
}

func newEtcdTestSuite(t *testing.T) *etcdTestSuite {
	lg, _ := logger.CreateLoggerForTest(t)
	ts := &etcdTestSuite{
		t:  t,
		lg: lg,
	}

	ts.startServer("0.0.0.0:0")
	endpoint := ts.server.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)

	certMgr := cert.NewCertManager()
	err := certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	ts.client, err = etcd.InitEtcdClient(ts.lg, cfg, certMgr)
	require.NoError(t, err)
	ts.kv = clientv3.NewKV(ts.client)

	is := NewInfoSyncer(lg, ts.client)
	is.syncConfig = syncConfig{
		sessionTTL:        1,
		refreshIntvl:      50 * time.Millisecond,
		putTimeout:        1 * time.Second,
		putRetryIntvl:     10 * time.Millisecond,
		putRetryCnt:       3,
		getPromTimeout:    100 * time.Millisecond,
		getPromRetryIntvl: 0,
		getPromRetryCnt:   2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = is.Init(ctx, cfg)
	require.NoError(t, err)
	ts.is = is
	ts.cancel = cancel
	return ts
}

func (ts *etcdTestSuite) close() {
	if ts.is != nil {
		if err := ts.is.Close(); err != nil {
			require.ErrorIs(ts.t, err, context.DeadlineExceeded)
		}
		ts.is = nil
		ts.cancel()
	}
	if ts.client != nil {
		require.NoError(ts.t, ts.client.Close())
		ts.client = nil
	}
	if ts.server != nil {
		ts.server.Close()
		ts.server = nil
	}
}

func (ts *etcdTestSuite) startServer(addr string) {
	ts.createEtcdServer(addr)
}

func (ts *etcdTestSuite) shutdownServer() string {
	require.NotNil(ts.t, ts.server)
	addr := ts.server.Clients[0].Addr().String()
	ts.server.Close()
	ts.server = nil
	return addr
}

func (ts *etcdTestSuite) closeInfoSyncer() {
	require.NotNil(ts.t, ts.is)
	require.NoError(ts.t, ts.is.Close())
	ts.is = nil
}

func (ts *etcdTestSuite) stopInfoSyncer() {
	ts.cancel()
}

func (ts *etcdTestSuite) getTTLAndInfo(prefix string) (string, string) {
	var ttl, info string
	rs, err := ts.kv.Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(ts.t, err)
	for _, kv := range rs.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, ttlSuffix) {
			ttl = string(kv.Value)
		} else if strings.HasSuffix(key, infoSuffix) {
			info = string(kv.Value)
		}
	}
	return ttl, info
}

// Update the TTL for a backend.
func (ts *etcdTestSuite) updateTTL(addr, keyspace string, ttl []byte) {
	p := path.Join(tidbTopologyInformationPath, addr, ttlSuffix)
	if keyspace != "" {
		p = path.Join(tidbKeyspaceTopologyInformationPath, keyspace, p)
	}
	_, err := ts.kv.Put(context.Background(), p, string(ttl))
	require.NoError(ts.t, err)
}

func (ts *etcdTestSuite) deleteTTL(addr, keyspace string) {
	p := path.Join(tidbTopologyInformationPath, addr, ttlSuffix)
	if keyspace != "" {
		p = path.Join(tidbKeyspaceTopologyInformationPath, keyspace, p)
	}
	_, err := ts.kv.Delete(context.Background(), p)
	require.NoError(ts.t, err)
}

// Update the TopologyInfo for a backend.
func (ts *etcdTestSuite) updateInfo(sqlAddr, keyspace string, info *TiDBTopologyInfo) {
	data, err := json.Marshal(info)
	require.NoError(ts.t, err)
	p := path.Join(tidbTopologyInformationPath, sqlAddr, infoSuffix)
	if keyspace != "" {
		p = path.Join(tidbKeyspaceTopologyInformationPath, keyspace, p)
	}
	_, err = ts.kv.Put(context.Background(), p, string(data))
	require.NoError(ts.t, err)
}

func (ts *etcdTestSuite) setPromInfo(info *PrometheusInfo) {
	data, err := json.Marshal(info)
	require.NoError(ts.t, err)
	_, err = ts.kv.Put(context.Background(), promTopologyPath, string(data))
	require.NoError(ts.t, err)
}

func (ts *etcdTestSuite) createEtcdServer(addr string) {
	etcd, err := etcd.CreateEtcdServer(addr, ts.t.TempDir(), ts.lg)
	require.NoError(ts.t, err)
	ts.server = etcd
}
