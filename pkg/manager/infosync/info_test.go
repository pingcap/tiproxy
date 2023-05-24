// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// TTL is refreshed periodically.
func TestTTLRefresh(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	var ttl string
	for i := 0; i < 10; i++ {
		require.Eventually(t, func() bool {
			newTTL, info := ts.getTTLAndInfo(tiproxyTopologyPath)
			satisfied := newTTL != ttl && len(info) > 0
			if satisfied {
				ttl = newTTL
			}
			return satisfied
		}, 3*time.Second, 100*time.Millisecond)
	}
}

// InfoSyncer continues refreshing even after etcd server is down.
func TestEtcdServerDown(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	var ttl string
	for i := 0; i < 5; i++ {
		// Make the server down for some time.
		ts.restartServer(time.Second)
		require.Eventually(t, func() bool {
			newTTL, info := ts.getTTLAndInfo(tiproxyTopologyPath)
			satisfied := newTTL != ttl && len(info) > 0
			if satisfied {
				ttl = newTTL
			}
			return satisfied
		}, 5*time.Second, 100*time.Millisecond)
	}
}

// TTL is erased after the client is down.
func TestClientDown(t *testing.T) {
	ts := newEtcdTestSuite(t)
	t.Cleanup(ts.close)
	require.Eventually(t, func() bool {
		ttl, info := ts.getTTLAndInfo(tiproxyTopologyPath)
		return len(ttl) > 0 && len(info) > 0
	}, 3*time.Second, 100*time.Millisecond)
	ts.closeInfoSyncer()
	require.Eventually(t, func() bool {
		ttl, _ := ts.getTTLAndInfo(tiproxyTopologyPath)
		return len(ttl) == 0
	}, 3*time.Second, 100*time.Millisecond)
}

type etcdTestSuite struct {
	t      *testing.T
	lg     *zap.Logger
	server *embed.Etcd
	client *clientv3.Client
	kv     clientv3.KV
	is     *InfoSyncer
}

func newEtcdTestSuite(t *testing.T) *etcdTestSuite {
	lg := logger.CreateLoggerForTest(t)
	etcd := createEtcdServer(t, lg, "0.0.0.0:0")
	client := createEtcdClient(t, lg, etcd)
	kv := clientv3.NewKV(client)
	is := NewInfoSyncer(client, lg)
	is.syncConfig = syncConfig{
		sessionTTL:    1,
		refreshIntvl:  50 * time.Millisecond,
		putTimeout:    30 * time.Millisecond,
		putRetryIntvl: 10 * time.Millisecond,
		putRetryCnt:   3,
	}
	err := is.Init(context.Background(), newConfig())
	require.NoError(t, err)
	return &etcdTestSuite{
		t:      t,
		lg:     lg,
		server: etcd,
		client: client,
		kv:     kv,
		is:     is,
	}
}

func (ts *etcdTestSuite) close() {
	if ts.is != nil {
		ts.is.Close()
		ts.is = nil
	}
	if ts.server != nil {
		ts.server.Close()
		ts.server = nil
	}
	if ts.client != nil {
		require.NoError(ts.t, ts.client.Close())
		ts.client = nil
	}
}

func (ts *etcdTestSuite) restartServer(waitTime time.Duration) {
	require.NotNil(ts.t, ts.server)
	addr := ts.server.Clients[0].Addr().String()
	ts.server.Close()
	time.Sleep(waitTime)
	ts.server = createEtcdServer(ts.t, ts.lg, addr)
}

func (ts *etcdTestSuite) closeInfoSyncer() {
	require.NotNil(ts.t, ts.is)
	ts.is.Close()
}

func (ts *etcdTestSuite) getTTLAndInfo(prefix string) (string, string) {
	var ttl, info string
	rs, err := ts.kv.Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(ts.t, err)
	for _, kv := range rs.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, TTLSuffix) {
			ttl = string(kv.Value)
		} else if strings.HasSuffix(key, InfoSuffix) {
			info = string(kv.Value)
		}
	}
	return ttl, info
}

func createEtcdServer(t *testing.T, lg *zap.Logger, addr string) *embed.Etcd {
	serverURL, err := url.Parse(fmt.Sprintf("http://%s", addr))
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LCUrls = []url.URL{*serverURL}
	cfg.LPUrls = []url.URL{*serverURL}
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(lg)
	cfg.LogLevel = "fatal"
	// Reuse port so that it can reboot with the same port immediately.
	cfg.SocketOpts = transport.SocketOpts{
		ReuseAddress: true,
		ReusePort:    true,
	}
	var etcd *embed.Etcd
	require.Eventually(t, func() bool {
		var err error
		etcd, err = embed.StartEtcd(cfg)
		return err == nil
	}, 3*time.Second, time.Second)
	<-etcd.Server.ReadyNotify()
	return etcd
}

func createEtcdClient(t *testing.T, lg *zap.Logger, etcd *embed.Etcd) *clientv3.Client {
	cfg := &config.Config{}
	certMgr := cert.NewCertManager()
	err := certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	lg = lg.WithOptions(zap.IncreaseLevel(zap.FatalLevel))
	client, err := InitEtcdClient(lg, []string{etcd.Clients[0].Addr().String()}, certMgr)
	require.NoError(t, err)
	return client
}

func newConfig() *config.Config {
	return &config.Config{
		Proxy: config.ProxyServer{
			Addr: "0.0.0.0:6000",
		},
		API: config.API{
			Addr: "0.0.0.0:3080",
		},
	}
}
