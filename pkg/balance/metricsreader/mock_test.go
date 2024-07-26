// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

type mockPromFetcher struct {
	getPromInfo func(ctx context.Context) (*infosync.PrometheusInfo, error)
}

func (mpf *mockPromFetcher) GetPromInfo(ctx context.Context) (*infosync.PrometheusInfo, error) {
	return mpf.getPromInfo(ctx)
}

func newMockPromFetcher(port int) *mockPromFetcher {
	return &mockPromFetcher{
		getPromInfo: func(ctx context.Context) (*infosync.PrometheusInfo, error) {
			return &infosync.PrometheusInfo{
				IP:   "127.0.0.1",
				Port: port,
			}, nil
		},
	}
}

var _ TopologyFetcher = (*mockBackendFetcher)(nil)

type mockBackendFetcher struct {
	infos map[string]*infosync.TiDBTopologyInfo
	err   error
}

func (mbf *mockBackendFetcher) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	return mbf.infos, mbf.err
}

func newMockBackendFetcher(infos map[string]*infosync.TiDBTopologyInfo, err error) *mockBackendFetcher {
	return &mockBackendFetcher{
		infos: infos,
		err:   err,
	}
}

type mockHttpHandler struct {
	statusCode  atomic.Int32
	getRespBody atomic.Pointer[func(reqBody string) string]
	wg          waitgroup.WaitGroup
	t           *testing.T
	server      *http.Server
}

func newMockHttpHandler(t *testing.T) *mockHttpHandler {
	handler := &mockHttpHandler{
		t: t,
	}
	handler.statusCode.Store(http.StatusOK)
	return handler
}

func (handler *mockHttpHandler) Start() int {
	statusListener, addr := testkit.StartListener(handler.t, "")
	_, port := testkit.ParseHostPort(handler.t, addr)
	handler.server = &http.Server{Addr: addr, Handler: handler}
	handler.wg.Run(func() {
		_ = handler.server.Serve(statusListener)
	})
	return int(port)
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	code := int(handler.statusCode.Load())
	w.WriteHeader(code)
	if code != http.StatusOK {
		return
	}
	body, err := io.ReadAll(r.Body)
	require.NoError(handler.t, err)
	respBody := (*handler.getRespBody.Load())(string(body))
	require.True(handler.t, len(respBody) > 0, string(body))
	_, err = w.Write([]byte(respBody))
	require.NoError(handler.t, err)
}

func (handler *mockHttpHandler) Close() {
	require.NoError(handler.t, handler.server.Close())
	handler.wg.Wait()
}

var _ policy.BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	observer.BackendInfo
	addr string
}

func newMockBackend(addr string, ip string, port uint) *mockBackend {
	return &mockBackend{
		BackendInfo: observer.BackendInfo{
			IP:         ip,
			StatusPort: port,
		},
		addr: addr,
	}
}

func (mb *mockBackend) Healthy() bool {
	return false
}

func (mb *mockBackend) ConnScore() int {
	return 0
}

func (mb *mockBackend) ConnCount() int {
	return 0
}

func (mb *mockBackend) Addr() string {
	return mb.addr
}

func (mb *mockBackend) GetBackendInfo() observer.BackendInfo {
	return mb.BackendInfo
}

func (mb *mockBackend) Local() bool {
	return true
}

func mockMfs() map[string]*dto.MetricFamily {
	floats := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	return map[string]*dto.MetricFamily{
		"name1": {Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &floats[0]}}}},
		"name2": {Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: &floats[1]}}}},
	}
}

type mockConfigGetter struct {
	cfg *config.Config
}

func newMockConfigGetter(cfg *config.Config) *mockConfigGetter {
	return &mockConfigGetter{
		cfg: cfg,
	}
}

func (cfgGetter *mockConfigGetter) GetConfig() *config.Config {
	return cfgGetter.cfg
}

type etcdTestSuite struct {
	t      *testing.T
	lg     *zap.Logger
	server *embed.Etcd
	client *clientv3.Client
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
	return ts
}

func (ts *etcdTestSuite) close() {
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
	etcd, err := etcd.CreateEtcdServer(addr, ts.t.TempDir(), ts.lg)
	require.NoError(ts.t, err)
	ts.server = etcd
}

func (ts *etcdTestSuite) putKV(key, value string) {
	_, err := ts.client.Put(context.Background(), key, value)
	require.NoError(ts.t, err)
}

func (ts *etcdTestSuite) getKV(key string) []*mvccpb.KeyValue {
	resp, err := ts.client.Get(context.Background(), key, clientv3.WithPrefix())
	require.NoError(ts.t, err)
	return resp.Kvs
}

func (ts *etcdTestSuite) delKV(key string) {
	_, err := ts.client.Delete(context.Background(), key, clientv3.WithPrefix())
	require.NoError(ts.t, err)
}
