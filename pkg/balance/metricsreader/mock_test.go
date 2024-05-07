// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
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
