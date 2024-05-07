// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"go.uber.org/atomic"
)

type mockTpFetcher struct {
	t     *testing.T
	infos map[string]*infosync.TiDBInfo
	err   error
}

func newMockTpFetcher(t *testing.T) *mockTpFetcher {
	return &mockTpFetcher{
		t: t,
	}
}

func (ft *mockTpFetcher) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBInfo, error) {
	return ft.infos, ft.err
}

type mockBackendFetcher struct {
	sync.Mutex
	backends map[string]*BackendInfo
}

func newMockBackendFetcher() *mockBackendFetcher {
	return &mockBackendFetcher{
		backends: make(map[string]*BackendInfo),
	}
}

func (mbf *mockBackendFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	mbf.Lock()
	defer mbf.Unlock()
	backends := make(map[string]*BackendInfo, len(mbf.backends))
	for addr, backend := range mbf.backends {
		backends[addr] = backend
	}
	return backends, nil
}

func (mbf *mockBackendFetcher) setBackend(addr string, info *BackendInfo) {
	mbf.Lock()
	defer mbf.Unlock()
	mbf.backends[addr] = info
}

func (mbf *mockBackendFetcher) removeBackend(addr string) {
	mbf.Lock()
	defer mbf.Unlock()
	delete(mbf.backends, addr)
}

type mockHealthCheck struct {
	sync.Mutex
	backends map[string]*BackendHealth
}

func newMockHealthCheck() *mockHealthCheck {
	return &mockHealthCheck{
		backends: make(map[string]*BackendHealth),
	}
}

func (mhc *mockHealthCheck) Check(_ context.Context, addr string, _ *BackendInfo) *BackendHealth {
	mhc.Lock()
	defer mhc.Unlock()
	return mhc.backends[addr]
}

func (mhc *mockHealthCheck) setBackend(addr string, health *BackendHealth) {
	mhc.Lock()
	defer mhc.Unlock()
	mhc.backends[addr] = health
}

func (mhc *mockHealthCheck) setHealth(addr string, healthy bool) {
	mhc.Lock()
	defer mhc.Unlock()
	health := *mhc.backends[addr]
	health.Healthy = healthy
	mhc.backends[addr] = &health
}

func (mhc *mockHealthCheck) removeBackend(addr string) {
	mhc.Lock()
	defer mhc.Unlock()
	delete(mhc.backends, addr)
}

type mockHttpHandler struct {
	t        *testing.T
	httpOK   atomic.Bool
	respBody atomic.String
	wait     atomic.Int64
}

func (handler *mockHttpHandler) setHTTPResp(succeed bool) {
	handler.httpOK.Store(succeed)
}

func (handler *mockHttpHandler) setHTTPRespBody(body string) {
	handler.respBody.Store(body)
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
		_, _ = w.Write([]byte(handler.respBody.Load()))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
