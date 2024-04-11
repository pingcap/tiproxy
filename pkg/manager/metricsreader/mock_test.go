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
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
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
	getRespBody atomic.Pointer[func(reqBody string) string]
	wg          waitgroup.WaitGroup
	t           *testing.T
	server      *http.Server
}

func (handler *mockHttpHandler) Start() int {
	statusListener, addr := startListener(handler.t, "")
	_, port := parseHostPort(handler.t, addr)
	handler.server = &http.Server{Addr: addr, Handler: handler}
	handler.wg.Run(func() {
		_ = handler.server.Serve(statusListener)
	})
	return int(port)
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
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
