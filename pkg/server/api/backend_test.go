// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"io"
	"net/http"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestBackendMetrics(t *testing.T) {
	server, doHTTP := createServer(t, nil)
	mbr := server.mgr.br.(*mockBackendReader)
	mbr.data.Store(`{"key": "value"}`)

	doHTTP(t, http.MethodGet, "/api/backend/metrics", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, string(all), `{"key": "value"}`)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	mbr.err.Store(errors.New("mock error"))
	doHTTP(t, http.MethodGet, "/api/backend/metrics", nil, nil, func(t *testing.T, r *http.Response) {
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NotEqual(t, http.StatusOK, r.StatusCode)
	})
}

type mockBackendReader struct {
	data atomic.String
	err  atomic.Error
}

func (mbr *mockBackendReader) GetBackendMetrics() ([]byte, error) {
	return []byte(mbr.data.Load()), mbr.err.Load()
}
