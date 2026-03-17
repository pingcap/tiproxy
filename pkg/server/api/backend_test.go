// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestBackendMetrics(t *testing.T) {
	tests := []struct {
		data   []byte
		expect []byte
	}{
		{
			data:   []byte(`{"key": "value"}`),
			expect: []byte(`{"key": "value"}`),
		},
		{
			data:   []byte{},
			expect: []byte{},
		},
		{
			data:   nil,
			expect: []byte{},
		},
	}

	server, doHTTP := createServer(t)
	mbr := server.mgr.BackendReader.(*mockBackendReader)
	for _, tt := range tests {
		mbr.data.Store(string(tt.data))
		mbr.cluster.Store("")
		doHTTP(t, http.MethodGet, "/api/backend/metrics", httpOpts{}, func(t *testing.T, r *http.Response) {
			all, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, tt.expect, all)
			require.Equal(t, http.StatusOK, r.StatusCode)
			require.Empty(t, mbr.cluster.Load())
		})
	}
}

func TestBackendMetricsClusterQuery(t *testing.T) {
	server, doHTTP := createServer(t)
	mbr := server.mgr.BackendReader.(*mockBackendReader)
	mbr.data.Store(`{"key":"value"}`)

	doHTTP(t, http.MethodGet, "/api/backend/metrics?cluster=cluster-a", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"key":"value"}`), all)
		require.Equal(t, http.StatusOK, r.StatusCode)
		require.Equal(t, "cluster-a", mbr.cluster.Load())
	})
}

type mockBackendReader struct {
	data    atomic.String
	cluster atomic.String
}

func (mbr *mockBackendReader) GetBackendMetricsByCluster(cluster string) []byte {
	mbr.cluster.Store(cluster)
	return []byte(mbr.data.Load())
}
