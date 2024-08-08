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
	mbr := server.mgr.br.(*mockBackendReader)
	for _, tt := range tests {
		mbr.data.Store(string(tt.data))
		doHTTP(t, http.MethodGet, "/api/backend/metrics", nil, nil, func(t *testing.T, r *http.Response) {
			all, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, tt.expect, all)
			require.Equal(t, http.StatusOK, r.StatusCode)
		})
	}
}

type mockBackendReader struct {
	data atomic.String
}

func (mbr *mockBackendReader) GetBackendMetrics() []byte {
	return []byte(mbr.data.Load())
}
