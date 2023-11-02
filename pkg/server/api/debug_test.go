// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebug(t *testing.T) {
	closing := true
	_, doHTTP := createServer(t, &closing)

	doHTTP(t, http.MethodGet, "/api/debug/health", nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})

	closing = false
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPost, "/api/debug/redirect", nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/redirect", nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusNotFound, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/debug/pprof", nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}
