// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebug(t *testing.T) {
	server, doHTTP := createServer(t)

	doHTTP(t, http.MethodPost, "/api/debug/redirect", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/redirect", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusNotFound, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/debug/pprof", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	server.PreClose()
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})
}
