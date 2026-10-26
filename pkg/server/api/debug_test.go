// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pingcap/tiproxy/pkg/manager/health"
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

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(false)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(true)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	server.mgr.Health.(*health.Manager).PreClose()
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		require.Equal(t, "server is shutting down", health["unhealthy_reason"])
	})
}
