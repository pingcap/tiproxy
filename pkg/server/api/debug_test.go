// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"encoding/json"
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

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(false)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		require.Equal(t, "server is not ready", health["unhealthy_reason"])
	})

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(true)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	server.PreClose()
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		require.Equal(t, "server is closing", health["unhealthy_reason"])
	})
}

func TestDebugHealthManualOverride(t *testing.T) {
	server, doHTTP := createServer(t)
	assertHealth := func(statusCode int, unhealthyReason string) {
		doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
			require.Equal(t, statusCode, r.StatusCode)
			var health map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
			if unhealthyReason == "" {
				_, ok := health["unhealthy_reason"]
				require.False(t, ok)
			} else {
				require.Equal(t, unhealthyReason, health["unhealthy_reason"])
			}
		})
	}

	assertHealth(http.StatusOK, "")

	doHTTP(t, http.MethodPut, "/api/debug/health", httpOpts{
		reader: bytes.NewBufferString(`{"healthy":false}`),
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	assertHealth(http.StatusBadGateway, "")

	doHTTP(t, http.MethodDelete, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	assertHealth(http.StatusOK, "")

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(false)
	assertHealth(http.StatusBadGateway, "server is not ready")

	doHTTP(t, http.MethodPut, "/api/debug/health", httpOpts{
		reader: bytes.NewBufferString(`{"healthy":true,"reason":"manual-restore"}`),
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	assertHealth(http.StatusOK, "")

	doHTTP(t, http.MethodDelete, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	assertHealth(http.StatusBadGateway, "server is not ready")
}

func TestDebugHealthAllowsHTTPWithHTTPTLS(t *testing.T) {
	_, doHTTP, doHTTPS := createServerWithConfig(t, `security.server-http-tls.auto-certs = true`)

	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTPS(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTPS(t, http.MethodGet, "/api/metrics", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}
