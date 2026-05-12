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
	})

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(true)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	server.PreClose()
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})
}

func TestDebugHealthManualUnhealthy(t *testing.T) {
	_, doHTTP := createServer(t)

	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		_, ok := health["unhealthy_reason"]
		require.False(t, ok)
	})

	doHTTP(t, http.MethodPut, "/api/debug/health", httpOpts{
		reader: bytes.NewBufferString(`{"healthy":false,"reason":"graceful-shutdown"}`),
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		require.Equal(t, "graceful-shutdown", health["unhealthy_reason"])
	})

	doHTTP(t, http.MethodDelete, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		_, ok := health["unhealthy_reason"]
		require.False(t, ok)
	})
}

func TestDebugHealthManualHealthyOverride(t *testing.T) {
	server, doHTTP := createServer(t)

	server.mgr.NsMgr.(*mockNamespaceManager).success.Store(false)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/debug/health", httpOpts{
		reader: bytes.NewBufferString(`{"healthy":true,"reason":"manual-restore"}`),
	}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		var health map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&health))
		_, ok := health["unhealthy_reason"]
		require.False(t, ok)
	})

	doHTTP(t, http.MethodDelete, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusBadGateway, r.StatusCode)
	})
}
