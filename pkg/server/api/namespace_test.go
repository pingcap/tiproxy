// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespace(t *testing.T) {
	srv, doHTTP := createServer(t)

	// test list
	doHTTP(t, http.MethodGet, "/api/admin/namespace", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `""`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	// test set
	doHTTP(t, http.MethodPut, "/api/admin/namespace/gg", httpOpts{reader: strings.NewReader(`{}`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/namespace", httpOpts{reader: strings.NewReader(`{"namespace": "dge"}`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	// test get
	doHTTP(t, http.MethodGet, "/api/admin/namespace/dge", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"namespace":"dge","frontend":{"user":"","security":{}},"backend":{"instances":null,"security":{}}}`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	// test remove
	doHTTP(t, http.MethodDelete, "/api/admin/namespace/dge", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/namespace/dge", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
	})

	// test commit
	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit?namespace=xx", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
	})
	srv.mgr.NsMgr.(*mockNamespaceManager).success.Store(true)
	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}

func TestNamespaceCommitPropagatesDeletion(t *testing.T) {
	srv, doHTTP := createServer(t)
	nsMgr := srv.mgr.NsMgr.(*mockNamespaceManager)

	doHTTP(t, http.MethodPut, "/api/admin/namespace/keep", httpOpts{reader: strings.NewReader(`{"namespace":"keep","frontend":{"user":"keep-user"}}`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/namespace/drop", httpOpts{reader: strings.NewReader(`{"namespace":"drop","frontend":{"user":"drop-user"}}`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodDelete, "/api/admin/namespace/drop", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	nsMgr.mu.Lock()
	nsMgr.live = map[string]bool{"keep": true, "drop": true}
	nsMgr.mu.Unlock()

	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit?namespace=drop", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	call := nsMgr.lastCommit()
	require.Equal(t, []string{"drop"}, call.names)
	require.Equal(t, []bool{true}, call.delete)
	require.False(t, call.deleteNil)

	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit?namespace=keep", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	call = nsMgr.lastCommit()
	require.Equal(t, []string{"keep"}, call.names)
	require.Equal(t, []bool{false}, call.delete)

	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	call = nsMgr.lastCommit()
	require.Equal(t, []string{"keep"}, call.names)
	require.True(t, call.deleteNil)

	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit?namespace=missing", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
	})
}
