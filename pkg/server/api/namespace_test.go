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
	_, doHTTP := createServer(t)

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
	doHTTP(t, http.MethodPost, "/api/admin/namespace/commit", httpOpts{}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
	})
}
