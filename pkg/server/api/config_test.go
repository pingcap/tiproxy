// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	_, doHTTP := createServer(t)

	doHTTP(t, http.MethodGet, "/api/admin/config", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Contains(t, string(all), "addr = '0.0.0.0:6000'")
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Contains(t, string(all), `"addr":"0.0.0.0:6000"`)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("security.require-backend-tls = true"), nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	sum := ""
	sumreg := regexp.MustCompile(`{"config_checksum":(.+)}`)
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sum = string(sumreg.Find(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("proxy.require-back = false"), nil, func(t *testing.T, r *http.Response) {
		// no error
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, sum, string(sumreg.Find(all)))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("security.require-backend-tls = false"), nil, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NotEqual(t, sum, string(sumreg.Find(all)))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}

func TestAcceptType(t *testing.T) {
	_, doHTTP := createServer(t)
	checkRespContentType := func(expectedType string, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var cfg config.Config
		switch expectedType {
		case "json":
			require.Contains(t, r.Header.Get("Content-Type"), "application/json")
			require.NoError(t, json.Unmarshal(data, &cfg))
		default:
			require.Contains(t, r.Header.Get("Content-Type"), "application/toml")
			require.NoError(t, toml.Unmarshal(data, &cfg))
		}
	}
	doHTTP(t, http.MethodGet, "/api/admin/config", nil, nil, func(t *testing.T, r *http.Response) {
		checkRespContentType("toml", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config", nil, map[string]string{"Accept": "application/json"}, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config", nil, map[string]string{"Accept": "application/toml"}, func(t *testing.T, r *http.Response) {
		checkRespContentType("toml", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", nil, nil, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=JSON", nil, nil, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
}
