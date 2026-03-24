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

	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Contains(t, string(all), "addr = '0.0.0.0:6000'")
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Contains(t, string(all), `"addr":"0.0.0.0:6000"`)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader("security.require-backend-tls = true")}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	sum := ""
	sumreg := regexp.MustCompile(`{"config_checksum":(.+)}`)
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sum = string(sumreg.Find(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader("proxy.require-back = false")}, func(t *testing.T, r *http.Response) {
		// no error
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, sum, string(sumreg.Find(all)))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader("security.require-backend-tls = false")}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NotEqual(t, sum, string(sumreg.Find(all)))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(`
[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "127.0.0.1:2379"
ns-servers = ["10.0.0.1"]

[[proxy.backend-clusters]]
name = "cluster-b"
pd-addrs = "127.0.0.2:2379"
ns-servers = ["10.0.0.2", "10.0.0.3"]
`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		var cfg config.Config
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(all, &cfg))
		require.Len(t, cfg.Proxy.BackendClusters, 2)
		require.Equal(t, "cluster-a", cfg.Proxy.BackendClusters[0].Name)
		require.Equal(t, "127.0.0.1:2379", cfg.Proxy.BackendClusters[0].PDAddrs)
		require.Equal(t, []string{"10.0.0.1"}, cfg.Proxy.BackendClusters[0].NSServers)
		require.Equal(t, "cluster-b", cfg.Proxy.BackendClusters[1].Name)
		require.Equal(t, "127.0.0.2:2379", cfg.Proxy.BackendClusters[1].PDAddrs)
		require.Equal(t, []string{"10.0.0.2", "10.0.0.3"}, cfg.Proxy.BackendClusters[1].NSServers)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(`
[[proxy.backend-clusters]]
name = "cluster-d"
pd-addrs = "127.0.0.4:2379"
ns-servers = ["10.0.0.4:abc"]
`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusInternalServerError, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		var cfg config.Config
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(all, &cfg))
		require.Len(t, cfg.Proxy.BackendClusters, 2)
		require.Equal(t, "cluster-a", cfg.Proxy.BackendClusters[0].Name)
		require.Equal(t, "cluster-b", cfg.Proxy.BackendClusters[1].Name)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(`
[[proxy.backend-clusters]]
name = "cluster-c"
pd-addrs = "127.0.0.3:2379"
`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		var cfg config.Config
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(all, &cfg))
		require.Len(t, cfg.Proxy.BackendClusters, 1)
		require.Equal(t, "cluster-c", cfg.Proxy.BackendClusters[0].Name)
		require.Equal(t, "127.0.0.3:2379", cfg.Proxy.BackendClusters[0].PDAddrs)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(`
[proxy]
backend-clusters = []
`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		var cfg config.Config
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(all, &cfg))
		require.Empty(t, cfg.Proxy.BackendClusters)
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
	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{}, func(t *testing.T, r *http.Response) {
		checkRespContentType("toml", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{header: map[string]string{"Accept": "application/json"}}, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{header: map[string]string{"Accept": "application/toml"}}, func(t *testing.T, r *http.Response) {
		checkRespContentType("toml", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=JSON", httpOpts{}, func(t *testing.T, r *http.Response) {
		checkRespContentType("json", r)
	})
}
