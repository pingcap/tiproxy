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
	_, doHTTP := createServer(t, nil)

	doHTTP(t, http.MethodGet, "/api/admin/config", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `
[proxy]
addr = '0.0.0.0:6000'
pd-addrs = '127.0.0.1:2379'
graceful-close-conn-timeout = 15

[proxy.frontend-keepalive]
enabled = true

[proxy.backend-healthy-keepalive]
enabled = true
idle = 60000000000
cnt = 5
intvl = 3000000000
timeout = 15000000000

[proxy.backend-unhealthy-keepalive]
enabled = true
idle = 10000000000
cnt = 5
intvl = 1000000000
timeout = 5000000000

[api]
addr = '0.0.0.0:3080'

[advance]
ignore-wrong-namespace = true

[security]
[security.server-tls]
min-tls-version = '1.2'

[security.server-http-tls]
min-tls-version = '1.2'

[security.cluster-tls]
min-tls-version = '1.2'

[security.sql-tls]
min-tls-version = '1.2'

[log]
encoder = 'tidb'
level = 'info'

[log.log-file]
max-size = 300
max-days = 3
max-backups = 3
`, string(regexp.MustCompile("workdir = '.+'\n").ReplaceAll(all, nil)))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", nil, nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"proxy":{"addr":"0.0.0.0:6000","pd-addrs":"127.0.0.1:2379","frontend-keepalive":{"enabled":true},"backend-healthy-keepalive":{"enabled":true,"idle":60000000000,"cnt":5,"intvl":3000000000,"timeout":15000000000},"backend-unhealthy-keepalive":{"enabled":true,"idle":10000000000,"cnt":5,"intvl":1000000000,"timeout":5000000000},"graceful-close-conn-timeout":15},"api":{"addr":"0.0.0.0:3080"},"advance":{"ignore-wrong-namespace":true},"security":{"server-tls":{"min-tls-version":"1.2"},"server-http-tls":{"min-tls-version":"1.2"},"cluster-tls":{"min-tls-version":"1.2"},"sql-tls":{"min-tls-version":"1.2"}},"log":{"encoder":"tidb","level":"info","log-file":{"max-size":300,"max-days":3,"max-backups":3}},"balance":{"label":{}}}`,
			string(regexp.MustCompile(`"workdir":"[^"]+",`).ReplaceAll(all, nil)))
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
	_, doHTTP := createServer(t, nil)
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
