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

func TestConfig(t *testing.T) {
	_, doHTTP := createServer(t, nil)

	doHTTP(t, http.MethodGet, "/api/admin/config", nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `workdir = '/Users/xhe/pingcap/tiproxy/pkg/server/api/work'

[proxy]
addr = '0.0.0.0:6000'
pd-addrs = '127.0.0.1:2379'
require-backend-tls = true

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
min-tls-version = '1.1'

[security.peer-tls]
min-tls-version = '1.1'

[security.cluster-tls]
min-tls-version = '1.1'

[security.sql-tls]
min-tls-version = '1.1'

[log]
encoder = 'tidb'
level = 'info'

[log.log-file]
max-size = 300
max-days = 3
max-backups = 3
`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"proxy":{"addr":"0.0.0.0:6000","pd-addrs":"127.0.0.1:2379","require-backend-tls":true,"frontend-keepalive":{"enabled":true},"backend-healthy-keepalive":{"enabled":true,"idle":60000000000,"cnt":5,"intvl":3000000000,"timeout":15000000000},"backend-unhealthy-keepalive":{"enabled":true,"idle":10000000000,"cnt":5,"intvl":1000000000,"timeout":5000000000}},"api":{"addr":"0.0.0.0:3080"},"advance":{"ignore-wrong-namespace":true},"workdir":"/Users/xhe/pingcap/tiproxy/pkg/server/api/work","security":{"server-tls":{"min-tls-version":"1.1"},"peer-tls":{"min-tls-version":"1.1"},"cluster-tls":{"min-tls-version":"1.1"},"sql-tls":{"min-tls-version":"1.1"}},"metrics":{"metrics-addr":"","metrics-interval":0},"log":{"encoder":"tidb","level":"info","log-file":{"max-size":300,"max-days":3,"max-backups":3}}}`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("proxy.require-backend-tls = false"), func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"config_checksum":1822233951}`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("proxy.require-back = false"), func(t *testing.T, r *http.Response) {
		// no error
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"config_checksum":1822233951}`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodPut, "/api/admin/config", strings.NewReader("proxy.require-backend-tls = true"), func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/debug/health", nil, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"config_checksum":1977313839}`, string(all))
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
}
