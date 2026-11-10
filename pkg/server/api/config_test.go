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
	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader("balance.cpu.enabled = false")}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		cfg := config.NewConfig()
		require.NoError(t, toml.Unmarshal(all, cfg))
		require.False(t, cfg.Balance.CPU.Enabled)
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		all, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		cfg := config.NewConfig()
		require.NoError(t, json.Unmarshal(all, cfg))
		require.False(t, cfg.Balance.CPU.Enabled)
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
}

func TestConfigGetOmitsCloudSecrets(t *testing.T) {
	srv, doHTTP := createServer(t)
	const secret = "secret-access-key-value"
	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(`
[metering]
type = "cos"
bucket = "metering-bucket"
[metering.cos]
assume-role-arn = "qcs::cam::uin/1:roleName/metering"
access-key = "access-key-value"
secret-access-key = "` + secret + `"
session-token = "session-token-value"
`)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})

	require.Equal(t, secret, srv.mgr.CfgMgr.GetConfig().Metering.COS.SecretAccessKey)

	var publicBody string
	doHTTP(t, http.MethodGet, "/api/admin/config", httpOpts{}, func(t *testing.T, r *http.Response) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		publicBody = string(body)
		require.Equal(t, http.StatusOK, r.StatusCode)
		require.Contains(t, publicBody, "metering-bucket")
		require.Contains(t, publicBody, "qcs::cam::uin/1:roleName/metering")
		require.NotContains(t, publicBody, secret)
		require.NotContains(t, publicBody, "access-key-value")
		require.NotContains(t, publicBody, "session-token-value")
	})
	doHTTP(t, http.MethodGet, "/api/admin/config?format=json", httpOpts{}, func(t *testing.T, r *http.Response) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode)
		require.NotContains(t, string(body), secret)
		require.Contains(t, string(body), "metering-bucket")
	})

	doHTTP(t, http.MethodPut, "/api/admin/config", httpOpts{reader: strings.NewReader(publicBody)}, func(t *testing.T, r *http.Response) {
		require.Equal(t, http.StatusOK, r.StatusCode)
	})
	require.Equal(t, secret, srv.mgr.CfgMgr.GetConfig().Metering.COS.SecretAccessKey)
	require.Equal(t, "metering-bucket", srv.mgr.CfgMgr.GetConfig().Metering.Bucket)
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
