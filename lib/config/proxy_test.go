// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

var testProxyConfig = Config{
	Workdir: "./wd",
	Advance: Advance{
		IgnoreWrongNamespace: true,
	},
	Proxy: ProxyServer{
		Addr:    "0.0.0.0:4000",
		PDAddrs: "127.0.0.1:4089",
		ProxyServerOnline: ProxyServerOnline{
			RequireBackendTLS:          true,
			MaxConnections:             1,
			FrontendKeepalive:          KeepAlive{Enabled: true},
			ProxyProtocol:              "v2",
			GracefulWaitBeforeShutdown: 10,
			ConnBufferSize:             32 * 1024,
		},
	},
	API: API{
		Addr: "0.0.0.0:3080",
	},
	Metrics: Metrics{
		MetricsAddr:     "127.0.0.1:9021",
		MetricsInterval: 15,
	},
	Log: Log{
		Encoder: "tidb",
		LogOnline: LogOnline{
			Level: "info",
			LogFile: LogFile{
				Filename:   ".",
				MaxSize:    10,
				MaxDays:    1,
				MaxBackups: 1,
			},
		},
	},
	Security: Security{
		ServerSQLTLS: TLSConfig{
			CA:        "a",
			Cert:      "b",
			Key:       "c",
			AutoCerts: true,
		},
		ServerHTTPTLS: TLSConfig{
			CA:        "a",
			Cert:      "b",
			Key:       "c",
			AutoCerts: true,
		},
		ClusterTLS: TLSConfig{
			CA:     "a",
			SkipCA: true,
			Cert:   "b",
			Key:    "c",
		},
		SQLTLS: TLSConfig{
			CA:                 "a",
			RSAKeySize:         0,
			AutoExpireDuration: "1y",
			SkipCA:             true,
			Cert:               "b",
			Key:                "c",
		},
	},
}

func TestProxyConfig(t *testing.T) {
	data1, err := testProxyConfig.ToBytes()
	require.NoError(t, err)
	var cfg Config
	err = toml.Unmarshal(data1, &cfg)
	require.NoError(t, err)
	data2, err := cfg.ToBytes()
	require.NoError(t, err)
	require.Equal(t, data1, data2)
}

func TestProxyCheck(t *testing.T) {
	testcases := []struct {
		pre  func(*testing.T, *Config)
		post func(*testing.T, *Config)
		err  error
	}{
		{
			pre: func(t *testing.T, c *Config) {
				c.Workdir = ""
			},
			post: func(t *testing.T, c *Config) {
				cwd, err := os.Getwd()
				require.NoError(t, err)
				require.Equal(t, filepath.Clean(filepath.Join(cwd, "work")), c.Workdir)
			},
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.ProxyProtocol = "v1"
			},
			err: ErrUnsupportedProxyProtocolVersion,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.ConnBufferSize = 100 * 1024 * 1024
			},
			err: ErrInvalidConfigValue,
		},
	}
	for _, tc := range testcases {
		cfg := testProxyConfig
		tc.pre(t, &cfg)
		if tc.err != nil {
			require.ErrorIs(t, cfg.Check(), tc.err)
			continue
		}
		require.NoError(t, cfg.Check())
		tc.post(t, &cfg)
	}
}
