// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiproxy/lib/util/sys"
	"github.com/stretchr/testify/require"
)

var testProxyConfig = Config{
	Workdir: "./wd",
	Proxy: ProxyServer{
		Addr:    "0.0.0.0:4000",
		PDAddrs: "127.0.0.1:4089",
		ProxyServerOnline: ProxyServerOnline{
			MaxConnections:                 1,
			HighMemoryUsageRejectThreshold: 0.9,
			FrontendKeepalive:              KeepAlive{Enabled: true},
			ProxyProtocol:                  "v2",
			GracefulWaitBeforeShutdown:     10,
			FailBackendList:                []string{"db-tidb-0", "db-tidb-1"},
			FailoverTimeout:                60,
			ConnBufferSize:                 32 * 1024,
		},
	},
	API: API{
		Addr: "0.0.0.0:3080",
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
		RequireBackendTLS: true,
	},
	HA: HA{
		VirtualIP:        "10.10.10.10/32",
		Interface:        "eth0",
		GARPBurstCount:   5,
		GARPRefreshCount: 30,
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

<<<<<<< HEAD
=======
func TestConfigZeroValueOverridesRoundTrip(t *testing.T) {
	cfg := NewConfig()
	cfg.Proxy.Addr = ""
	cfg.Proxy.PDAddrs = ""
	cfg.Proxy.HighMemoryUsageRejectThreshold = 0
	cfg.Proxy.GracefulCloseConnTimeout = 0
	cfg.Proxy.FailoverTimeout = 0
	cfg.Proxy.FrontendKeepalive = KeepAlive{}
	cfg.Proxy.BackendHealthyKeepalive = KeepAlive{}
	cfg.Proxy.BackendUnhealthyKeepalive = KeepAlive{}
	cfg.API.Addr = ""
	cfg.Log.Level = ""
	cfg.Log.Encoder = ""
	cfg.Log.LogFile.MaxSize = 0
	cfg.Log.LogFile.MaxDays = 0
	cfg.Log.LogFile.MaxBackups = 0
	cfg.Balance.Health.Enabled = false
	cfg.Balance.Memory.Enabled = false
	cfg.Balance.CPU.Enabled = false
	cfg.Balance.Location.Enabled = false
	cfg.HA.GARPRefreshCount = 0
	cfg.EnableTrafficReplay = false
	require.NoError(t, cfg.Check())

	t.Run("toml", func(t *testing.T) {
		data, err := cfg.ToBytes()
		require.NoError(t, err)
		decoded := NewConfig()
		require.NoError(t, toml.Unmarshal(data, decoded))
		require.Equal(t, cfg, decoded)
	})

	t.Run("json", func(t *testing.T) {
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		decoded := NewConfig()
		require.NoError(t, json.Unmarshal(data, decoded))
		require.Equal(t, cfg, decoded)
	})
}

func TestProxyConfigCOS(t *testing.T) {
	data := []byte(`
[metering]
type = "cos"
region = "ap-beijing"
bucket = "metering-1234567890"
prefix = "tiproxy/metering"

[metering.cos]
assume-role-arn = "qcs::cam::uin/1234567890:roleName/metering"
access-key = "access-key"
secret-access-key = "secret-access-key"
session-token = "session-token"
`)
	var cfg Config
	require.NoError(t, toml.Unmarshal(data, &cfg))
	require.Equal(t, mconfig.MeteringConfig{
		Type:   storage.ProviderTypeCOS,
		Region: "ap-beijing",
		Bucket: "metering-1234567890",
		Prefix: "tiproxy/metering",
		COS: &mconfig.MeteringCOSConfig{
			AssumeRoleARN:   "qcs::cam::uin/1234567890:roleName/metering",
			AccessKey:       "access-key",
			SecretAccessKey: "secret-access-key",
			SessionToken:    "session-token",
		},
	}, cfg.Metering)

	providerCfg := cfg.Metering.ToProviderConfig()
	require.Equal(t, storage.ProviderTypeCOS, providerCfg.Type)
	require.Equal(t, "ap-beijing", providerCfg.Region)
	require.Equal(t, "metering-1234567890", providerCfg.Bucket)
	require.Equal(t, "tiproxy/metering", providerCfg.Prefix)
	require.Equal(t, &storage.COSConfig{
		AssumeRoleARN:   "qcs::cam::uin/1234567890:roleName/metering",
		AccessKey:       "access-key",
		SecretAccessKey: "secret-access-key",
		SessionToken:    "session-token",
	}, providerCfg.COS)
}

>>>>>>> 31158a3d (config, balance: add switch configs to disable balance factors (#1218))
func TestProxyCheck(t *testing.T) {
	testcases := []struct {
		pre  func(*testing.T, *Config)
		post func(*testing.T, *Config)
		err  error
	}{
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.HighMemoryUsageRejectThreshold = -0.1
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.HighMemoryUsageRejectThreshold = 1.1
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.HighMemoryUsageRejectThreshold = 0.4
			},
			post: func(t *testing.T, c *Config) {
				require.Equal(t, 0.5, c.Proxy.HighMemoryUsageRejectThreshold)
			},
		},
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
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.FailBackendList = []string{"db-tidb-0", " "}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.FailoverTimeout = -1
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.HA.GARPBurstCount = -1
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.HA.GARPRefreshCount = -1
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

func TestGetIPPort(t *testing.T) {
	for _, cas := range []struct {
		addr          string
		advertiseAddr string
		port          string
		nonUnicast    bool
	}{
		{":34", "", "34", true},
		{"0.0.0.0:34", "", "34", true},
		{"255.255.255.255:34", "", "34", true},
		{"239.255.255.255:34", "", "34", true},
		{"[FF02::1:FF47]:34", "", "34", true},
		{"127.0.0.1:34", "", "34", false},
		{"[F02::1:FF47]:34", "", "34", false},
		{"192.0.0.1:6049", "", "6049", false},
		{"0.0.0.0:1000", "tc-tiproxy-0.tc-tiproxy-peer.ns.svc", "1000", false},
	} {
		cfg := &Config{
			Proxy: ProxyServer{
				Addr:          cas.addr,
				AdvertiseAddr: cas.advertiseAddr,
			},
			API: API{
				Addr: cas.addr,
			},
		}
		ip, port, statusPort, err := cfg.GetIPPort()
		require.NoError(t, err)

		expectedIP := cas.advertiseAddr
		if len(expectedIP) == 0 {
			expectedIP, _, err = net.SplitHostPort(cas.addr)
			require.NoError(t, err)
			if cas.nonUnicast {
				expectedIP = sys.GetGlobalUnicastIP()
			}
		}
		require.Equal(t, expectedIP, ip)
		require.Equal(t, cas.port, port)
		require.Equal(t, cas.port, statusPort)
	}
}

func TestCloneConfig(t *testing.T) {
	cfg := testProxyConfig
	cfg.Labels = map[string]string{"a": "b"}
	clone := cfg.Clone()
	require.Equal(t, cfg, *clone)
	cfg.Labels["c"] = "d"
	cfg.Proxy.FailBackendList[0] = "db-tidb-9"
	require.NotContains(t, clone.Labels, "c")
	require.Equal(t, []string{"db-tidb-0", "db-tidb-1"}, clone.Proxy.FailBackendList)
}
