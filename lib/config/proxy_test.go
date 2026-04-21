// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	mconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
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
			BackendClusters: []BackendCluster{
				{
					Name:      "cluster-a",
					PDAddrs:   "127.0.0.1:12379,127.0.0.1:22379",
					NSServers: []string{"10.0.0.2", "10.0.0.3"},
				},
				{
					Name:      "cluster-b",
					PDAddrs:   "127.0.0.1:32379",
					NSServers: []string{"10.0.0.4"},
				},
			},
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
	Metering: mconfig.MeteringConfig{
		Type:     storage.ProviderTypeAzure,
		Bucket:   "metering-container",
		Prefix:   "tiproxy/metering",
		Endpoint: "https://account.blob.core.windows.net",
		Azure: &mconfig.MeteringAzureConfig{
			AccountName: "account",
			SASToken:    "sig=example",
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
				c.Proxy.PortRange = []int{10000}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.PortRange = []int{10000, 9999}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.Addr = "0.0.0.0:6000,0.0.0.0:6001"
				c.Proxy.PortRange = []int{10000, 10001}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.BackendClusters = append(c.Proxy.BackendClusters, BackendCluster{})
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.BackendClusters = []BackendCluster{{Name: "c1", PDAddrs: ""}}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.BackendClusters = []BackendCluster{{Name: "c1", PDAddrs: "127.0.0.1"}}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.BackendClusters = []BackendCluster{
					{Name: "c1", PDAddrs: "127.0.0.1:2379"},
					{Name: "c1", PDAddrs: "127.0.0.1:2380"},
				}
			},
			err: ErrInvalidConfigValue,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.BackendClusters = []BackendCluster{{Name: "c1", PDAddrs: "127.0.0.1:2379", NSServers: []string{"10.0.0.1:abc"}}}
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

func TestGetSQLAddrs(t *testing.T) {
	cfg := NewConfig()
	cfg.Proxy.Addr = "0.0.0.0:6000"
	cfg.Proxy.PortRange = nil
	addrs, err := cfg.Proxy.GetSQLAddrs()
	require.NoError(t, err)
	require.Equal(t, []string{"0.0.0.0:6000"}, addrs)

	cfg.Proxy.PortRange = []int{10000, 10002}
	addrs, err = cfg.Proxy.GetSQLAddrs()
	require.NoError(t, err)
	require.Equal(t, []string{"0.0.0.0:10000", "0.0.0.0:10001", "0.0.0.0:10002"}, addrs)
}

func TestParseNSServers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     []string
		expected  []string
		expectErr bool
	}{
		{
			name:     "ipv4 default port",
			input:    []string{"10.0.0.1"},
			expected: []string{"10.0.0.1:53"},
		},
		{
			name:     "hostname explicit port",
			input:    []string{"dns.example.com:5353"},
			expected: []string{"dns.example.com:5353"},
		},
		{
			name:     "ipv6 default port",
			input:    []string{"2001:db8::1"},
			expected: []string{"[2001:db8::1]:53"},
		},
		{
			name:      "bracketed ipv6 without port is invalid",
			input:     []string{"[2001:db8::1]"},
			expectErr: true,
		},
		{
			name:      "invalid port",
			input:     []string{"10.0.0.1:abc"},
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addrs, err := ParseNSServers(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, addrs)
		})
	}
}

func TestCloneConfig(t *testing.T) {
	cfg := testProxyConfig
	cfg.Labels = map[string]string{"a": "b"}
	cfg.Proxy.PublicEndpoints = []string{"1.1.1.0/24"}
	clone := cfg.Clone()
	require.Equal(t, cfg, *clone)
	cfg.Labels["c"] = "d"
	cfg.Proxy.PublicEndpoints[0] = "2.2.2.0/24"
	cfg.Proxy.FailBackendList[0] = "db-tidb-9"
	cfg.Proxy.BackendClusters[0].Name = "cluster-updated"
	cfg.Proxy.BackendClusters[0].NSServers[0] = "10.0.0.9"
	require.NotContains(t, clone.Labels, "c")
	require.Equal(t, []string{"1.1.1.0/24"}, clone.Proxy.PublicEndpoints)
	require.Equal(t, []string{"db-tidb-0", "db-tidb-1"}, clone.Proxy.FailBackendList)
	require.Equal(t, "cluster-a", clone.Proxy.BackendClusters[0].Name)
	require.Equal(t, []string{"10.0.0.2", "10.0.0.3"}, clone.Proxy.BackendClusters[0].NSServers)
}

func TestGetBackendClusters(t *testing.T) {
	cfg := NewConfig()
	cfg.Proxy.PDAddrs = "127.0.0.1:2379,127.0.0.2:2379"
	cfg.Proxy.BackendClusters = nil

	clusters := cfg.GetBackendClusters()
	require.Len(t, clusters, 1)
	require.Equal(t, DefaultBackendClusterName, clusters[0].Name)
	require.Equal(t, cfg.Proxy.PDAddrs, clusters[0].PDAddrs)

	cfg.Proxy.BackendClusters = []BackendCluster{
		{Name: "cluster-a", PDAddrs: "127.0.0.3:2379"},
	}
	clusters = cfg.GetBackendClusters()
	require.Len(t, clusters, 1)
	require.Equal(t, "cluster-a", clusters[0].Name)
	require.Equal(t, "127.0.0.3:2379", clusters[0].PDAddrs)

	cfg.Proxy.BackendClusters = nil
	cfg.Proxy.PDAddrs = ""
	require.Nil(t, cfg.GetBackendClusters())
}
