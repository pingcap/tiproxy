package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testProxyConfig = Proxy{
	Version: "v1",
	ProxyServer: ProxyServer{
		Addr:           "0.0.0.0:4000",
		MaxConnections: 1,
	},
	AdminServer: AdminServer{
		Addr:            "0.0.0.0:4001",
		EnableBasicAuth: false,
		User:            "user",
		Password:        "pwd",
	},
	Log: Log{
		Level:  "info",
		Format: "console",
		LogFile: LogFile{
			Filename:   ".",
			MaxSize:    10,
			MaxDays:    1,
			MaxBackups: 1,
		},
	},
	Registry: Registry{
		Enable: false,
		Type:   "etcd",
		Addrs:  []string{"127.0.0.1:4000", "127.0.0.1:4001"},
	},
	ConfigCenter: ConfigCenter{
		Type: "file",
		ConfigFile: ConfigFile{
			Path: ".",
		},
		ConfigEtcd: ConfigEtcd{
			Addrs: []string{},
		},
	},
	Performance: Performance{
		TCPKeepAlive: true,
	},
	Security: Security{
		ClusterVerifyCN: []string{},
	},
}

func TestProxyConfig(t *testing.T) {
	data, err := testProxyConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewProxyConfig(data)
	require.NoError(t, err)
	require.Equal(t, testProxyConfig, *cfg)
}
