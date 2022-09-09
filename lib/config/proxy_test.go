package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testProxyConfig = Config{
	Workdir: "./wd",
	Advance: Advance{
		PeerPort:             "343",
		IgnoreWrongNamespace: true,
		WatchInterval:        "30m",
	},
	Proxy: ProxyServer{
		Addr:    "0.0.0.0:4000",
		PDAddrs: "127.0.0.1:4089",
		ProxyServerOnline: ProxyServerOnline{
			MaxConnections: 1,
			TCPKeepAlive:   true,
		},
	},
	API: API{
		Addr:            "0.0.0.0:3080",
		EnableBasicAuth: false,
		User:            "user",
		Password:        "pwd",
	},
	Metrics: Metrics{},
	Log: Log{
		Level:   "info",
		Encoder: "tidb",
		LogFile: LogFile{
			Filename:   ".",
			MaxSize:    10,
			MaxDays:    1,
			MaxBackups: 1,
		},
	},
	Security: Security{
		RSAKeySize: 64,
		Server: TLSCert{
			CA:   "a",
			Cert: "b",
			Key:  "c",
		},
		Cluster: TLSCert{
			CA:   "a",
			Cert: "b",
			Key:  "c",
		},
	},
}

func TestProxyConfig(t *testing.T) {
	data1, err := testProxyConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewConfig(data1)
	require.NoError(t, err)
	data2, err := cfg.ToBytes()
	require.NoError(t, err)
	require.Equal(t, data1, data2)
}
