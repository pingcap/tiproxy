package config

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

var testProxyConfig = Config{
	Workdir: "./wd",
	LCUrls:  []url.URL{},
	ACUrls:  []url.URL{},
	LPUrls:  []url.URL{},
	APUrls:  []url.URL{},
	Config: ConfigManager{
		IgnoreWrongNamespace: true,
	},
	Proxy: ProxyServer{
		Addr:           "0.0.0.0:4000",
		MaxConnections: 1,
		TCPKeepAlive:   true,
		PDAddrs:        "127.0.0.1:4089",
	},
	API: API{
		EnableBasicAuth: false,
		User:            "user",
		Password:        "pwd",
	},
	Metrics: Metrics{
		PromCluster: "ffgfg",
	},
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
	data, err := testProxyConfig.ToBytes()
	require.NoError(t, err)
	cfg, err := NewConfig(data)
	require.NoError(t, err)
	require.Equal(t, testProxyConfig, *cfg)
}
