// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		RSAKeySize: 64,
		ServerTLS: TLSConfig{
			CA:        "a",
			Cert:      "b",
			Key:       "c",
			AutoCerts: true,
		},
		PeerTLS: TLSConfig{
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
			CA:     "a",
			SkipCA: true,
			Cert:   "b",
			Key:    "c",
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
