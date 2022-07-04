// Copyright 2020 Ipalfish, Inc.
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

	"github.com/stretchr/testify/assert"
)

var testNamespaceConfig = Namespace{
	Version:   "v1",
	Namespace: "test_ns",
	Frontend: FrontendNamespace{
		AllowedDBs:  []string{"db0", "db1"},
		SlowSQLTime: 10,
		DeniedIPs:   []string{"127.0.0.0", "128.0.0.0"},
		Usernames:   []string{"user0", "user1"},
	},
	Backend: BackendNamespace{
		Instances:    []string{"127.0.0.1:4000", "127.0.0.1:4001"},
		SelectorType: "random",
	},
}

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
	},
	Performance: Performance{
		TCPKeepAlive: true,
	},
}

func TestNamespaceConfigEncodeAndDecode(t *testing.T) {
	data, err := MarshalNamespaceConfig(&testNamespaceConfig)
	assert.NoError(t, err)
	cfg, err := UnmarshalNamespaceConfig(data)
	assert.NoError(t, err)
	assert.Equal(t, testNamespaceConfig, *cfg)
}

func TestProxyConfigEncodeAndDecode(t *testing.T) {
	data, err := MarshalProxyConfig(&testProxyConfig)
	assert.NoError(t, err)
	cfg, err := UnmarshalProxyConfig(data)
	assert.NoError(t, err)
	assert.Equal(t, testProxyConfig, *cfg)
}
