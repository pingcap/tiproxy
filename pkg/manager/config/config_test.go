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
	"context"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestProxyConfig(t *testing.T) {
	cases := []*config.ProxyServerOnline{
		{
			MaxConnections: 1,
			TCPKeepAlive:   false,
		},
		{
			MaxConnections: 1,
			TCPKeepAlive:   true,
		},
		{
			MaxConnections: 0,
			TCPKeepAlive:   false,
		},
		{
			MaxConnections: 0,
			TCPKeepAlive:   true,
		},
	}

	testWatch(t, cases, func(cfgmgr *ConfigManager) <-chan *config.ProxyServerOnline {
		return cfgmgr.GetProxyConfigWatch()
	}, func(ctx context.Context, cfgmgr *ConfigManager, tc *config.ProxyServerOnline) error {
		return cfgmgr.SetProxyConfig(ctx, tc)
	})
}

func TestLogConfig(t *testing.T) {
	cases := []*config.LogOnline{
		{
			Level: "info",
			LogFile: config.LogFile{
				Filename:   "proxy.log",
				MaxSize:    100,
				MaxDays:    10,
				MaxBackups: 10,
			},
		},
		{
			Level: "debug",
			LogFile: config.LogFile{
				Filename:   "l.log",
				MaxSize:    1,
				MaxDays:    1,
				MaxBackups: 1,
			},
		},
		{
			Level:   "",
			LogFile: config.LogFile{},
		},
	}

	testWatch(t, cases, func(cfgmgr *ConfigManager) <-chan *config.LogOnline {
		return cfgmgr.GetLogConfigWatch()
	}, func(ctx context.Context, cfgmgr *ConfigManager, tc *config.LogOnline) error {
		return cfgmgr.SetLogConfig(ctx, tc)
	})
}

func testWatch[T OnlineCfgTypes](t *testing.T, cases []*T, getWatch func(*ConfigManager) <-chan *T,
	setConfig func(context.Context, *ConfigManager, *T) error) {
	cfgmgr, ctx := testConfigManager(t, &config.Config{})
	ch := getWatch(cfgmgr)
	require.Equal(t, <-ch, new(T))

	for _, tc := range cases {
		require.NoError(t, setConfig(ctx, cfgmgr, tc))
		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("\n\ntimeout waiting chan\n\n")
		case tg := <-ch:
			require.Equal(t, tc, tg)
		}
	}
}
