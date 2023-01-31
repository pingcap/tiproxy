// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestConfigReload(t *testing.T) {
	tmpdir := t.TempDir()
	tmpcfg := filepath.Join(tmpdir, "cfg")

	f, err := os.Create(tmpcfg)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfgmgr1, _ := testConfigManager(t, tmpcfg)

	cfgmgr2, _ := testConfigManager(t, "")

	cases := []struct {
		name      string
		precfg    string
		precheck  func(*config.Config) bool
		postcfg   string
		postcheck func(*config.Config) bool
	}{
		{
			name:   "pd override",
			precfg: `proxy.pd-addrs = "127.0.0.1:2379"`,
			precheck: func(c *config.Config) bool {
				return c.Proxy.PDAddrs == "127.0.0.1:2379"
			},
			postcfg: `proxy.pd-addrs = ""`,
			postcheck: func(c *config.Config) bool {
				return c.Proxy.PDAddrs == ""
			},
		},
		{
			name:   "proxy-protocol override",
			precfg: `proxy.proxy-protocol = "v2"`,
			precheck: func(c *config.Config) bool {
				return c.Proxy.ProxyProtocol == "v2"
			},
			postcfg: `proxy.proxy-protocol = ""`,
			postcheck: func(c *config.Config) bool {
				return c.Proxy.ProxyProtocol == ""
			},
		},
		{
			name:   "logfile name override",
			precfg: `log.log-file.filename = "gdfg"`,
			precheck: func(c *config.Config) bool {
				return c.Log.LogFile.Filename == "gdfg"
			},
			postcfg: `log.log-file.filename = ""`,
			postcheck: func(c *config.Config) bool {
				return c.Log.LogFile.Filename == ""
			},
		},
		{
			name:   "override empty fields",
			precfg: `metrics.metrics-addr = ""`,
			precheck: func(c *config.Config) bool {
				return c.Metrics.MetricsAddr == ""
			},
			postcfg: `metrics.metrics-addr = "gg"`,
			postcheck: func(c *config.Config) bool {
				return c.Metrics.MetricsAddr == "gg"
			},
		},
		{
			name:   "override non-empty fields",
			precfg: `metrics.metrics-addr = "ee"`,
			precheck: func(c *config.Config) bool {
				return c.Metrics.MetricsAddr == "ee"
			},
			postcfg: `metrics.metrics-addr = "gg"`,
			postcheck: func(c *config.Config) bool {
				return c.Metrics.MetricsAddr == "gg"
			},
		},
		{
			name:   "non empty fields should not be override by empty fields",
			precfg: `proxy.addr = "gg"`,
			precheck: func(c *config.Config) bool {
				return c.Proxy.Addr == "gg"
			},
			postcfg: ``,
			postcheck: func(c *config.Config) bool {
				return c.Proxy.Addr == "gg"
			},
		},
	}

	for i, tc := range cases {
		msg := fmt.Sprintf("%s[%d]", tc.name, i)

		// normal path and HTTP API
		require.NoError(t, cfgmgr2.SetTOMLConfig([]byte(tc.precfg)), msg)
		if tc.precheck != nil {
			require.True(t, tc.precheck(cfgmgr2.GetConfig()), msg)
		}
		require.NoError(t, cfgmgr2.SetTOMLConfig([]byte(tc.postcfg)), msg)
		if tc.postcheck != nil {
			require.True(t, tc.postcheck(cfgmgr2.GetConfig()), msg)
		}

		// config file auto reload
		require.NoError(t, cfgmgr1.SetTOMLConfig([]byte(tc.precfg)), msg)
		if tc.precheck != nil {
			require.True(t, tc.precheck(cfgmgr1.GetConfig()), msg)
		}

		require.NoError(t, os.WriteFile(tmpcfg, []byte(tc.postcfg), 0644), msg)
		if tc.postcheck != nil {
			require.Eventually(t, func() bool { return tc.postcheck(cfgmgr1.GetConfig()) }, time.Second, 100*time.Millisecond, msg)
		}
	}
}

func TestConfigRemove(t *testing.T) {
	tmpdir := t.TempDir()
	tmpcfg := filepath.Join(tmpdir, "cfg")

	f, err := os.Create(tmpcfg)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfgmgr, _ := testConfigManager(t, tmpcfg)

	// remove and recreate the file in a very short time
	require.NoError(t, os.Remove(tmpcfg))
	require.NoError(t, os.WriteFile(tmpcfg, []byte(`proxy.addr = "gg"`), 0644))

	// check that reload still works
	require.Eventually(t, func() bool { return cfgmgr.GetConfig().Proxy.Addr == "gg" }, time.Second, 100*time.Millisecond)

	// remove again but with a long sleep
	require.NoError(t, os.Remove(tmpcfg))
	time.Sleep(200 * time.Millisecond)

	// but eventually re-watched the file again
	require.NoError(t, os.WriteFile(tmpcfg, []byte(`proxy.addr = "vv"`), 0644))
	require.Eventually(t, func() bool { return cfgmgr.GetConfig().Proxy.Addr == "vv" }, time.Second, 100*time.Millisecond)
}

func TestChecksum(t *testing.T) {
	cfgmgr, _ := testConfigManager(t, "")
	c1 := cfgmgr.GetConfigChecksum()
	require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.addr = "gg"`)))
	c2 := cfgmgr.GetConfigChecksum()
	require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.addr = "vv"`)))
	c3 := cfgmgr.GetConfigChecksum()
	require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.addr="gg"`)))
	c4 := cfgmgr.GetConfigChecksum()
	require.Equal(t, c2, c4)
	require.NotEqual(t, c1, c2)
	require.NotEqual(t, c1, c3)
	require.NotEqual(t, c2, c3)
}
