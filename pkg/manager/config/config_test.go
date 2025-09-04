// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestConfigReload(t *testing.T) {
	tmpdir := t.TempDir()
	tmpcfg := filepath.Join(tmpdir, "cfg")

	f, err := os.Create(tmpcfg)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfgmgr1, _, _ := testConfigManager(t, tmpcfg, "addr")

	cfgmgr2, _, _ := testConfigManager(t, "", "addr")

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
				return c.Proxy.PDAddrs == "" && c.Proxy.AdvertiseAddr == "addr"
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
			precfg: `api.addr = ""`,
			precheck: func(c *config.Config) bool {
				return c.API.Addr == ""
			},
			postcfg: `api.addr = "0.0.0.0:3080"`,
			postcheck: func(c *config.Config) bool {
				return c.API.Addr == "0.0.0.0:3080"
			},
		},
		{
			name:   "override non-empty fields",
			precfg: `api.addr = "0.0.0.0:3080"`,
			precheck: func(c *config.Config) bool {
				return c.API.Addr == "0.0.0.0:3080"
			},
			postcfg: `api.addr = "0.0.0.0:3081"`,
			postcheck: func(c *config.Config) bool {
				return c.API.Addr == "0.0.0.0:3081"
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
			require.Eventually(t, func() bool { return tc.postcheck(cfgmgr1.GetConfig()) }, 3*time.Second, 100*time.Millisecond, msg)
		}
	}
}

func TestConfigRemove(t *testing.T) {
	tmpdir := t.TempDir()
	tmpcfg := filepath.Join(tmpdir, "cfg")

	f, err := os.Create(tmpcfg)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfgmgr, _, _ := testConfigManager(t, tmpcfg, "")

	// remove and recreate the file in a very short time
	require.NoError(t, os.Remove(tmpcfg))
	require.NoError(t, os.WriteFile(tmpcfg, []byte(`proxy.addr = "gg"`), 0644))

	// check that re-watch still works
	require.Eventually(t, func() bool { return cfgmgr.GetConfig() != nil && cfgmgr.GetConfig().Proxy.Addr == "gg" }, 3*time.Second, 100*time.Millisecond)

	// remove again but with a long sleep
	require.NoError(t, os.Remove(tmpcfg))
	time.Sleep(200 * time.Millisecond)

	// but eventually reload the file again
	require.NoError(t, os.WriteFile(tmpcfg, []byte(`proxy.addr = "vv"`), 0644))
	require.Eventually(t, func() bool { return cfgmgr.GetConfig().Proxy.Addr == "vv" }, 3*time.Second, 100*time.Millisecond)
}

func TestFilePath(t *testing.T) {
	for i := 0; i < 10; i++ {
		var cfgmgr *ConfigManager
		tmpdir := t.TempDir()
		pdAddr1, pdAddr2, pdAddr3 := "127.0.0.1:1000", "127.0.0.1:2000", "127.0.0.1:3000"

		tests := []struct {
			filename   string
			createFile func()
			cleanFile  func()
			checker    func(filename string)
		}{
			{
				// Test updating another file in the same directory.
				filename: filepath.Join(tmpdir, "cfg"),
				checker: func(filename string) {
					tmplog := filepath.Join(tmpdir, "log")
					f, err := os.Create(tmplog)
					require.NoError(t, err)
					require.NoError(t, f.Close())
					require.NoError(t, os.WriteFile(tmplog, []byte("hello"), 0644))
					newlog := filepath.Join(tmpdir, "log1")
					require.NoError(t, os.Rename(tmplog, newlog))
					require.NoError(t, os.Remove(newlog))
					require.Equal(t, pdAddr2, cfgmgr.GetConfig().Proxy.PDAddrs)
				},
			},
			{
				// Test case-insensitive.
				filename: filepath.Join(tmpdir, "cfg"),
				createFile: func() {
					f, err := os.Create(filepath.Join(tmpdir, "CFG"))
					require.NoError(t, err)
					require.NoError(t, f.Close())
					// Linux is case-sensitive but macOS is case-insensitive.
					// For linux, it creates another file. For macOS, it doesn't touch the file.
					f, err = os.Create(filepath.Join(tmpdir, "cfg"))
					require.NoError(t, err)
					_, err = f.WriteString(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr1))
					require.NoError(t, err)
					require.NoError(t, f.Close())
				},
			},
			{
				// Test relative path.
				// `event.Name` is `cfg` on MacOS, but it's `./cfg` on Linux.
				filename: "cfg",
			},
			{
				// Test relative path.
				filename: "./cfg",
			},
			{
				// Test uncleaned path.
				filename: fmt.Sprintf("%s%c%ccfg", tmpdir, filepath.Separator, filepath.Separator),
			},
			{
				// Test removing and recreating the directory.
				filename: "_tmp/cfg",
				createFile: func() {
					if err := os.Mkdir("_tmp", 0755); err != nil {
						require.ErrorIs(t, err, os.ErrExist)
					}
					f, err := os.Create("_tmp/cfg")
					require.NoError(t, err)
					_, err = f.WriteString(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr1))
					require.NoError(t, err)
					require.NoError(t, f.Close())
				},
				cleanFile: func() {
					require.NoError(t, os.RemoveAll("_tmp"))
				},
				checker: func(filename string) {
					require.NoError(t, os.RemoveAll("_tmp"))
					t.Log("remove _tmp")

					require.NoError(t, os.Mkdir("_tmp", 0755))
					f, err := os.Create("_tmp/cfg")
					require.NoError(t, err)
					_, err = f.WriteString(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr3))
					require.NoError(t, err)
					require.NoError(t, f.Close())
					t.Log("write _tmp")
					require.Eventually(t, func() bool {
						return pdAddr3 == cfgmgr.GetConfig().Proxy.PDAddrs
					}, 3*time.Second, 10*time.Millisecond, cfgmgr.GetConfig().Proxy.PDAddrs)
				},
			},
			{
				// Test removing and recreating the file.
				filename: "cfg",
				checker: func(filename string) {
					require.NoError(t, os.Remove(filename))

					f, err := os.Create(filename)
					require.NoError(t, err)
					_, err = f.WriteString(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr3))
					require.NoError(t, err)
					require.NoError(t, f.Close())
					require.Eventually(t, func() bool {
						return pdAddr3 == cfgmgr.GetConfig().Proxy.PDAddrs
					}, 3*time.Second, 10*time.Millisecond, cfgmgr.GetConfig().Proxy.PDAddrs)
				},
			},
		}

		for i, test := range tests {
			t.Logf("%dth test", i+1)
			if test.createFile != nil {
				test.createFile()
			} else {
				f, err := os.Create(test.filename)
				require.NoError(t, err)
				_, err = f.WriteString(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr1))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}

			cfgmgr, _, _ = testConfigManager(t, test.filename, "")
			require.Equal(t, pdAddr1, cfgmgr.GetConfig().Proxy.PDAddrs)

			// Test write.
			require.NoError(t, os.WriteFile(test.filename, []byte(fmt.Sprintf("proxy.pd-addrs = \"%s\"", pdAddr2)), 0644))
			require.Eventually(t, func() bool {
				return pdAddr2 == cfgmgr.GetConfig().Proxy.PDAddrs
			}, 3*time.Second, 10*time.Millisecond, cfgmgr.GetConfig().Proxy.PDAddrs)

			// Test other.
			if test.checker != nil {
				test.checker(test.filename)
			}

			// Test remove.
			if test.cleanFile != nil {
				test.cleanFile()
			} else {
				// It doesn't matter whether it triggers reload or not.
				require.NoError(t, os.Remove(test.filename))
			}
			require.NoError(t, cfgmgr.Close())
		}
	}
}

func TestChecksum(t *testing.T) {
	cfgmgr, _, _ := testConfigManager(t, "", "")

	c1 := cfgmgr.GetConfigChecksum()
	require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.addr = "gg"`)))
	// same config, shouldn't log it again
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
