// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func testConfigManager(t *testing.T, configFile string, advertiseAddr string) (*ConfigManager, fmt.Stringer, context.Context) {
	lg, text := logger.CreateLoggerForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}

	cfgmgr := NewConfigManager()
	cfgmgr.checkFileInterval = 20 * time.Millisecond
	require.NoError(t, cfgmgr.Init(ctx, lg, configFile, advertiseAddr))

	t.Cleanup(func() {
		require.NoError(t, cfgmgr.Close())
	})

	t.Cleanup(cancel)

	return cfgmgr, text, ctx
}

func TestUnknownConfigKeysWarning(t *testing.T) {
	t.Run("runtime update", func(t *testing.T) {
		cfgmgr, text, _ := testConfigManager(t, "", "")

		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`
proxy.max-connections = 123
proxy.unknown-option = "secret-value-must-not-be-logged"
`)))

		require.Equal(t, uint64(123), cfgmgr.GetConfig().Proxy.MaxConnections)
		require.Contains(t, text.String(), "configuration contains unknown keys")
		require.Contains(t, text.String(), "proxy.unknown-option")
		require.NotContains(t, text.String(), "secret-value-must-not-be-logged")
	})

	t.Run("initial configuration", func(t *testing.T) {
		configFile := filepath.Join(t.TempDir(), "tiproxy.toml")
		require.NoError(t, os.WriteFile(configFile, []byte(`
proxy.max-connections = 456
security.unknown-option = "another-secret-value"
`), 0o644))

		lg, text := logger.CreateLoggerForTest(t)
		cfgmgr := NewConfigManager()
		require.NoError(t, cfgmgr.Init(t.Context(), lg, configFile, ""))
		t.Cleanup(func() {
			require.NoError(t, cfgmgr.Close())
		})

		require.Equal(t, uint64(456), cfgmgr.GetConfig().Proxy.MaxConnections)
		require.Contains(t, text.String(), "security.unknown-option")
		require.NotContains(t, text.String(), "another-secret-value")
	})
}
