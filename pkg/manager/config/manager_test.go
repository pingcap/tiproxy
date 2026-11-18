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
<<<<<<< HEAD
	logger, text := logger.CreateLoggerForTest(t)
=======
	lg, text := logger.CreateLoggerForTest(t)
>>>>>>> a2944978 (config, api: Add audit logs for API (#1422))

	ctx, cancel := context.WithCancel(context.Background())
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}

	cfgmgr := NewConfigManager()
	cfgmgr.checkFileInterval = 20 * time.Millisecond
<<<<<<< HEAD
	require.NoError(t, cfgmgr.Init(ctx, logger, configFile, advertiseAddr))
=======
	require.NoError(t, cfgmgr.Init(ctx, configFile, advertiseAddr))
	cfgmgr.SetLogger(lg)
>>>>>>> a2944978 (config, api: Add audit logs for API (#1422))

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
		require.NoError(t, cfgmgr.Init(t.Context(), configFile, ""))
		t.Cleanup(func() {
			require.NoError(t, cfgmgr.Close())
		})

		require.NotContains(t, text.String(), "security.unknown-option")
		cfgmgr.SetLogger(lg)
		require.Equal(t, uint64(456), cfgmgr.GetConfig().Proxy.MaxConnections)
		require.Contains(t, text.String(), "security.unknown-option")
		require.NotContains(t, text.String(), "another-secret-value")
	})

	t.Run("keep only latest pending warning", func(t *testing.T) {
		lg, text := logger.CreateLoggerForTest(t)
		cfgmgr := NewConfigManager()
		require.NoError(t, cfgmgr.Init(t.Context(), "", ""))
		t.Cleanup(func() {
			require.NoError(t, cfgmgr.Close())
		})

		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.first-unknown = "first-secret-value"`)))
		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.latest-unknown = "latest-secret-value"`)))
		cfgmgr.SetLogger(lg)

		require.NotContains(t, text.String(), "proxy.first-unknown")
		require.Contains(t, text.String(), "proxy.latest-unknown")
		require.NotContains(t, text.String(), "first-secret-value")
		require.NotContains(t, text.String(), "latest-secret-value")
	})

	t.Run("clear pending warning after clean update", func(t *testing.T) {
		lg, text := logger.CreateLoggerForTest(t)
		cfgmgr := NewConfigManager()
		require.NoError(t, cfgmgr.Init(t.Context(), "", ""))
		t.Cleanup(func() {
			require.NoError(t, cfgmgr.Close())
		})

		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.old-unknown = "old-secret-value"`)))
		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(`proxy.max-connections = 123`)))
		cfgmgr.SetLogger(lg)

		require.NotContains(t, text.String(), "configuration contains unknown keys")
		require.NotContains(t, text.String(), "proxy.old-unknown")
		require.NotContains(t, text.String(), "old-secret-value")
	})
}
