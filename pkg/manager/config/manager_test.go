// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func testConfigManager(t *testing.T, configFile string, overlays ...*config.Config) (*ConfigManager, fmt.Stringer, context.Context) {
	logger, text := logger.CreateLoggerForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}

	cfgmgr := NewConfigManager()
	require.NoError(t, cfgmgr.Init(ctx, logger, configFile, nil))

	t.Cleanup(func() {
		require.NoError(t, cfgmgr.Close())
	})

	t.Cleanup(cancel)

	return cfgmgr, text, ctx
}
