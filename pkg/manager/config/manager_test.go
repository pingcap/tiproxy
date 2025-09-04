// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func testConfigManager(t *testing.T, configFile string, advertiseAddr string) (*ConfigManager, fmt.Stringer, context.Context) {
	_, text := logger.CreateLoggerForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}

	cfgmgr := NewConfigManager()
	cfgmgr.checkFileInterval = 20 * time.Millisecond
	require.NoError(t, cfgmgr.Init(ctx, configFile, advertiseAddr))

	t.Cleanup(func() {
		require.NoError(t, cfgmgr.Close())
	})

	t.Cleanup(cancel)

	return cfgmgr, text, ctx
}
