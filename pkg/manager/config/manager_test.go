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

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func testConfigManager(t *testing.T, configFile string, overlays ...*config.Config) (*ConfigManager, context.Context) {
	logger := logger.CreateLoggerForTest(t)

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

	return cfgmgr, ctx
}
