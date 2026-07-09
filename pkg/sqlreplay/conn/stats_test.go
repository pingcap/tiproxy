// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayStatsUpdateCurCmdTs(t *testing.T) {
	var stats ReplayStats
	stats.UpdateCurCmdTs(100)
	require.Equal(t, int64(100), stats.CurCmdTs.Load())
	stats.UpdateCurCmdTs(50)
	require.Equal(t, int64(100), stats.CurCmdTs.Load())
	stats.UpdateCurCmdTs(200)
	require.Equal(t, int64(200), stats.CurCmdTs.Load())
}

func TestReplayStatsUpdateCurCmdEndTs(t *testing.T) {
	var stats ReplayStats
	stats.UpdateCurCmdEndTs(100)
	require.Equal(t, int64(100), stats.CurCmdEndTs.Load())
	stats.UpdateCurCmdEndTs(50)
	require.Equal(t, int64(100), stats.CurCmdEndTs.Load())
	stats.UpdateCurCmdEndTs(200)
	require.Equal(t, int64(200), stats.CurCmdEndTs.Load())
}
