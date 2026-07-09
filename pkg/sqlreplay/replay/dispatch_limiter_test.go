// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestIsDispatchLimitedCmd(t *testing.T) {
	require.True(t, isDispatchLimitedCmd(&cmd.Command{Type: pnet.ComQuery}))
	require.True(t, isDispatchLimitedCmd(&cmd.Command{Type: pnet.ComStmtExecute}))
	require.False(t, isDispatchLimitedCmd(&cmd.Command{Type: pnet.ComStmtPrepare}))
	require.False(t, isDispatchLimitedCmd(&cmd.Command{Type: pnet.ComStmtClose}))
	require.False(t, isDispatchLimitedCmd(nil))
}

func TestCalcDispatchQPS(t *testing.T) {
	require.InDelta(t, 1200.0, calcDispatchQPS(1200), 1e-9)
	require.InDelta(t, 500.0, calcDispatchQPS(500), 1e-9)
	require.InDelta(t, 1.0, calcDispatchQPS(0), 1e-9)
}

func TestDispatchLimiterWait(t *testing.T) {
	limiter := &dispatchLimiter{}
	limiter.reset(defaultQPSLimit)

	start := time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	first := time.Since(start)
	require.Less(t, first, 5*time.Millisecond)

	start = time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	elapsed := time.Since(start)
	require.Greater(t, elapsed, 200*time.Microsecond)
	require.Less(t, elapsed, 5*time.Millisecond)

	limiter.reset(2400)
	start = time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	require.NoError(t, limiter.wait(context.Background()))
	higherLimitElapsed := time.Since(start)
	require.Less(t, higherLimitElapsed, elapsed)
}
