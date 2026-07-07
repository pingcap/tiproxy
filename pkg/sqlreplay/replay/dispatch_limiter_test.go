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

func TestCalcAutoDispatchQPS(t *testing.T) {
	require.InDelta(t, 500.0, calcAutoDispatchQPS(0), 1e-9)
	require.InDelta(t, 501.0, calcAutoDispatchQPS(1000), 1e-9)
	require.InDelta(t, 550.0, calcAutoDispatchQPS(50000), 1e-9)
	require.InDelta(t, 760.0, calcAutoDispatchQPS(260000), 1e-9)
	require.InDelta(t, 760.0, calcAutoDispatchQPS(1000000), 1e-9)
}

func TestShortenDispatchWait(t *testing.T) {
	wait := time.Second / 190
	require.InDelta(t, 0.9, float64(shortenDispatchWait(wait))/float64(wait), 1e-6)

	require.Equal(t, time.Duration(0), shortenDispatchWait(10*time.Millisecond))
	require.Equal(t, 5*time.Millisecond, shortenDispatchWait(5*time.Millisecond))
	require.Equal(t, time.Duration(0), shortenDispatchWait(20*time.Millisecond))
}

func TestApplyShortenDispatchWait(t *testing.T) {
	lastReplayTs := time.Now().Add(-500 * time.Millisecond)
	timelineWait := 500 * time.Millisecond

	// High QPS between queries: do not shorten even if the full timeline wait is large.
	shortened := applyShortenDispatchWait(timelineWait, 2*time.Millisecond, lastReplayTs)
	require.Equal(t, timelineWait, shortened)

	// Low QPS between queries: accelerate from the last dispatch-limited command.
	shortened = applyShortenDispatchWait(timelineWait, 10*time.Second, lastReplayTs)
	require.Less(t, shortened, timelineWait)

	// No previous dispatch-limited command: keep the timeline wait.
	shortened = applyShortenDispatchWait(timelineWait, 10*time.Second, time.Time{})
	require.Equal(t, timelineWait, shortened)
}

func TestDispatchLimiterWait(t *testing.T) {
	limiter := &dispatchLimiter{}

	start := time.Now()
	require.NoError(t, limiter.wait(context.Background(), 0))
	first := time.Since(start)
	require.Less(t, first, 5*time.Millisecond)

	start = time.Now()
	require.NoError(t, limiter.wait(context.Background(), 0))
	elapsed := time.Since(start)
	require.Greater(t, elapsed, time.Millisecond)
	require.Less(t, elapsed, 10*time.Millisecond)

	limiter.reset()
	start = time.Now()
	require.NoError(t, limiter.wait(context.Background(), 0))
	require.NoError(t, limiter.wait(context.Background(), 260000))
	highPendingElapsed := time.Since(start)
	require.Less(t, highPendingElapsed, elapsed)
}
