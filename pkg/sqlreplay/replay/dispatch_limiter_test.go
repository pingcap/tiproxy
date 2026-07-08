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
	require.InDelta(t, 940.0, calcDispatchQPS(1200, 0), 1e-9)
	require.InDelta(t, 941.0, calcDispatchQPS(1200, 1000), 1e-9)
	require.InDelta(t, 990.0, calcDispatchQPS(1200, 50000), 1e-9)
	require.InDelta(t, 1200.0, calcDispatchQPS(1200, 260000), 1e-9)
	require.InDelta(t, 2140.0, calcDispatchQPS(1200, 1200000), 1e-9)
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
	limiter.reset(defaultQPSLimit)

	start := time.Now()
	require.NoError(t, limiter.wait(context.Background(), 0))
	first := time.Since(start)
	require.Less(t, first, 5*time.Millisecond)

	lowPendingWait := limiter.waitDuration(0)
	highPendingWait := limiter.waitDuration(260000)
	require.Greater(t, lowPendingWait, highPendingWait)

	start = time.Now()
	require.NoError(t, limiter.wait(context.Background(), 0))
	elapsed := time.Since(start)
	require.Greater(t, elapsed, 200*time.Microsecond)
	require.Less(t, elapsed, 5*time.Millisecond)
}
