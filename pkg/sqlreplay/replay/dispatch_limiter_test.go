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

func TestCalcEffectiveDispatchQPS(t *testing.T) {
	require.InDelta(t, 1200.0, calcEffectiveDispatchQPS(800, defaultQPSLimit, false), 1e-9)
	require.InDelta(t, 1200.0, calcEffectiveDispatchQPS(800, defaultQPSLimit, true), 1e-9)
	require.InDelta(t, 600.0, calcEffectiveDispatchQPS(400, defaultQPSLimit, true), 1e-9)
	require.InDelta(t, 100.0, calcEffectiveDispatchQPS(50, defaultQPSLimit, true), 1e-9)
	require.InDelta(t, 100.0, calcEffectiveDispatchQPS(0, defaultQPSLimit, true), 1e-9)
}

func TestDecodeQPSWindow(t *testing.T) {
	window := newDecodeQPSWindowWithDuration(time.Second, 180*time.Second)
	require.False(t, window.windowFull())
	require.InDelta(t, 0, window.avgQPS(), 1e-9)

	for range 180 {
		window.add(10)
	}
	require.True(t, window.windowFull())
	require.InDelta(t, 10, window.avgQPS(), 1e-9)

	window.add(100)
	require.InDelta(t, 10.5, window.avgQPS(), 1e-9)
}

func TestDispatchLimiterWait(t *testing.T) {
	limiter := &dispatchLimiter{}
	limiter.reset(defaultQPSLimit, time.Second)

	start := time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	first := time.Since(start)
	require.Less(t, first, 5*time.Millisecond)

	start = time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	elapsed := time.Since(start)
	require.Greater(t, elapsed, 200*time.Microsecond)
	require.Less(t, elapsed, 5*time.Millisecond)

	limiter.reset(2400, time.Second)
	start = time.Now()
	require.NoError(t, limiter.wait(context.Background()))
	require.NoError(t, limiter.wait(context.Background()))
	higherLimitElapsed := time.Since(start)
	require.Less(t, higherLimitElapsed, elapsed)
}

func TestDispatchLimiterUpdateDecodeCount(t *testing.T) {
	limiter := &dispatchLimiter{
		maxQPS:       defaultQPSLimit,
		effectiveQPS: calcDispatchQPS(defaultQPSLimit),
		decodeWindow: newDecodeQPSWindowWithDuration(time.Second, 180*time.Second),
	}
	require.InDelta(t, 1200, limiter.currentQPS(), 1e-9)

	for range 180 {
		limiter.updateDecodeCount(400)
	}
	require.InDelta(t, 600, limiter.currentQPS(), 1e-9)

	for range 180 {
		limiter.updateDecodeCount(200)
	}
	require.InDelta(t, 300, limiter.currentQPS(), 1e-9)

	for range 180 {
		limiter.updateDecodeCount(10)
	}
	require.InDelta(t, 100, limiter.currentQPS(), 1e-9)
}
