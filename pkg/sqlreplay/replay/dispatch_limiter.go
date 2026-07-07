// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"sync"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

func calcAutoDispatchQPS(pending int64) float64 {
	if pending < 0 {
		pending = 0
	}
	extra := pending / 1000
	if extra > 260 {
		extra = 260
	}
	return float64(500 + extra)
}

func isDispatchLimitedCmd(command *cmd.Command) bool {
	if command == nil {
		return false
	}
	return command.Type == pnet.ComQuery || command.Type == pnet.ComStmtExecute
}

func shortenDispatchWait(wait time.Duration) time.Duration {
	if wait <= 0 {
		return 0
	}
	qps := float64(time.Second) / float64(wait)
	if qps >= 200 {
		return wait
	}
	reduction := (200 - qps) / 100
	if reduction >= 1 {
		return 0
	}
	return time.Duration(float64(wait) * (1 - reduction))
}

// applyShortenDispatchWait accelerates replay only when the QPS between dispatch-limited
// commands in the capture is low. timelineWait follows the full capture timeline, while
// dispatchCaptureGap is the interval since the previous dispatch-limited command.
func applyShortenDispatchWait(timelineWait, dispatchCaptureGap time.Duration, lastDispatchReplayTs time.Time) time.Duration {
	if dispatchCaptureGap <= 0 || lastDispatchReplayTs.IsZero() {
		return timelineWait
	}
	shortenedGap := shortenDispatchWait(dispatchCaptureGap)
	if shortenedGap >= dispatchCaptureGap {
		return timelineWait
	}
	shortenedWait := max(time.Until(lastDispatchReplayTs.Add(shortenedGap)), 0)
	return min(timelineWait, shortenedWait)
}

type dispatchLimiter struct {
	mu           sync.Mutex
	lastDispatch time.Time
}

func (l *dispatchLimiter) reset() {
	l.mu.Lock()
	l.lastDispatch = time.Time{}
	l.mu.Unlock()
}

func (l *dispatchLimiter) waitDuration(pending int64) time.Duration {
	qps := calcAutoDispatchQPS(pending)
	interval := time.Duration(float64(time.Second) / qps)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastDispatch.IsZero() {
		return 0
	}
	wait := time.Until(l.lastDispatch.Add(interval))
	if wait < 0 {
		return 0
	}
	return wait
}

func (l *dispatchLimiter) markDispatched() {
	l.mu.Lock()
	l.lastDispatch = time.Now()
	l.mu.Unlock()
}

func (l *dispatchLimiter) wait(ctx context.Context, pending int64) error {
	wait := l.waitDuration(pending)
	if wait <= 0 {
		l.markDispatched()
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		l.markDispatched()
		return nil
	}
}
