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

const (
	defaultQPSLimit   = 1200
	minDispatchQPS    = 100
	dispatchQPSFactor = 1.3
	dispatchQPSWindow = 30 * time.Minute
)

func calcDispatchQPS(qpsLimit float64) float64 {
	if qpsLimit < 1 {
		return 1
	}
	return qpsLimit
}

func calcEffectiveDispatchQPS(avgDecodeQPS, maxQPS float64, windowFull bool) float64 {
	if !windowFull {
		return calcDispatchQPS(maxQPS)
	}
	effective := avgDecodeQPS * dispatchQPSFactor
	if effective < minDispatchQPS {
		effective = minDispatchQPS
	}
	if effective > maxQPS {
		effective = maxQPS
	}
	return calcDispatchQPS(effective)
}

func isDispatchLimitedCmd(command *cmd.Command) bool {
	if command == nil {
		return false
	}
	return command.Type == pnet.ComQuery || command.Type == pnet.ComStmtExecute
}

// decodeQPSWindow keeps a rolling window of per-bucket decoded query counts.
type decodeQPSWindow struct {
	mu         sync.Mutex
	bucketSecs float64
	buckets    []uint64
	idx        int
	filled     int
	total      uint64
}

func newDecodeQPSWindow(bucketInterval time.Duration) *decodeQPSWindow {
	return newDecodeQPSWindowWithDuration(bucketInterval, dispatchQPSWindow)
}

func newDecodeQPSWindowWithDuration(bucketInterval, windowDuration time.Duration) *decodeQPSWindow {
	n := int(windowDuration / bucketInterval)
	if n < 1 {
		n = 1
	}
	return &decodeQPSWindow{
		bucketSecs: bucketInterval.Seconds(),
		buckets:    make([]uint64, n),
	}
}

func (w *decodeQPSWindow) add(count uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total -= w.buckets[w.idx]
	w.buckets[w.idx] = count
	w.total += count
	w.idx = (w.idx + 1) % len(w.buckets)
	if w.filled < len(w.buckets) {
		w.filled++
	}
}

func (w *decodeQPSWindow) windowFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.filled >= len(w.buckets)
}

func (w *decodeQPSWindow) avgQPS() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.filled < len(w.buckets) {
		return 0
	}
	return float64(w.total) / (float64(len(w.buckets)) * w.bucketSecs)
}

type dispatchLimiter struct {
	mu           sync.Mutex
	lastDispatch time.Time
	maxQPS       float64
	effectiveQPS float64
	decodeWindow *decodeQPSWindow
}

func (l *dispatchLimiter) reset(maxQPS float64, bucketInterval time.Duration) {
	l.mu.Lock()
	l.lastDispatch = time.Time{}
	l.maxQPS = maxQPS
	l.effectiveQPS = calcDispatchQPS(maxQPS)
	l.decodeWindow = newDecodeQPSWindow(bucketInterval)
	l.mu.Unlock()
}

func (l *dispatchLimiter) updateDecodeCount(count uint64) {
	l.decodeWindow.add(count)
	effective := calcEffectiveDispatchQPS(l.decodeWindow.avgQPS(), l.maxQPS, l.decodeWindow.windowFull())
	l.mu.Lock()
	l.effectiveQPS = effective
	l.mu.Unlock()
}

func (l *dispatchLimiter) currentQPS() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.effectiveQPS
}

func (l *dispatchLimiter) waitDuration() time.Duration {
	l.mu.Lock()
	qps := l.effectiveQPS
	lastDispatch := l.lastDispatch
	l.mu.Unlock()

	interval := time.Duration(float64(time.Second) / qps)
	if lastDispatch.IsZero() {
		return 0
	}
	wait := time.Until(lastDispatch.Add(interval))
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

func (l *dispatchLimiter) wait(ctx context.Context) error {
	wait := l.waitDuration()
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
