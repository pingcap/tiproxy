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

const defaultQPSLimit = 1200

func calcDispatchQPS(qpsLimit float64) float64 {
	if qpsLimit < 1 {
		return 1
	}
	return qpsLimit
}

func isDispatchLimitedCmd(command *cmd.Command) bool {
	if command == nil {
		return false
	}
	return command.Type == pnet.ComQuery || command.Type == pnet.ComStmtExecute
}

type dispatchLimiter struct {
	mu           sync.Mutex
	lastDispatch time.Time
	qpsLimit     float64
}

func (l *dispatchLimiter) reset(qpsLimit float64) {
	l.mu.Lock()
	l.lastDispatch = time.Time{}
	l.qpsLimit = qpsLimit
	l.mu.Unlock()
}

func (l *dispatchLimiter) waitDuration() time.Duration {
	qps := calcDispatchQPS(l.qpsLimit)
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
