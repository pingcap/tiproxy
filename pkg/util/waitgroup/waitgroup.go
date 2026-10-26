// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package waitgroup

import (
	"time"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

// WaitGroup wraps waitgroup.WaitGroup to avoid cycle import.
// It increases the metrics counter if panic.
type WaitGroup struct {
	waitgroup.WaitGroup
}

func (w *WaitGroup) Run(exec func(), logger *zap.Logger) {
	w.WaitGroup.RunWithLogger(exec, logger)
}

func (w *WaitGroup) RunWithRecover(exec func(), recoverFn func(r interface{}), logger *zap.Logger) {
	w.WaitGroup.RunWithRecover(exec, func(r interface{}) {
		metrics.ServerErrCounter.WithLabelValues("panic").Inc()
		if r != nil && recoverFn != nil {
			recoverFn(r)
		}
	}, logger)
}

type WaitGroupPool struct {
	waitgroup.WaitGroupPool
}

func NewWaitGroupPool(n int, idleDuration time.Duration) *WaitGroupPool {
	return &WaitGroupPool{
		WaitGroupPool: *waitgroup.NewWaitGroupPool(n, idleDuration),
	}
}

func (w *WaitGroupPool) RunWithRecover(exec func(), recoverFn func(r interface{}), logger *zap.Logger) {
	w.WaitGroupPool.RunWithRecover(exec, func(r interface{}) {
		metrics.ServerErrCounter.WithLabelValues("panic").Inc()
		if r != nil && recoverFn != nil {
			recoverFn(r)
		}
	}, logger)
}
