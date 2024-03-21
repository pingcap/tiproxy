// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package waitgroup

import (
	"sync"

	"go.uber.org/zap"
)

// WaitGroup is a wrapper for sync.WaitGroup
type WaitGroup struct {
	sync.WaitGroup
}

// Run runs a function in a goroutine, adds 1 to WaitGroup
// and calls done when function returns. Please DO NOT use panic
// in the cb function.
func (w *WaitGroup) Run(exec func()) {
	w.Add(1)
	go func() {
		defer w.Done()
		exec()
	}()
}

// RunWithRecover wraps goroutine startup call with force recovery, add 1 to WaitGroup
// and call done when function return. it will dump current goroutine stack into log if catch any recover result.
// exec is that execute logic function. recoverFn is that handler will be called after recover and before dump stack,
// passing `nil` means noop.
func (w *WaitGroup) RunWithRecover(exec func(), recoverFn func(r interface{}), logger *zap.Logger) {
	w.Add(1)
	go func() {
		defer func() {
			r := recover()
			defer func() {
				// If it panics again in recovery, quit ASAP.
				_ = recover()
			}()
			if r != nil && logger != nil {
				logger.Error("panic in the recoverable goroutine",
					zap.Reflect("r", r),
					zap.Stack("stack trace"))
			}
			// Call Done() before recoverFn because recoverFn normally calls `Close()`, which may call `wg.Wait()`.
			w.Done()
			if r != nil && recoverFn != nil {
				recoverFn(r)
			}
		}()
		exec()
	}()
}
