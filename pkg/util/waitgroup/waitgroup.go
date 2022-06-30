package waitgroup

import (
	"sync"
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
func (w *WaitGroup) RunWithRecover(exec func(), recoverFn func(r interface{})) {
	w.Add(1)
	go func() {
		defer func() {
			r := recover()
			if r != nil && recoverFn != nil {
				recoverFn(r)
			}
			w.Done()
		}()
		exec()
	}()
}
