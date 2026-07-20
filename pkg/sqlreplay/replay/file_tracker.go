// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"sync"

	"go.uber.org/zap"
)

type fileExecTracker struct {
	lg         *zap.Logger
	mu         sync.Mutex
	pending    map[string]int // decoded but not yet executed
	decodeDone map[string]bool
}

func newFileExecTracker(lg *zap.Logger) *fileExecTracker {
	return &fileExecTracker{
		lg:         lg,
		pending:    make(map[string]int),
		decodeDone: make(map[string]bool),
	}
}

// onDecoded is called when singleDecoder produces a command from `file`.
func (t *fileExecTracker) onDecoded(file string) {
	if t == nil || file == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[file]++
}

// markDecodeDone is called when a singleDecoder has finished producing commands
// from `file` (FileName switch or EOF). It is safe to call before the buffered
// commands are executed: the pending counter protects against premature finish.
func (t *fileExecTracker) markDecodeDone(file string) {
	if t == nil || file == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.decodeDone[file] = true
	t.tryFinishLocked(file)
	t.lg.Info("finished decoding replay file", zap.String("file", file))
}

// onExecuted is called when a connection has finished processing a command
// (executed, filtered, or drained on close).
func (t *fileExecTracker) onExecuted(file string) {
	if t == nil || file == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[file]--
	t.tryFinishLocked(file)
}

func (t *fileExecTracker) tryFinishLocked(file string) {
	if !t.decodeDone[file] {
		return
	}
	if t.pending[file] > 0 {
		return
	}
	delete(t.pending, file)
	delete(t.decodeDone, file)
	t.lg.Info("finished executing replay file", zap.String("file", file))
}
