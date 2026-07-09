// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"sync"

	"go.uber.org/zap"
)

// fileTracker logs when all commands from a file are parsed or executed.
type fileTracker struct {
	lg *zap.Logger
	mu sync.Mutex

	pending  map[string]int64
	parsed   map[string]bool
	executed map[string]bool
}

func newFileTracker(lg *zap.Logger) *fileTracker {
	return &fileTracker{
		lg:       lg,
		pending:  make(map[string]int64),
		parsed:   make(map[string]bool),
		executed: make(map[string]bool),
	}
}

func (t *fileTracker) onFileParsed(fileName string) {
	if t == nil || fileName == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.parsed[fileName] {
		return
	}
	t.parsed[fileName] = true
	t.lg.Info("finished parsing file", zap.String("file", fileName))
	t.tryLogExecutedLocked(fileName)
}

func (t *fileTracker) onDispatched(fileName string) {
	if t == nil || fileName == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending[fileName]++
}

func (t *fileTracker) onExecuted(fileName string) {
	if t == nil || fileName == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending[fileName] > 0 {
		t.pending[fileName]--
	}
	t.tryLogExecutedLocked(fileName)
}

func (t *fileTracker) tryLogExecutedLocked(fileName string) {
	if !t.parsed[fileName] || t.pending[fileName] > 0 || t.executed[fileName] {
		return
	}
	t.executed[fileName] = true
	delete(t.pending, fileName)
	t.lg.Info("finished executing file", zap.String("file", fileName))
}
