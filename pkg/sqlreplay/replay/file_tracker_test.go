// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestFileExecTracker(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	tracker := newFileExecTracker(zap.New(core))
	const file1 = "/data/tidb-audit-1.log"
	const file2 = "/data/tidb-audit-2.log"

	tracker.onDecoded(file1)
	tracker.onDecoded(file1)
	tracker.onExecuted(file1)
	require.Empty(t, observed.FilterMessage("finished executing replay file").All())

	tracker.markDecodeDone(file1)
	require.Empty(t, observed.FilterMessage("finished executing replay file").All())

	tracker.onExecuted(file1)
	entries := observed.FilterMessage("finished executing replay file").All()
	require.Len(t, entries, 1)
	require.Equal(t, file1, entries[0].ContextMap()["file"])

	tracker.onDecoded(file2)
	tracker.onExecuted(file2)
	require.Len(t, observed.FilterMessage("finished executing replay file").All(), 1)

	tracker.markDecodeDone(file2)
	entries = observed.FilterMessage("finished executing replay file").All()
	require.Len(t, entries, 2)
	require.Equal(t, file2, entries[1].ContextMap()["file"])
}

func TestFileExecTrackerDecodeDoneBeforeExecute(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	tracker := newFileExecTracker(zap.New(core))
	const file = "/data/tidb-audit-1.log"

	tracker.onDecoded(file)
	tracker.onExecuted(file)
	tracker.markDecodeDone(file)
	require.Len(t, observed.FilterMessage("finished executing replay file").All(), 1)
}

// Simulates the bufferedDecoder race: markDecodeDone fires from the fillBuffer
// goroutine while some decoded commands are still buffered (onDecoded called,
// but not yet onExecuted). Must not log prematurely.
func TestFileExecTrackerMarkDecodeDoneWhileBuffered(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	tracker := newFileExecTracker(zap.New(core))
	const file = "/data/tidb-audit-1.log"

	tracker.onDecoded(file)
	tracker.onDecoded(file)
	// singleDecoder sees FileName switch -> markDecodeDone fires now,
	// but the two decoded commands are still in the channel buffer.
	tracker.markDecodeDone(file)
	require.Empty(t, observed.FilterMessage("finished executing replay file").All())

	tracker.onExecuted(file)
	require.Empty(t, observed.FilterMessage("finished executing replay file").All())

	tracker.onExecuted(file)
	require.Len(t, observed.FilterMessage("finished executing replay file").All(), 1)
}
