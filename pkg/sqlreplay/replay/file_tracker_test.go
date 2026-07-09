// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"io"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestFileTracker(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	tracker := newFileTracker(zap.New(core))
	file := "file:/data/tidb-audit.log"

	tracker.onDispatched(file)
	tracker.onDispatched(file)
	tracker.onExecuted(file)
	require.Equal(t, 0, observed.FilterMessage("finished executing file").Len())

	tracker.onFileParsed(file)
	require.Equal(t, 1, observed.FilterMessage("finished parsing file").Len())
	require.Equal(t, 0, observed.FilterMessage("finished executing file").Len())

	tracker.onExecuted(file)
	require.Equal(t, 1, observed.FilterMessage("finished executing file").Len())

	tracker.onFileParsed(file)
	tracker.onExecuted(file)
	require.Equal(t, 1, observed.FilterMessage("finished parsing file").Len())
	require.Equal(t, 1, observed.FilterMessage("finished executing file").Len())
}

type mockCmdDecoder struct {
	commands []*cmd.Command
	index    int
}

func (d *mockCmdDecoder) Decode(_ cmd.LineReader) (*cmd.Command, error) {
	if d.index >= len(d.commands) {
		return nil, io.EOF
	}
	command := d.commands[d.index]
	d.index++
	return command, nil
}

func (d *mockCmdDecoder) SetCommandStartTime(_ time.Time) {}

func TestSingleDecoderFileParsed(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	tracker := newFileTracker(zap.New(core))

	cmdDecoder := &mockCmdDecoder{
		commands: []*cmd.Command{
			{FileName: "file:/a.log"},
			{FileName: "file:/b.log"},
		},
	}
	decoder := newSingleDecoder(cmdDecoder, nil, tracker.onFileParsed)

	cmd1, err := decoder.Decode()
	require.NoError(t, err)
	require.Equal(t, "file:/a.log", cmd1.FileName)

	cmd2, err := decoder.Decode()
	require.NoError(t, err)
	require.Equal(t, "file:/b.log", cmd2.FileName)
	require.Equal(t, 1, observed.FilterMessage("finished parsing file").FilterField(zap.String("file", "file:/a.log")).Len())

	_, err = decoder.Decode()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, observed.FilterMessage("finished parsing file").FilterField(zap.String("file", "file:/b.log")).Len())
}
