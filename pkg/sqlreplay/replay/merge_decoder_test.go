// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"io"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

type mockDecoder struct {
	commands []*cmd.Command
	index    int
}

func newMockDecoder(commands []*cmd.Command) *mockDecoder {
	return &mockDecoder{
		commands: commands,
		index:    0,
	}
}

func (d *mockDecoder) Decode() (*cmd.Command, error) {
	if d.index >= len(d.commands) {
		return nil, io.EOF
	}
	cmd := d.commands[d.index]
	d.index++
	return cmd, nil
}

func TestMergeDecoderSingleDecoder(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(20 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(30 * time.Millisecond)},
	}

	decoder := newMockDecoder(commands)
	merger := newMergeDecoder(decoder)

	for i, expected := range commands {
		cmd, err := merger.Decode()
		require.NoError(t, err, "decode %d", i)
		require.Equal(t, expected.ConnID, cmd.ConnID, "decode %d", i)
		require.Equal(t, expected.StartTs, cmd.StartTs, "decode %d", i)
	}

	// Should return EOF when exhausted
	_, err := merger.Decode()
	require.Equal(t, io.EOF, err)
}

func TestMergeDecoderMultipleDecoders(t *testing.T) {
	now := time.Now()

	commands1 := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(30 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(50 * time.Millisecond)},
	}

	commands2 := []*cmd.Command{
		{ConnID: 2, StartTs: now.Add(20 * time.Millisecond)},
		{ConnID: 2, StartTs: now.Add(40 * time.Millisecond)},
		{ConnID: 2, StartTs: now.Add(60 * time.Millisecond)},
	}

	decoder1 := newMockDecoder(commands1)
	decoder2 := newMockDecoder(commands2)
	merger := newMergeDecoder(decoder1, decoder2)

	expectedOrder := []uint64{1, 2, 1, 2, 1, 2}
	expectedTimes := []time.Time{
		now.Add(10 * time.Millisecond),
		now.Add(20 * time.Millisecond),
		now.Add(30 * time.Millisecond),
		now.Add(40 * time.Millisecond),
		now.Add(50 * time.Millisecond),
		now.Add(60 * time.Millisecond),
	}

	for i, conn := range expectedOrder {
		cmd, err := merger.Decode()
		require.NoError(t, err, "decode %d", i)
		require.Equal(t, conn, cmd.ConnID, "decode %d", i)
		require.Equal(t, expectedTimes[i], cmd.StartTs, "decode %d", i)
	}

	// Should return EOF when all decoders are exhausted
	_, err := merger.Decode()
	require.Equal(t, io.EOF, err)
}

func TestMergeDecoderRandomizedCommands(t *testing.T) {
	now := time.Now()
	rng := rand.New(rand.NewSource(42))

	numDecoders := 5
	commandsPerDecoder := 20

	var allCommands []*cmd.Command
	var decoders []decoder

	for decoderID := 0; decoderID < numDecoders; decoderID++ {
		var commands []*cmd.Command

		for range commandsPerDecoder {
			offset := time.Duration(rng.Intn(1000)) * time.Millisecond
			command := &cmd.Command{
				ConnID:  uint64(decoderID + 1),
				StartTs: now.Add(offset),
			}
			commands = append(commands, command)
			allCommands = append(allCommands, command)
		}

		sort.Slice(commands, func(i, j int) bool {
			return commands[i].StartTs.Before(commands[j].StartTs)
		})

		decoders = append(decoders, newMockDecoder(commands))
	}

	sort.Slice(allCommands, func(i, j int) bool {
		if allCommands[i].StartTs.Equal(allCommands[j].StartTs) {
			// For equal timestamps, maintain stable sort by ConnID
			return allCommands[i].ConnID < allCommands[j].ConnID
		}
		return allCommands[i].StartTs.Before(allCommands[j].StartTs)
	})

	merger := newMergeDecoder(decoders...)

	for i, expected := range allCommands {
		cmd, err := merger.Decode()
		require.NoError(t, err, "decode %d", i)
		require.Equal(t, expected.ConnID, cmd.ConnID, "decode %d", i)
		require.Equal(t, expected.StartTs, cmd.StartTs, "decode %d", i)
	}

	_, err := merger.Decode()
	require.Equal(t, io.EOF, err)
}

func TestMergeDecoderEmptyDecoders(t *testing.T) {
	decoder1 := newMockDecoder([]*cmd.Command{})
	decoder2 := newMockDecoder([]*cmd.Command{})
	merger := newMergeDecoder(decoder1, decoder2)

	// Should return EOF immediately
	_, err := merger.Decode()
	require.Equal(t, io.EOF, err)
}
