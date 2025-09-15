// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

// mockSlowDecoder simulates a decoder with configurable delays
type mockSlowDecoder struct {
	commands []*cmd.Command
	index    int
	delay    time.Duration
	mu       sync.Mutex
}

func newMockSlowDecoder(commands []*cmd.Command, delay time.Duration) *mockSlowDecoder {
	return &mockSlowDecoder{
		commands: commands,
		index:    0,
		delay:    delay,
	}
}

func (d *mockSlowDecoder) Decode() (*cmd.Command, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.delay > 0 {
		time.Sleep(d.delay)
	}

	if d.index >= len(d.commands) {
		return nil, io.EOF
	}

	cmd := d.commands[d.index]
	d.index++
	return cmd, nil
}

// mockErrorDecoder returns an error after a specified number of commands
type mockErrorDecoder struct {
	commands   []*cmd.Command
	index      int
	errorAfter int
	testError  error
}

func newMockErrorDecoder(commands []*cmd.Command, errorAfter int, err error) *mockErrorDecoder {
	return &mockErrorDecoder{
		commands:   commands,
		index:      0,
		errorAfter: errorAfter,
		testError:  err,
	}
}

func (d *mockErrorDecoder) Decode() (*cmd.Command, error) {
	if d.index >= d.errorAfter {
		return nil, d.testError
	}

	if d.index >= len(d.commands) {
		return nil, io.EOF
	}

	cmd := d.commands[d.index]
	d.index++
	return cmd, nil
}

func TestBufferedDecoderBasicFunctionality(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(20 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(30 * time.Millisecond)},
	}

	mockDec := newMockDecoder(commands)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, mockDec, 5)
	defer bufDec.Close()

	for i, expected := range commands {
		cmd, err := bufDec.Decode()
		require.NoError(t, err, "decode %d", i)
		require.Equal(t, expected.ConnID, cmd.ConnID, "decode %d", i)
		require.Equal(t, expected.StartTs, cmd.StartTs, "decode %d", i)
	}

	_, err := bufDec.Decode()
	require.Equal(t, io.EOF, err)
}

func TestBufferedDecoderAsyncFilling(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(20 * time.Millisecond)},
		{ConnID: 1, StartTs: now.Add(30 * time.Millisecond)},
	}

	slowDec := newMockSlowDecoder(commands, 50*time.Millisecond)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, slowDec, 2)
	defer bufDec.Close()

	start := time.Now()
	cmd1, err := bufDec.Decode()
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, commands[0].ConnID, cmd1.ConnID)
	require.Less(t, elapsed, 100*time.Millisecond)
}

func TestBufferedDecoderBufferSizeLimit(t *testing.T) {
	now := time.Now()
	commands := make([]*cmd.Command, 10)
	for i := range commands {
		commands[i] = &cmd.Command{
			ConnID:  uint64(i + 1),
			StartTs: now.Add(time.Duration(i*10) * time.Millisecond),
		}
	}

	fastDec := newMockSlowDecoder(commands, 1*time.Millisecond)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, fastDec, 3)
	defer bufDec.Close()

	for i := 0; i < 3; i++ {
		cmd, err := bufDec.Decode()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), cmd.ConnID)
	}

	for i := 3; i < 10; i++ {
		cmd, err := bufDec.Decode()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), cmd.ConnID)
	}

	_, err := bufDec.Decode()
	require.Equal(t, io.EOF, err)
}

func TestBufferedDecoderBlockingBehavior(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
	}

	slowDec := newMockSlowDecoder(commands, 200*time.Millisecond)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, slowDec, 1)
	defer bufDec.Close()

	start := time.Now()
	cmd, err := bufDec.Decode()
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, commands[0].ConnID, cmd.ConnID)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestBufferedDecoderOrdering(t *testing.T) {
	now := time.Now()

	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(30 * time.Millisecond)},
		{ConnID: 2, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 3, StartTs: now.Add(20 * time.Millisecond)},
		{ConnID: 4, StartTs: now.Add(40 * time.Millisecond)},
		{ConnID: 5, StartTs: now.Add(5 * time.Millisecond)},
	}

	mockDec := newMockDecoder(commands)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, mockDec, 10)
	defer bufDec.Close()

	time.Sleep(100 * time.Millisecond)

	expectedOrder := []uint64{5, 2, 3, 1, 4}
	expectedTimes := []time.Time{
		now.Add(5 * time.Millisecond),
		now.Add(10 * time.Millisecond),
		now.Add(20 * time.Millisecond),
		now.Add(30 * time.Millisecond),
		now.Add(40 * time.Millisecond),
	}

	for i, expectedConnID := range expectedOrder {
		cmd, err := bufDec.Decode()
		require.NoError(t, err, "decode %d", i)
		require.Equal(t, expectedConnID, cmd.ConnID, "decode %d", i)
		require.Equal(t, expectedTimes[i], cmd.StartTs, "decode %d", i)
	}

	_, err := bufDec.Decode()
	require.Equal(t, io.EOF, err)
}

func TestBufferedDecoderErrorPropagation(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
		{ConnID: 2, StartTs: now.Add(20 * time.Millisecond)},
	}

	testErr := errors.New("test decoder error")
	errDec := newMockErrorDecoder(commands, 1, testErr)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, errDec, 5)
	defer bufDec.Close()

	// First command should succeed
	cmd, err := bufDec.Decode()
	require.NoError(t, err)
	require.Equal(t, commands[0].ConnID, cmd.ConnID)

	// Second decode should return the error
	_, err = bufDec.Decode()
	require.Equal(t, testErr, err)

	// Subsequent calls should also return the same error
	_, err = bufDec.Decode()
	require.Equal(t, testErr, err)
}

func TestBufferedDecoderImmediateError(t *testing.T) {
	testErr := errors.New("immediate error")
	errDec := newMockErrorDecoder([]*cmd.Command{}, 0, testErr)
	ctx := context.Background()
	bufDec := newBufferedDecoder(ctx, errDec, 5)
	defer bufDec.Close()

	_, err := bufDec.Decode()
	require.Equal(t, testErr, err)
}

func TestBufferedDecoderContextCancellation(t *testing.T) {
	now := time.Now()
	commands := make([]*cmd.Command, 10)
	for i := range commands {
		commands[i] = &cmd.Command{
			ConnID:  uint64(i + 1),
			StartTs: now.Add(time.Duration(i*10) * time.Millisecond),
		}
	}

	slowDec := newMockSlowDecoder(commands, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	bufDec := newBufferedDecoder(ctx, slowDec, 5)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := bufDec.Decode()
		require.Error(t, err)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Decode operation did not respond to context cancellation")
	}
}

func TestBufferedDecoderContextCancellationBeforeStart(t *testing.T) {
	now := time.Now()
	commands := []*cmd.Command{
		{ConnID: 1, StartTs: now.Add(10 * time.Millisecond)},
	}

	mockDec := newMockDecoder(commands)
	ctx, cancel := context.WithCancel(context.Background())

	cancel()

	bufDec := newBufferedDecoder(ctx, mockDec, 5)
	defer bufDec.Close()

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	_, err := bufDec.Decode()
	elapsed := time.Since(start)

	require.Less(t, elapsed, 100*time.Millisecond)
	require.Error(t, err)
}

type randomStartTsDecoder struct {
	now    time.Time
	connID uint64
}

func (d *randomStartTsDecoder) Decode() (*cmd.Command, error) {
	cmd := &cmd.Command{
		ConnID:  d.connID,
		StartTs: d.now.Add(time.Duration(rand.Intn(1000)) * time.Millisecond),
	}
	return cmd, nil
}

func BenchmarkBufferedDecoder(b *testing.B) {
	bufferSizes := []int{0, 1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000}

	for _, bufSize := range bufferSizes {
		b.Run(fmt.Sprintf("BufSize%d", bufSize), func(b *testing.B) {
			ctx := context.Background()
			var dec decoder
			dec = &randomStartTsDecoder{
				now:    time.Now(),
				connID: 1,
			}
			if bufSize > 0 {
				dec = newBufferedDecoder(ctx, dec, bufSize)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := dec.Decode()
				if err != nil {
					b.Fatal(err)
				}
			}

			if closable, ok := dec.(closableDecoder); ok {
				closable.Close()
			}
		})
	}
}
