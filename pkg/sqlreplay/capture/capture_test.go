// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStartAndStop(t *testing.T) {
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(packet, time.Now(), 100)
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
	}

	// start capture and the traffic should be outputted
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100)
	cpt.Stop(errors.Errorf("mock error"))
	cpt.wg.Wait()
	data := writer.getData()
	require.Greater(t, len(data), 0)
	require.Contains(t, string(data), "select 1")
	require.Equal(t, uint64(1), cpt.capturedCmds)

	// stop capture and traffic should not be outputted
	cpt.Capture(packet, time.Now(), 100)
	cpt.wg.Wait()
	require.Equal(t, len(data), len(writer.getData()))

	// start capture again
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100)
	cpt.Stop(nil)
	cpt.wg.Wait()
	require.Greater(t, len(writer.getData()), len(data))

	// duplicated start and stop
	require.NoError(t, cpt.Start(cfg))
	require.Error(t, cpt.Start(cfg))
	cpt.Stop(nil)
	cpt.Stop(nil)
}

func TestConcurrency(t *testing.T) {
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:         t.TempDir(),
		Duration:       10 * time.Second,
		bufferCap:      12 * 1024,
		flushThreshold: 8 * 1024,
		cmdLogger:      writer,
	}
	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	wg.Run(func() {
		packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Microsecond):
				cpt.Capture(packet, time.Now(), 100)
			}
		}
	})
	wg.Run(func() {
		started := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				if started {
					cpt.Stop(nil)
					started = false
				} else {
					require.NoError(t, cpt.Start(cfg))
					started = true
				}
			}
		}
	})
	wg.Wait()
	cancel()

	require.Greater(t, len(writer.getData()), 0)
}

func TestCaptureCfgError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "traffic.log")
	require.NoError(t, os.WriteFile(path, []byte{}, 0666))
	cfgs := []CaptureConfig{
		{
			Duration: 10 * time.Second,
		},
		{
			Output: dir,
		},
		{
			Duration: 10 * time.Second,
			Output:   path,
		},
	}

	for i, cfg := range cfgs {
		err := cfg.Validate()
		require.Error(t, err, "case %d", i)
	}

	cfg := CaptureConfig{
		Output:   dir,
		Duration: 10 * time.Second,
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, bufferCap, cfg.bufferCap)
	require.Equal(t, flushThreshold, cfg.flushThreshold)
	require.Equal(t, maxBuffers, cfg.maxBuffers)
	require.Equal(t, maxPendingCommands, cfg.maxPendingCommands)
}

func TestProgress(t *testing.T) {
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
	}
	setStartTime := func(t time.Time) {
		cpt.Lock()
		cpt.startTime = t
		cpt.Unlock()
	}

	now := time.Now()
	require.NoError(t, cpt.Start(cfg))
	progress, err := cpt.Progress()
	require.NoError(t, err)
	require.Less(t, progress, 0.3)

	setStartTime(now.Add(-5 * time.Second))
	progress, err = cpt.Progress()
	require.NoError(t, err)
	require.GreaterOrEqual(t, progress, 0.5)

	cpt.Stop(errors.Errorf("mock error"))
	cpt.wg.Wait()
	progress, err = cpt.Progress()
	require.ErrorContains(t, err, "mock error")
	require.GreaterOrEqual(t, progress, 0.5)
	require.Less(t, progress, 1.0)
}
