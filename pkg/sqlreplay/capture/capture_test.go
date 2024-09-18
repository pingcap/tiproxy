// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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
	cpt.Capture(packet, time.Now(), 100, mockInitSession)
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
	}

	// start capture and the traffic should be outputted
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100, mockInitSession)
	cpt.Stop(errors.Errorf("mock error"))
	data := writer.getData()
	require.Greater(t, len(data), 0)
	require.Contains(t, string(data), "select 1")
	require.Equal(t, uint64(2), cpt.capturedCmds)

	// stop capture and traffic should not be outputted
	cpt.Capture(packet, time.Now(), 100, mockInitSession)
	cpt.wg.Wait()
	require.Equal(t, len(data), len(writer.getData()))

	// start capture again
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(packet, time.Now(), 100, mockInitSession)
	cpt.Stop(nil)
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
				id := rand.Intn(100) + 1
				cpt.Capture(packet, time.Now(), uint64(id), mockInitSession)
			}
		}
	})
	wg.Run(func() {
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Microsecond):
				cpt.InitConn(time.Now(), uint64(i), "abc")
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

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(packet, time.Now(), 100, mockInitSession)
	cpt.Stop(errors.Errorf("mock error"))
	cpt.wg.Wait()
	progress, err = cpt.Progress()
	require.ErrorContains(t, err, "mock error")
	require.GreaterOrEqual(t, progress, 0.5)
	require.Less(t, progress, 1.0)

	m := store.Meta{}
	require.NoError(t, m.Read(cfg.Output))
	require.Equal(t, uint64(2), m.Cmds)
	require.GreaterOrEqual(t, m.Duration, 5*time.Second)
	require.Less(t, m.Duration, 10*time.Second)
}

func TestInitConn(t *testing.T) {
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
	}

	require.NoError(t, cpt.Start(cfg))
	cpt.InitConn(time.Now(), 100, "mockDB")
	cpt.Capture(packet, time.Now(), 100, func() (string, error) {
		return "init session 100", nil
	})
	cpt.Capture(packet, time.Now(), 101, func() (string, error) {
		return "init session fail 101", errors.New("init session fail 101")
	})
	cpt.Capture(packet, time.Now(), 101, func() (string, error) {
		return "init session 101", nil
	})
	cpt.Stop(errors.Errorf("mock error"))
	data := string(writer.getData())
	require.Equal(t, 1, strings.Count(data, "mockDB"))
	require.Equal(t, 0, strings.Count(data, "init session 100"))
	require.Equal(t, 0, strings.Count(data, "init session fail 101"))
	require.Equal(t, 1, strings.Count(data, "init session 101"))
	require.Equal(t, 2, strings.Count(data, "select 1"))
	require.Equal(t, uint64(4), cpt.capturedCmds)
}

func TestQuit(t *testing.T) {
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	quitPacket := []byte{pnet.ComQuit.Byte()}
	queryPacket := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    t.TempDir(),
		Duration:  10 * time.Second,
		cmdLogger: writer,
	}

	require.NoError(t, cpt.Start(cfg))
	// 100: quit
	cpt.Capture(quitPacket, time.Now(), 100, func() (string, error) {
		return "init session 100", nil
	})
	// 101: select + quit + quit
	cpt.Capture(queryPacket, time.Now(), 101, func() (string, error) {
		return "init session 101", nil
	})
	cpt.Capture(quitPacket, time.Now(), 101, func() (string, error) {
		return "init session 101", nil
	})
	cpt.Capture(quitPacket, time.Now(), 101, func() (string, error) {
		return "init session 101", nil
	})
	cpt.Stop(errors.Errorf("mock error"))

	data := string(writer.getData())
	require.Equal(t, 0, strings.Count(data, "init session 100"))
	require.Equal(t, 1, strings.Count(data, "init session 101"))
	require.Equal(t, 1, strings.Count(data, "select 1"))
	require.Equal(t, 1, strings.Count(data, "# Cmd_type: Quit"))
	require.Equal(t, uint64(3), cpt.capturedCmds)
}
