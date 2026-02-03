// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
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
	dir := t.TempDir()
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(StmtInfo{
		Request:     packet,
		StartTime:   time.Now(),
		ConnID:      100,
		InitSession: mockInitSession,
	})
	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:    dir,
		Duration:  10 * time.Second,
		cmdLogger: writer,
		StartTime: time.Now(),
	}

	// start capture and the traffic should be outputted
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(StmtInfo{
		Request:     packet,
		StartTime:   time.Now(),
		ConnID:      100,
		InitSession: mockInitSession,
	})
	cpt.Stop(errors.Errorf("mock error"))
	data := writer.getData()
	require.Greater(t, len(data), 0)
	require.Contains(t, string(data), "select 1")
	require.Equal(t, uint64(2), cpt.capturedCmds)

	// stop capture and traffic should not be outputted
	cpt.Capture(StmtInfo{
		Request:     packet,
		StartTime:   time.Now(),
		ConnID:      100,
		InitSession: mockInitSession,
	})
	cpt.wg.Wait()
	require.Equal(t, len(data), len(writer.getData()))

	// start capture again
	removeMeta(dir)
	require.NoError(t, cpt.Start(cfg))
	cpt.Capture(StmtInfo{
		Request:     packet,
		StartTime:   time.Now(),
		ConnID:      100,
		InitSession: mockInitSession,
	})
	cpt.Stop(nil)
	require.Greater(t, len(writer.getData()), len(data))

	// duplicated start and stop
	removeMeta(dir)
	require.NoError(t, cpt.Start(cfg))
	require.Error(t, cpt.Start(cfg))
	cpt.Stop(nil)
	cpt.Stop(nil)
}

func TestConcurrency(t *testing.T) {
	dir := t.TempDir()
	cpt := NewCapture(zap.NewNop())
	defer cpt.Close()

	writer := newMockWriter(store.WriterCfg{})
	cfg := CaptureConfig{
		Output:         dir,
		Duration:       10 * time.Second,
		StartTime:      time.Now(),
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
				cpt.Capture(StmtInfo{
					Request:     packet,
					StartTime:   time.Now(),
					ConnID:      uint64(id),
					InitSession: mockInitSession,
				})
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
					removeMeta(dir)
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
	now := time.Now()
	dir := t.TempDir()
	path := filepath.Join(dir, "traffic.log")
	require.NoError(t, os.WriteFile(path, []byte{}, 0666))
	cfgs := []CaptureConfig{
		{
			Duration:  10 * time.Second,
			StartTime: now,
		},
		{
			Output:    dir,
			StartTime: now,
		},
		{
			Duration: 10 * time.Second,
			Output:   dir,
		},
		{
			Duration:  10 * time.Second,
			Output:    path,
			StartTime: now.Add(time.Hour),
		},
		{
			Duration:  10 * time.Second,
			Output:    path,
			StartTime: now.Add(-time.Hour),
		},
		{
			Duration:  10 * time.Second,
			Output:    path,
			StartTime: now,
		},
		{
			Duration:         10 * time.Second,
			Output:           dir,
			StartTime:        now,
			EncryptionMethod: store.EncryptAes,
			KeyFile:          "",
		},
	}

	for i, cfg := range cfgs {
		storage, err := cfg.Validate()
		require.Error(t, err, "case %d", i)
		if storage != nil && !reflect.ValueOf(storage).IsNil() {
			storage.Close()
		}
	}

	cfg := CaptureConfig{
		Output:    dir,
		Duration:  10 * time.Second,
		StartTime: now,
	}
	storage, err := cfg.Validate()
	require.NoError(t, err)
	require.NotNil(t, storage)
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
		StartTime: time.Now(),
	}
	setStartTime := func(t time.Time) {
		cpt.Lock()
		cpt.startTime = t
		cpt.Unlock()
	}

	now := time.Now()
	require.NoError(t, cpt.Start(cfg))
	progress, _, done, err := cpt.Progress()
	require.NoError(t, err)
	require.Less(t, progress, 0.3)
	require.False(t, done)

	setStartTime(now.Add(-5 * time.Second))
	progress, _, _, err = cpt.Progress()
	require.NoError(t, err)
	require.GreaterOrEqual(t, progress, 0.5)

	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(StmtInfo{
		Request:     packet,
		StartTime:   time.Now(),
		ConnID:      100,
		InitSession: mockInitSession,
	})
	cpt.Stop(errors.Errorf("mock error"))
	progress, _, done, err = cpt.Progress()
	require.ErrorContains(t, err, "mock error")
	require.GreaterOrEqual(t, progress, 0.5)
	require.Less(t, progress, 1.0)
	require.True(t, done)

	m := store.Meta{}
	storage, err := store.NewStorage(cfg.Output)
	require.NoError(t, err)
	defer storage.Close()
	require.NoError(t, m.Read(storage))
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
		StartTime: time.Now(),
	}

	require.NoError(t, cpt.Start(cfg))
	cpt.InitConn(time.Now(), 100, "mockDB")
	cpt.Capture(StmtInfo{
		Request:   packet,
		StartTime: time.Now(),
		ConnID:    100,
		InitSession: func() (string, error) {
			return "init session 100", nil
		},
	})
	cpt.Capture(StmtInfo{
		Request:   packet,
		StartTime: time.Now(),
		ConnID:    101,
		InitSession: func() (string, error) {
			return "init session fail 101", errors.New("init session fail 101")
		},
	})
	cpt.Capture(StmtInfo{
		Request:   packet,
		StartTime: time.Now(),
		ConnID:    101,
		InitSession: func() (string, error) {
			return "init session 101", nil
		},
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
		StartTime: time.Now(),
	}

	require.NoError(t, cpt.Start(cfg))
	// 100: quit
	cpt.Capture(StmtInfo{
		Request:   quitPacket,
		StartTime: time.Now(),
		ConnID:    100,
		InitSession: func() (string, error) {
			return "init session 100", nil
		},
	})
	// 101: select + quit + quit
	cpt.Capture(StmtInfo{
		Request:   queryPacket,
		StartTime: time.Now(),
		ConnID:    101,
		InitSession: func() (string, error) {
			return "init session 101", nil
		},
	})
	cpt.Capture(StmtInfo{
		Request:   quitPacket,
		StartTime: time.Now(),
		ConnID:    101,
		InitSession: func() (string, error) {
			return "init session 101", nil
		},
	})
	cpt.Capture(StmtInfo{
		Request:   quitPacket,
		StartTime: time.Now(),
		ConnID:    101,
		InitSession: func() (string, error) {
			return "init session 101", nil
		},
	})
	cpt.Stop(errors.Errorf("mock error"))

	data := string(writer.getData())
	require.Equal(t, 0, strings.Count(data, "init session 100"))
	require.Equal(t, 1, strings.Count(data, "init session 101"))
	require.Equal(t, 1, strings.Count(data, "select 1"))
	require.Equal(t, 1, strings.Count(data, "# Cmd_type: Quit"))
	require.Equal(t, uint64(3), cpt.capturedCmds)
}

func TestFilterCmds(t *testing.T) {
	tests := []struct {
		packet  []byte
		want    string
		notWant string
	}{
		{
			packet: pnet.MakeChangeUser(&pnet.ChangeUserReq{
				User: "root",
				DB:   "test",
			}, 0),
			want:    pnet.ComResetConnection.String(),
			notWant: pnet.ComChangeUser.String(),
		},
		{
			packet:  append([]byte{pnet.ComQuery.Byte()}, []byte("CREATE USER u1 IDENTIFIED BY '123456'")...),
			notWant: "123456",
		},
		{
			packet: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
			want:   "select 1",
		},
	}

	dir := t.TempDir()
	cfg := CaptureConfig{
		Output:    dir,
		Duration:  10 * time.Second,
		StartTime: time.Now(),
	}
	for i, test := range tests {
		cpt := NewCapture(zap.NewNop())
		writer := newMockWriter(store.WriterCfg{})
		cfg.cmdLogger = writer
		removeMeta(dir)
		require.NoError(t, cpt.Start(cfg))
		cpt.Capture(StmtInfo{
			Request:   test.packet,
			StartTime: time.Now(),
			ConnID:    100,
			InitSession: func() (string, error) {
				return "init session 100", nil
			},
		})
		cpt.Stop(nil)

		data := string(writer.getData())
		if len(test.want) > 0 {
			require.Equal(t, 1, strings.Count(data, test.want), "case %d", i)
			require.Equal(t, uint64(2), cpt.capturedCmds, "case %d", i)
			require.Equal(t, uint64(0), cpt.filteredCmds, "case %d", i)
		} else {
			require.Equal(t, uint64(1), cpt.capturedCmds, "case %d", i)
			require.Equal(t, uint64(1), cpt.filteredCmds, "case %d", i)
		}
		if len(test.notWant) > 0 {
			require.Equal(t, 0, strings.Count(data, test.notWant), "case %d", i)
		}

		cpt.Close()
	}
}

func removeMeta(dir string) {
	_ = os.Remove(filepath.Join(dir, "meta"))
}
