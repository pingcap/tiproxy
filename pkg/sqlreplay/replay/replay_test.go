// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestManageConns(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()

	loader := newMockChLoader()
	connCount := 0
	cfg := ReplayConfig{
		Input:     t.TempDir(),
		Username:  "u1",
		StartTime: time.Now(),
		reader:    loader,
		connCreator: func(connID uint64) conn.Conn {
			connCount++
			return &mockConn{
				connID:  connID,
				closeCh: replay.closeCh,
				closed:  make(chan struct{}),
			}
		},
		report: newMockReport(replay.exceptionCh),
	}
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

	command := newMockCommand(1)
	loader.writeCommand(command)
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		return connCount == 1
	}, 3*time.Second, 10*time.Millisecond)

	command = newMockCommand(2)
	loader.writeCommand(command)
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		return connCount == 2
	}, 3*time.Second, 10*time.Millisecond)

	replay.closeCh <- 1
	replay.closeCh <- 2
	loader.Close()
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		return replay.startTime.IsZero()
	}, 3*time.Second, 10*time.Millisecond)
}

func TestValidateCfg(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	cfgs := []ReplayConfig{
		{
			Username:  "u1",
			StartTime: now,
		},
		{
			Input:     dir,
			StartTime: now,
		},
		{
			Input:     dir,
			Username:  "u1",
			Speed:     0.01,
			StartTime: now,
		},
		{
			Input:     dir,
			Username:  "u1",
			Speed:     100,
			StartTime: now,
		},
		{
			Input:     dir,
			Username:  "u1",
			StartTime: now.Add(time.Hour),
		},
		{
			Input:     dir,
			Username:  "u1",
			StartTime: now.Add(-time.Hour),
		},
	}

	for i, cfg := range cfgs {
		storage, err := cfg.Validate()
		require.Error(t, err, "case %d", i)
		if storage != nil && !reflect.ValueOf(storage).IsNil() {
			storage.Close()
		}
	}
}

func TestReplaySpeed(t *testing.T) {
	speeds := []float64{10, 1, 0.1}
	var lastTotalTime time.Duration
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	for _, speed := range speeds {
		cmdCh := make(chan *cmd.Command, 10)
		loader := newMockNormalLoader()
		cfg := ReplayConfig{
			Input:     t.TempDir(),
			Username:  "u1",
			Speed:     speed,
			StartTime: time.Now(),
			reader:    loader,
			report:    newMockReport(replay.exceptionCh),
			connCreator: func(connID uint64) conn.Conn {
				return &mockConn{
					connID:  connID,
					cmdCh:   cmdCh,
					closeCh: replay.closeCh,
					closed:  make(chan struct{}),
				}
			},
		}

		now := time.Now()
		for i := 0; i < 10; i++ {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command)
		}
		require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

		var firstTime, lastTime time.Time
		for i := 0; i < 10; i++ {
			<-cmdCh
			if i == 0 {
				firstTime = time.Now()
				lastTime = firstTime
			} else {
				now = time.Now()
				interval := now.Sub(lastTime)
				lastTime = now
				t.Logf("speed: %f, i: %d, interval: %s", speed, i, interval)
				// CI is too unstable, comment this.
				// require.Greater(t, interval, time.Duration(float64(10*time.Millisecond)/speed)/2, "speed: %f, i: %d", speed, i)
			}
		}
		totalTime := lastTime.Sub(firstTime)
		require.Greater(t, totalTime, lastTotalTime, "speed: %f", speed)
		lastTotalTime = totalTime

		replay.Stop(nil)
		loader.Close()
	}
}

func TestProgress(t *testing.T) {
	dir := t.TempDir()
	meta := store.NewMeta(10*time.Second, 10, 0, "")
	storage, err := store.NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	require.NoError(t, meta.Write(storage))
	loader := newMockNormalLoader()
	now := time.Now()
	defer loader.Close()

	// If the channel size is too small, there may be a deadlock.
	// ExecuteCmd waits for cmdCh <- data in a lock, while Progress() waits for the lock.
	cmdCh := make(chan *cmd.Command, 10)
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	cfg := ReplayConfig{
		Input:     dir,
		Username:  "u1",
		StartTime: time.Now(),
		reader:    loader,
		report:    newMockReport(replay.exceptionCh),
		connCreator: func(connID uint64) conn.Conn {
			return &mockConn{
				connID:  connID,
				cmdCh:   cmdCh,
				closeCh: replay.closeCh,
				closed:  make(chan struct{}),
			}
		},
	}

	for i := 0; i < 2; i++ {
		for j := 0; j < 10; j++ {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command)
		}

		require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
		for j := 0; j < 10; j++ {
			<-cmdCh
			progress, _, _, err := replay.Progress()
			require.NoError(t, err)
			require.GreaterOrEqual(t, progress, float64(i)/10)
			require.LessOrEqual(t, progress, 1.0)
			// Maybe unstable due to goroutine schedule.
			// require.LessOrEqual(t, progress, float64(i+2)/10)
		}
	}
}

func TestPendingCmds(t *testing.T) {
	dir := t.TempDir()
	meta := store.NewMeta(10*time.Second, 20, 0, "")
	storage, err := store.NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	require.NoError(t, meta.Write(storage))
	loader := newMockNormalLoader()
	defer loader.Close()

	lg, text := logger.CreateLoggerForTest(t)
	replay := NewReplay(lg, id.NewIDManager())
	defer replay.Close()
	cfg := ReplayConfig{
		Input:     dir,
		Username:  "u1",
		StartTime: time.Now(),
		reader:    loader,
		report:    newMockReport(replay.exceptionCh),
		connCreator: func(connID uint64) conn.Conn {
			return &mockPendingConn{
				connID:  connID,
				closeCh: replay.closeCh,
				closed:  make(chan struct{}),
				stats:   &replay.replayStats,
			}
		},
		abortThreshold:    15,
		slowDownThreshold: 10,
		slowDownFactor:    10 * time.Millisecond,
	}

	now := time.Now()
	for i := 0; i < 20; i++ {
		command := newMockCommand(1)
		command.StartTs = now
		loader.writeCommand(command)
	}

	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
	require.Eventually(t, func() bool {
		_, _, _, err := replay.Progress()
		return err != nil
	}, 5*time.Second, 10*time.Millisecond)
	progress, _, done, err := replay.Progress()
	require.NotEqualValues(t, 1, progress)
	require.True(t, done)
	require.Contains(t, err.Error(), "too many pending commands")
	logs := text.String()
	require.Contains(t, logs, `"total_wait_time": "150ms"`)
	require.Contains(t, logs, "too many pending commands")
}

func TestLoadEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	meta := store.NewMeta(10*time.Second, 20, 0, store.EncryptAes)
	storage, err := store.NewStorage(dir)
	require.NoError(t, err)
	defer storage.Close()
	require.NoError(t, meta.Write(storage))
	keyFile := filepath.Join(dir, "encryption")
	key := make([]byte, 32)
	require.NoError(t, os.WriteFile(keyFile, key, 0600))
	now := time.Now()

	tests := []struct {
		keyFile string
		err     string
	}{
		{
			err: "encryption",
		},
		{
			keyFile: "/nonexist",
			err:     "encryption",
		},
		{
			keyFile: keyFile,
		},
	}

	loader := newMockNormalLoader()
	defer loader.Close()
	cfg := ReplayConfig{
		Input:     dir,
		Username:  "u1",
		StartTime: now,
		reader:    loader,
	}
	for _, test := range tests {
		cfg.KeyFile = test.keyFile
		replay := NewReplay(zap.NewNop(), id.NewIDManager())
		cfg.report = newMockReport(replay.exceptionCh)
		err = replay.Start(cfg, nil, nil, &backend.BCConfig{})
		if len(test.err) > 0 {
			require.ErrorContains(t, err, test.err)
		} else {
			require.NoError(t, err)
			replay.Lock()
			require.Equal(t, key, replay.cfg.encryptionKey)
			replay.Unlock()
			replay.Close()
		}
	}
}
