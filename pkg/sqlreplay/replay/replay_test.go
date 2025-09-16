// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"fmt"
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
		readers:   []cmd.LineReader{loader},
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
			for _, s := range storage {
				s.Close()
			}
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
			readers:   []cmd.LineReader{loader},
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
		for i := range 10 {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command, cmd.FormatNative)
		}
		require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

		var firstTime, lastTime time.Time
		for i := range 10 {
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
		readers:   []cmd.LineReader{loader},
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

	for i := range 2 {
		for range 10 {
			command := newMockCommand(1)
			command.StartTs = now.Add(time.Duration(i*10) * time.Millisecond)
			loader.writeCommand(command, cmd.FormatNative)
		}

		require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
		for range 10 {
			<-cmdCh
			progress, _, _, err := replay.Progress()
			require.NoError(t, err)
			require.GreaterOrEqual(t, progress, float64(i)/10)
			require.LessOrEqual(t, progress, 1.0)
			// Maybe unstable due to goroutine schedule.
			// require.LessOrEqual(t, progress, float64(i+2)/10)
		}
		replay.Wait()
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
		readers:   []cmd.LineReader{loader},
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
	for range 20 {
		command := newMockCommand(1)
		command.StartTs = now
		loader.writeCommand(command, cmd.FormatNative)
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
		readers:   []cmd.LineReader{loader},
	}
	for i, test := range tests {
		cfg.KeyFile = test.keyFile
		replay := NewReplay(zap.NewNop(), id.NewIDManager())
		cfg.report = newMockReport(replay.exceptionCh)
		err = replay.Start(cfg, nil, nil, &backend.BCConfig{})
		if len(test.err) > 0 {
			require.ErrorContains(t, err, test.err, "test %d", i)
		} else {
			require.NoError(t, err)
			replay.Lock()
			require.Equal(t, key, replay.cfg.encryptionKey)
			replay.Unlock()
			replay.Close()
		}
	}
}

func TestIgnoreErrors(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	cmdCh := make(chan *cmd.Command, 10)
	loader := newMockNormalLoader()
	cfg := ReplayConfig{
		Input:      t.TempDir(),
		Username:   "u1",
		StartTime:  time.Now(),
		IgnoreErrs: true,
		readers:    []cmd.LineReader{loader},
		report:     newMockReport(replay.exceptionCh),
		connCreator: func(connID uint64) conn.Conn {
			return &mockConn{
				connID:  connID,
				cmdCh:   cmdCh,
				closeCh: replay.closeCh,
				closed:  make(chan struct{}),
			}
		},
	}

	loader.write([]byte("invalid command\n"))
	now := time.Now()
	command := newMockCommand(1)
	command.StartTs = now
	loader.writeCommand(command, cmd.FormatAuditLogPlugin)
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

	<-cmdCh
	replay.Stop(nil)
	loader.Close()
}

func BenchmarkMultiBufferedDecoder(b *testing.B) {
	bufferSizes := []int{-1, 0, 1, 2, 4, 8, 16, 32, 64, 128, 256}
	readerCounts := []int{1, 2, 4, 8, 16, 32}

	for _, bufSize := range bufferSizes {
		for _, readerCount := range readerCounts {
			b.Run(fmt.Sprintf("BufSize%d/ReaderCount%d", bufSize, readerCount), func(b *testing.B) {
				ctx := context.Background()
				readers := make([]cmd.LineReader, readerCount)
				for i := range readers {
					readers[i] = &endlessReader{
						line: `[2025/09/14 16:16:31.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=b] [EVENT=COMPLETED]`,
					}
				}
				r := &replay{
					cfg: ReplayConfig{
						Format: cmd.FormatAuditLogPlugin,
					},
				}
				decoder, err := r.constructMergeDecoders(ctx, readers)
				require.NoError(b, err)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := decoder.Decode()
					require.NoError(b, err)
				}
			})
		}
	}
}
