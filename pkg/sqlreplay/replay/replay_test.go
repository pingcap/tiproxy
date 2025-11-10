// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/store"
	"github.com/siddontang/go/hack"
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
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			connCount++
			return &mockConn{
				connID:  connID,
				closeCh: replay.closeConnCh,
				closed:  make(chan struct{}),
			}
		},
		report:          newMockReport(replay.exceptionCh),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
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

	replay.closeConnCh <- 1
	replay.closeConnCh <- 2
	loader.Close()
	require.Eventually(t, func() bool {
		replay.Lock()
		defer replay.Unlock()
		return replay.startTime.IsZero()
	}, 3*time.Second, 10*time.Millisecond)
}

func TestCloseConns(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()

	i := 1
	loader := &customizedReader{
		getCmd: func() *cmd.Command {
			if i >= 2000 {
				return nil
			}
			command := newMockCommand(uint64(i))
			command.StartTs = time.Unix(0, 1)
			i++
			return command
		},
	}
	defer loader.Close()

	cfg := ReplayConfig{
		Input:     t.TempDir(),
		Username:  "u1",
		StartTime: time.Now(),
		readers:   []cmd.LineReader{loader},
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			return &nopConn{
				connID:     connID,
				closeCh:    replay.closeConnCh,
				execInfoCh: replay.execInfoCh,
				stats:      &replay.replayStats,
			}
		},
		report:          newMockReport(replay.exceptionCh),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
	}
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
	replay.Wait()
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
		{
			Input:          dir,
			CommandEndTime: time.Now(),
			Format:         cmd.FormatNative,
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
			connCreator: func(connID uint64, _ uint64) conn.Conn {
				return &mockConn{
					connID:  connID,
					cmdCh:   cmdCh,
					closeCh: replay.closeConnCh,
					closed:  make(chan struct{}),
				}
			},
			PSCloseStrategy: cmd.PSCloseStrategyDirected,
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

		replay.Stop(nil, false)
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
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			return &mockConn{
				connID:  connID,
				cmdCh:   cmdCh,
				closeCh: replay.closeConnCh,
				closed:  make(chan struct{}),
			}
		},
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
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
			progress, _, _, _, _, err := replay.Progress()
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
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			return &mockPendingConn{
				connID:  connID,
				closeCh: replay.closeConnCh,
				closed:  make(chan struct{}),
				stats:   &replay.replayStats,
			}
		},
		abortThreshold:    15,
		slowDownThreshold: 10,
		slowDownFactor:    10 * time.Millisecond,
		PSCloseStrategy:   cmd.PSCloseStrategyDirected,
	}

	now := time.Now()
	for range 20 {
		command := newMockCommand(1)
		command.StartTs = now
		loader.writeCommand(command, cmd.FormatNative)
	}

	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
	require.Eventually(t, func() bool {
		_, _, _, _, _, err := replay.Progress()
		return err != nil
	}, 5*time.Second, 10*time.Millisecond)
	progress, _, _, _, done, err := replay.Progress()
	require.NotEqualValues(t, 1, progress)
	require.True(t, done)
	require.Contains(t, err.Error(), "too many pending commands")
	logs := text.String()
	require.Contains(t, logs, `"extra_wait_time": "150ms"`)
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
		Input:           dir,
		Username:        "u1",
		StartTime:       now,
		readers:         []cmd.LineReader{loader},
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
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
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			return &mockConn{
				connID:  connID,
				cmdCh:   cmdCh,
				closeCh: replay.closeConnCh,
				closed:  make(chan struct{}),
			}
		},
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
	}

	loader.write([]byte("invalid command\n"))
	now := time.Now()
	command := newMockCommand(1)
	command.StartTs = now
	loader.writeCommand(command, cmd.FormatAuditLogPlugin)
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

	<-cmdCh
	replay.Stop(nil, false)
	loader.Close()
}

func TestGracefulStop(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()

	i := 0
	loader := &customizedReader{
		getCmd: func() *cmd.Command {
			j := rand.Uint64N(100) + 1
			command := newMockCommand(j)
			i++
			command.StartTs = time.Unix(0, int64(i)*int64(time.Microsecond))
			return command
		},
	}

	cfg := ReplayConfig{
		Input:     t.TempDir(),
		Username:  "u1",
		StartTime: time.Now(),
		readers:   []cmd.LineReader{loader},
		connCreator: func(connID uint64, _ uint64) conn.Conn {
			return &mockDelayConn{
				stats:   &replay.replayStats,
				closeCh: replay.closeConnCh,
				connID:  connID,
			}
		},
		report:          newMockReport(replay.exceptionCh),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
	}
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))

	time.Sleep(2 * time.Second)
	replay.Stop(errors.New("graceful stop"), true)
	// check that all the pending commands are replayed
	curCmdTs := replay.replayStats.CurCmdTs.Load()
	require.EqualValues(t, 0, replay.replayStats.PendingCmds.Load())
	require.EqualValues(t, curCmdTs, int64(replay.replayStats.ReplayedCmds.Load())*int64(time.Microsecond))
	_, _, lastTs, _, _, err := replay.Progress()
	require.ErrorContains(t, err, "graceful stop")
	require.Equal(t, curCmdTs, lastTs.UnixNano())
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

func TestDryRun(t *testing.T) {
	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()
	loader := newMockNormalLoader()
	cfg := ReplayConfig{
		DryRun:          true,
		Input:           t.TempDir(),
		StartTime:       time.Now(),
		readers:         []cmd.LineReader{loader},
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
	}

	loader.write([]byte("invalid command\n"))
	now := time.Now()
	command := newMockCommand(1)
	command.StartTs = now
	loader.writeCommand(command, cmd.FormatAuditLogPlugin)
	require.NoError(t, replay.Start(cfg, nil, nil, &backend.BCConfig{}))
	loader.Close()
}

func TestLoadFromCheckpoint(t *testing.T) {
	tests := []struct {
		checkpointData string
		setupFile      bool
		fileNotExists  bool
		expectedError  bool
		cmdTs          int64
		cmdEndTs       int64
	}{
		{
			checkpointData: "",
			setupFile:      false,
			expectedError:  false,
		},
		{
			checkpointData: "",
			setupFile:      false,
			expectedError:  false,
			fileNotExists:  true,
		},
		{
			checkpointData: `{"cur_cmd_ts":1640995200000000000,"cur_cmd_end_ts":1640995300000000000}`,
			setupFile:      true,
			expectedError:  false,
			cmdTs:          1640995200000000000,
			cmdEndTs:       1640995300000000000,
		},
		{
			checkpointData: `{"cur_cmd_ts":1640995200000000000,"cur_cmd_end_ts":0}`,
			setupFile:      true,
			expectedError:  false,
			cmdTs:          1640995200000000000,
		},
		{
			checkpointData: `{"cur_cmd_ts":0,"cur_cmd_end_ts":1640995300000000000}`,
			setupFile:      true,
			expectedError:  false,
			cmdEndTs:       1640995300000000000,
		},
		{
			checkpointData: `{invalid json}`,
			setupFile:      true,
			expectedError:  true,
		},
		{
			checkpointData: "",
			setupFile:      true,
			expectedError:  true,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			cfg := &ReplayConfig{Format: cmd.FormatAuditLogPlugin}

			if tt.fileNotExists {
				cfg.CheckPointFilePath = filepath.Join(t.TempDir(), "nonexistent.json")
			} else if tt.setupFile {
				tmpDir := t.TempDir()
				checkpointFile := filepath.Join(tmpDir, "checkpoint.json")
				err := os.WriteFile(checkpointFile, []byte(tt.checkpointData), 0644)
				require.NoError(t, err)
				cfg.CheckPointFilePath = checkpointFile
			}

			err := cfg.LoadFromCheckpoint()

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.cmdTs > 0 {
				expectedStartTime := time.Unix(0, tt.cmdTs)
				require.Equal(t, expectedStartTime, cfg.CommandStartTime)
			} else {
				require.True(t, cfg.CommandStartTime.IsZero())
			}

			if tt.cmdEndTs > 0 {
				expectedEndTime := time.Unix(0, tt.cmdEndTs)
				require.Equal(t, expectedEndTime, cfg.CommandEndTime)
			} else {
				require.True(t, cfg.CommandEndTime.IsZero())
			}
		})
	}
}

func TestSaveCurrentStateLoop(t *testing.T) {
	t.Run("stops when context is cancelled", func(t *testing.T) {
		tmpDir := t.TempDir()
		checkpointFile := filepath.Join(tmpDir, "checkpoint.json")

		replay := &replay{
			lg: zap.NewNop(),
			cfg: ReplayConfig{
				CheckPointFilePath: checkpointFile,
			},
		}

		ctx, cancel := context.WithCancel(context.Background())

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			replay.saveCheckpointLoop(ctx)
		}()

		replay.replayStats.CurCmdTs.Store(1234567890)
		replay.replayStats.CurCmdEndTs.Store(9876543210)

		require.Eventually(t, func() bool {
			cfg := &ReplayConfig{CheckPointFilePath: checkpointFile, Format: cmd.FormatAuditLogPlugin}
			_ = cfg.LoadFromCheckpoint()

			return cfg.CommandStartTime.UnixNano() == 1234567890 &&
				cfg.CommandEndTime.UnixNano() == 9876543210
		}, 1*time.Second, 10*time.Millisecond, "checkpoint file should be updated with current state")

		cancel()
		wg.Wait()
	})

	t.Run("file truncation on overwrite", func(t *testing.T) {
		tmpDir := t.TempDir()
		checkpointFile := filepath.Join(tmpDir, "checkpoint.json")

		largeData := `{"cur_cmd_ts":1640995200000000000,"cur_cmd_end_ts":1640995300000000000,"large_field":"` + strings.Repeat("x", 1000) + `"}`
		err := os.WriteFile(checkpointFile, []byte(largeData), 0644)
		require.NoError(t, err)

		replay := &replay{
			lg: zap.NewNop(),
			cfg: ReplayConfig{
				CheckPointFilePath: checkpointFile,
			},
		}

		testCmdTs := time.Now().UnixNano()
		testCmdEndTs := time.Now().Add(time.Second).UnixNano()
		replay.replayStats.CurCmdTs.Store(testCmdTs)
		replay.replayStats.CurCmdEndTs.Store(testCmdEndTs)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			replay.saveCheckpointLoop(ctx)
		}()

		require.Eventually(t, func() bool {
			data, err := os.ReadFile(checkpointFile)
			if err != nil {
				return false
			}
			return len(data) < len(largeData) && len(data) > 0
		}, 1*time.Second, 10*time.Millisecond, "checkpoint file should be overwritten with smaller data")

		data, err := os.ReadFile(checkpointFile)
		require.NoError(t, err)

		var state replayCheckpoint
		err = json.Unmarshal(data, &state)
		require.NoError(t, err)
		require.Equal(t, testCmdTs, state.CurCmdTs)
		require.Equal(t, testCmdEndTs, state.CurCmdEndTs)

		cancel()
		wg.Wait()
	})
}

func TestDynamicInput(t *testing.T) {
	// To run this test on S3, please set the S3_URL_FOR_TEST environment variable.
	// Example: s3://test-bucket/tidb-?force-path-style=true&endpoint=http://127.0.0.1:9000&access-key=minioadmin&secret-access-key=minioadmin&provider=minio
	// Or any valid S3 URL.

	tempDir := t.TempDir()
	url := tempDir + "/tidb-"

	if s3Addr, ok := os.LookupEnv("S3_URL_FOR_TEST"); ok {
		url = s3Addr
	}

	replay := NewReplay(zap.NewNop(), id.NewIDManager())
	defer replay.Close()

	replay.cfg = ReplayConfig{
		Input:           url + "," + url,
		Username:        "u1",
		StartTime:       time.Now(),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
		DynamicInput:    true,
		ReplayerCount:   3,
		ReplayerIndex:   1,
		Format:          cmd.FormatAuditLogPlugin,
	}
	// Validate should fail because we have multiple inputs
	_, err := replay.cfg.Validate()
	require.Error(t, err)

	replay.cfg.Input = url
	storages, err := replay.cfg.Validate()
	require.NoError(t, err)
	replay.storages = storages
	defer func() {
		for _, s := range storages {
			s.Close()
		}
	}()

	dirWatcherInterval := 10 * time.Millisecond
	store.SetDirWatcherPollIntervalForTest(dirWatcherInterval)

	auditLog := `[2025/09/08 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/06 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]
		[2025/09/09 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/07 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 2"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]
		[2025/09/10 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/08 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 3"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get a list of files whose hash locates in 0
	locateZeroDirs := []string{}
	otherDirs := []string{}
	for i := 0; len(locateZeroDirs) < 2 || len(otherDirs) < 1; i++ {
		filename := fmt.Sprintf("tidb-%d/", i)
		if _, ok := storages[0].(*storage.LocalStorage); ok {
			filename = tempDir + "/" + filename
		}

		h := fnv.New64a()
		// error is never returned for Hash
		_, _ = h.Write([]byte(filename))
		sum := h.Sum64()

		if sum%replay.cfg.ReplayerCount == replay.cfg.ReplayerIndex {
			if len(locateZeroDirs) < 2 {
				locateZeroDirs = append(locateZeroDirs, fmt.Sprintf("tidb-%d", i))
			}
		} else {
			if len(otherDirs) < 1 {
				otherDirs = append(otherDirs, fmt.Sprintf("tidb-%d", i))
			}
		}
	}

	// This decoder should be able to read the files in locateZeroDirs[0], locateZeroDirs[1], ...
	err = storages[0].WriteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", locateZeroDirs[0]), []byte(auditLog))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, storages[0].DeleteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", locateZeroDirs[0])))
	}()

	// don't use buffer to avoid exhausting S3 file too fast
	chanBufForEachDecoder = 0
	decoder, err := replay.constructDynamicDecoder(ctx)
	require.NoError(t, err)

	// Decode the first command `SELECT 1` from tidb-0
	command, err := decoder.Decode()
	require.NoError(t, err)
	require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), command.Payload)
	require.NoError(t, err)

	// Add a new log to otherDirs[0], which should not be read by this replayer
	err = storages[0].WriteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", otherDirs[0]), []byte(auditLog))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, storages[0].DeleteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", otherDirs[0])))
	}()
	time.Sleep(3 * dirWatcherInterval)

	// Still decode the second command `SELECT 2` from tidb-0
	command, err = decoder.Decode()
	require.NoError(t, err)
	require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte("select 2")...), command.Payload)

	// Add a new log to locateZeroDirs[1], which should be read by this replayer
	err = storages[0].WriteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", locateZeroDirs[1]), []byte(auditLog))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, storages[0].DeleteFile(ctx, fmt.Sprintf("%s/tidb-audit-2006-01-02T15-04-05.log", locateZeroDirs[1])))
	}()
	time.Sleep(3 * dirWatcherInterval)

	// Decode the command `SELECT 1` from locateZeroDirs[1]
	command, err = decoder.Decode()
	require.NoError(t, err)
	require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), command.Payload)

	// Then, the following commands should be `SELECT 2`, `SELECT 3`, `SELECT 3`, EOF
	for _, expectedSQL := range []string{"select 2", "select 3", "select 3"} {
		command, err = decoder.Decode()
		require.NoError(t, err)
		require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte(expectedSQL)...), command.Payload)
	}

	_, err = decoder.Decode()
	require.ErrorIs(t, err, io.EOF)
}

func TestGetDirForInput(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "/path/to/dir",
			expected: "/path/to",
		},
		{
			input:    "/path/to/dir/",
			expected: "/path/to/dir",
		},
		{
			input:    "s3://bucket/prefix-xyz?param=1",
			expected: "s3://bucket/?param=1",
		},
		{
			input:    "s3://bucket/prefix-xyz/",
			expected: "s3://bucket/prefix-xyz",
		},
		{
			input:    "s3://bucket/prefix-xyz/subdir",
			expected: "s3://bucket/prefix-xyz",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			result, err := getDirForInput(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRecordExecInfoLoop(t *testing.T) {
	tests := []struct {
		execInfo conn.ExecInfo
		log      string
	}{
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					CurDB:   "db1",
					Type:    pnet.ComQuery,
					Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
				},
				CostTime: time.Second,
			},
			log: "{\"sql\":\"select ?\",\"db\":\"db1\",\"cost\":\"1000.000\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					CurDB:   "db1",
					Type:    pnet.ComQuery,
					Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("insert into t values(1)")...),
				},
				CostTime: time.Millisecond,
			},
			log: "{\"sql\":\"insert into `t` values ( ? )\",\"db\":\"db1\",\"cost\":\"1.000\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					CurDB:   "db1",
					Type:    pnet.ComQuery,
					Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("insert into t values(1, 2),(3,4)")...),
				},
				CostTime: 1234567,
			},
			log: "{\"sql\":\"insert into `t` values ( ... )\",\"db\":\"db1\",\"cost\":\"1.235\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					CurDB:   "db1",
					Type:    pnet.ComQuery,
					Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select * from where id in (1, 2)")...),
				},
				CostTime: 1234567,
			},
			log: "{\"sql\":\"select * from where `id` in ( ... )\",\"db\":\"db1\",\"cost\":\"1.235\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					Type:         pnet.ComStmtExecute,
					PreparedStmt: "select ?",
				},
				CostTime: 9999 * time.Microsecond,
			},
			log: "{\"sql\":\"select ?\",\"db\":\"\",\"cost\":\"9.999\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					Type:         pnet.ComStmtExecute,
					PreparedStmt: "select \n\"\"",
				},
				CostTime: 9999 * time.Microsecond,
			},
			log: "{\"sql\":\"select \\n\\\"\\\"\",\"db\":\"\",\"cost\":\"9.999\",\"ex_time\":\"20250906 17:03:50.222\"}\n",
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					Type:    pnet.ComInitDB,
					Payload: []byte{pnet.ComInitDB.Byte()},
				},
				CostTime: time.Millisecond,
			},
		},
		{
			execInfo: conn.ExecInfo{
				Command: &cmd.Command{
					Type:         pnet.ComStmtPrepare,
					PreparedStmt: "select ?",
					Payload:      pnet.MakePrepareStmtRequest("select ?"),
				},
				CostTime: time.Millisecond,
			},
		},
	}

	startTime := time.Date(2025, 9, 6, 17, 3, 50, 222000000, time.UTC)
	for i, test := range tests {
		replay := NewReplay(zap.NewNop(), id.NewIDManager())
		dir := t.TempDir()
		replay.cfg = ReplayConfig{
			OutputPath: filepath.Join(dir, "replay.log"),
		}
		replay.execInfoCh = make(chan conn.ExecInfo, 1)
		replay.wg.Run(replay.recordExecInfoLoop, zap.NewNop())
		test.execInfo.StartTime = startTime
		replay.execInfoCh <- test.execInfo
		close(replay.execInfoCh)
		replay.Close()

		if len(test.log) > 0 {
			var log string
			require.Eventually(t, func() bool {
				data, _ := os.ReadFile(replay.cfg.OutputPath)
				if len(data) == 0 {
					return false
				}
				log = hack.String(data)
				return true
			}, 3*time.Second, 10*time.Millisecond)
			require.Equal(t, test.log, log, "case %d", i)
		} else {
			time.Sleep(100 * time.Millisecond)
			data, _ := os.ReadFile(replay.cfg.OutputPath)
			require.Empty(t, data)
		}
	}
}

func TestDynamicInputCoverAllDirectories(t *testing.T) {
	const maxDirectoriesCount = 50
	const maxCommandsPerFile = 20
	const auditlogFileName = "tidb-audit-2025-12-16T13-42-55.511.log"

	tempDir := t.TempDir()
	url := tempDir + "/tidb-"

	directoriesCount := rand.Uint64N(maxDirectoriesCount) + 1
	for i := range directoriesCount {
		dir := fmt.Sprintf("%s%d", url, i)
		require.NoError(t, os.MkdirAll(dir, 0755))
	}

	expectCommandCount := 0
	for i := range directoriesCount {
		dir := fmt.Sprintf("%s%d", url, i)
		logFile, err := os.OpenFile(filepath.Join(dir, auditlogFileName), os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)

		commandCount := rand.Uint64N(maxCommandsPerFile) + 1
		for range commandCount {
			expectCommandCount += 1
			_, err := logFile.WriteString(`[2025/09/08 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/06 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]`)
			require.NoError(t, err)
			_, err = logFile.WriteString("\n")
			require.NoError(t, err)
		}
	}

	// For static replayer
	staticReplayer := NewReplay(zap.NewNop(), id.NewIDManager())
	defer staticReplayer.Close()

	staticUrl := ""
	for i := range directoriesCount {
		if i > 0 {
			staticUrl += ","
		}
		staticUrl += fmt.Sprintf("%s%d", url, i)
	}

	staticReplayer.cfg = ReplayConfig{
		Input:           staticUrl,
		Username:        "u1",
		StartTime:       time.Now(),
		PSCloseStrategy: cmd.PSCloseStrategyDirected,
		Format:          cmd.FormatAuditLogPlugin,
	}
	storages, err := staticReplayer.cfg.Validate()
	require.NoError(t, err)
	staticReplayer.storages = storages

	readers, err := staticReplayer.constructReaders()
	require.NoError(t, err)
	staticDecoder, err := staticReplayer.constructStaticDecoder(context.Background(), readers)
	require.NoError(t, err)

	actualCommandCount := 0
	for {
		cmd, err := staticDecoder.Decode()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), cmd.Payload)

		actualCommandCount++
	}
	require.Equal(t, expectCommandCount, actualCommandCount)

	// For dynamic input
	replayerCount := rand.Uint64N(maxDirectoriesCount) + 1
	actualCommandCount = 0
	for replayerIndex := range replayerCount {
		dynamicReplayer := NewReplay(zap.NewNop(), id.NewIDManager())

		dynamicReplayer.cfg = ReplayConfig{
			Input:           url,
			Username:        "u1",
			StartTime:       time.Now(),
			PSCloseStrategy: cmd.PSCloseStrategyDirected,
			DynamicInput:    true,
			ReplayerCount:   replayerCount,
			ReplayerIndex:   replayerIndex,
			Format:          cmd.FormatAuditLogPlugin,
		}
		storages, err := dynamicReplayer.cfg.Validate()
		require.NoError(t, err)
		dynamicReplayer.storages = storages

		decoder, err := dynamicReplayer.constructDynamicDecoder(context.Background())
		require.NoError(t, err)

		for {
			cmd, err := decoder.Decode()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			require.Equal(t, append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), cmd.Payload)

			actualCommandCount++
		}

		dynamicReplayer.Close()
	}
	require.Equal(t, expectCommandCount, actualCommandCount)
}
