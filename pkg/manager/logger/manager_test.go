// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Test that the configuration change takes affect.
func TestUpdateCfg(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "proxy.log")
	cfg := &config.Config{
		Log: config.Log{
			Encoder: "tidb",
			LogOnline: config.LogOnline{
				Level: "info",
				LogFile: config.LogFile{
					Filename:   fileName,
					MaxSize:    1,
					MaxDays:    2,
					MaxBackups: 1,
				},
			},
		},
	}

	tests := []struct {
		updateCfg func(cfg *config.LogOnline)
		action    func(log *zap.Logger)
		check     func(files []os.FileInfo) bool
	}{
		{
			updateCfg: func(cfg *config.LogOnline) {
				cfg.Level = "error"
				cfg.LogFile.MaxBackups = 2
			},
			action: func(log *zap.Logger) {
				msg := strings.Repeat("a", 600*1024)
				log.Info(msg)
				msg = strings.Repeat("b", 600*1024)
				log.Error(msg)
			},
			check: func(files []os.FileInfo) bool {
				return len(files) == 1
			},
		},
		{
			updateCfg: func(cfg *config.LogOnline) {
				cfg.LogFile.MaxSize = 3
				cfg.LogFile.MaxBackups = 5
			},
			action: func(log *zap.Logger) {
				for i := 0; i < 5; i++ {
					msg := strings.Repeat("a", 500*1024)
					log.Info(msg)
				}
			},
			check: func(files []os.FileInfo) bool {
				if len(files) != 1 {
					return false
				}
				return files[0].Size() >= int64(5*500*1024)
			},
		},
		{
			updateCfg: func(cfg *config.LogOnline) {
				cfg.LogFile.MaxBackups = 2
			},
			action: func(log *zap.Logger) {
				for i := 0; i < 15; i++ {
					msg := strings.Repeat("a", 300*1024)
					log.Info(msg)
				}
			},
			check: func(files []os.FileInfo) bool {
				return len(files) == 3
			},
		},
		{
			updateCfg: func(cfg *config.LogOnline) {
				cfg.LogFile.Filename = ""
			},
			action: func(log *zap.Logger) {
				log.Info("a")
			},
			check: func(files []os.FileInfo) bool {
				return len(files) == 0
			},
		},
	}

	lg, ch := setupLogManager(t, cfg)
	// Make sure the latest config also applies to cloned loggers.
	lg = lg.Named("another").With(zap.String("field", "test_field"))
	for i, test := range tests {
		require.NoError(t, lg.Sync())

		clonedCfg := cfg.Clone()
		test.updateCfg(&clonedCfg.Log.LogOnline)
		ch <- clonedCfg

		// 2rd will block due to watch channel of size 1
		// this ensured all old data are flushed by closing the older file logger
		ch <- clonedCfg

		// delete old data after flushed
		err := os.RemoveAll(dir)
		require.NoError(t, err)

		// write new data
		test.action(lg)

		// retry before new data are flushed
		timer := time.NewTimer(3 * time.Second)
		succeed := false
		for !succeed {
			select {
			case <-timer.C:
				bstr := new(strings.Builder)
				logfiles := readLogFiles(t, dir)
				e := int64(0)
				for _, f := range logfiles {
					fmt.Fprintf(bstr, "%s: %d\n", f.Name(), f.Size())
					e += f.Size()
				}
				fmt.Fprintf(bstr, "3#### %d\n", e)
				t.Fatalf("%dth case time out:\n%s", i, bstr.String())
			case <-time.After(50 * time.Millisecond):
				logfiles := readLogFiles(t, dir)
				if test.check(logfiles) {
					succeed = true
					break
				}
			}
		}
		timer.Stop()
	}
}

func setupLogManager(t *testing.T, cfg *config.Config) (*zap.Logger, chan<- *config.Config) {
	lm, lg, err := NewLoggerManager(&cfg.Log)
	require.NoError(t, err)
	ch := make(chan *config.Config)
	lm.Init(ch)
	t.Cleanup(func() {
		require.NoError(t, lm.Close())
	})
	return lg, ch
}

func readLogFiles(t *testing.T, dir string) []os.FileInfo {
	entries, err := os.ReadDir(dir)
	// The directory may not exist when the output is stdout.
	if err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
		return nil
	}
	files := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if info, err := entry.Info(); err == nil {
			files = append(files, info)
		} else {
			require.ErrorIs(t, err, os.ErrNotExist)
		}
	}
	return files
}

// Test that the manager won't panic or hang when loggers log concurrently.
func TestLogConcurrently(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "proxy.log")
	cfg := &config.Config{
		Log: config.Log{
			Encoder: "tidb",
			LogOnline: config.LogOnline{
				Level: "info",
				LogFile: config.LogFile{
					Filename:   fileName,
					MaxSize:    1,
					MaxDays:    2,
					MaxBackups: 3,
				},
			},
		},
	}

	lg, ch := setupLogManager(t, cfg)
	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	for i := 0; i < 5; i++ {
		wg.Run(func() {
			for ctx.Err() == nil {
				lg = lg.Named("test_name")
				lg.Info("test_info")
				lg.Warn("test_warn")
				lg.Error("test_error")
				lg = lg.With(zap.String("with", "test_with"))
				lg.Info("test_info")
				lg.Warn("test_warn")
				lg.Error("test_error")
			}
		})
	}
	wg.Run(func() {
		newCfg := cfg.Clone()
		for ctx.Err() == nil {
			newCfg.Log.LogFile.MaxDays = int(rand.Int31n(10))
			ch <- newCfg
			time.Sleep(10 * time.Millisecond)
			newCfg.Log.LogFile.MaxBackups = int(rand.Int31n(10))
			ch <- newCfg
			time.Sleep(10 * time.Millisecond)
			newCfg.Log.LogFile.MaxSize = int(rand.Int31n(10))
			ch <- newCfg
			time.Sleep(10 * time.Millisecond)
		}
	})
	wg.Wait()
	cancel()
}
