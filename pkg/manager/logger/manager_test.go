// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
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
				// Filter the logs in LoggerManager.watchCfg().
				cfg.Level = "warn"
				cfg.LogFile.MaxSize = 3
				cfg.LogFile.MaxBackups = 5
			},
			action: func(log *zap.Logger) {
				for range 5 {
					msg := strings.Repeat("a", 500*1024)
					log.Warn(msg)
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
				for range 15 {
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

	for i, test := range tests {
		require.NoError(t, os.RemoveAll(dir))

		lm, lg, err := NewLoggerManager(&cfg.Log)
		require.NoError(t, err)
		ch := make(chan *config.Config)
		lm.Init(ch)
		// Make sure the latest config also applies to cloned loggers.
		lg = lg.Named("another").With(zap.String("field", "test_field"))
		require.NoError(t, lg.Sync())

		clonedCfg := cfg.Clone()
		test.updateCfg(&clonedCfg.Log.LogOnline)
		ch <- clonedCfg

		// 2rd will block due to watch channel of size 1
		// this ensured all old data are flushed by closing the older file logger
		ch <- clonedCfg

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
		require.NoError(t, lm.Close())
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

	newCfg := func() *config.Config {
		return &config.Config{
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
	}

	lg, ch := setupLogManager(t, newCfg())
	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	for range 5 {
		wg.Run(func() {
			for ctx.Err() == nil {
				namedLg := lg.Named("test_name")
				namedLg.Info("test_info")
				namedLg.Warn("test_warn")
				namedLg.Error("test_error")
				withLg := namedLg.With(zap.String("with", "test_with"))
				withLg.Info("test_info")
				withLg.Warn("test_warn")
				withLg.Error("test_error")
			}
		})
	}
	wg.Run(func() {
		for ctx.Err() == nil {
			cfg := newCfg()
			cfg.Log.LogFile.MaxDays = int(rand.Int31n(10))
			ch <- cfg
			time.Sleep(10 * time.Millisecond)
			cfg = newCfg()
			cfg.Log.LogFile.MaxBackups = int(rand.Int31n(10))
			ch <- cfg
			time.Sleep(10 * time.Millisecond)
			cfg = newCfg()
			cfg.Log.LogFile.MaxSize = int(rand.Int31n(10))
			ch <- cfg
			time.Sleep(10 * time.Millisecond)
		}
	})
	wg.Wait()
	cancel()
}
