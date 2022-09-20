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
	"os"
	"sync"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogMaxSize = 300 // MB
)

var _ closableSyncer = (*rotateLogger)(nil)
var _ closableSyncer = (*stdoutLogger)(nil)
var _ zapcore.WriteSyncer = (*AtomicWriteSyncer)(nil)

// Wrap the syncers as closableSyncer because lumberjack.Logger needs to be closed.
type closableSyncer interface {
	zapcore.WriteSyncer
	Close() error
}

type rotateLogger struct {
	*lumberjack.Logger
}

func (lg *rotateLogger) Sync() error {
	return nil
}

type stdoutLogger struct {
	zapcore.WriteSyncer
}

func (lg *stdoutLogger) Close() error {
	return nil
}

// AtomicWriteSyncer is a WriteSyncer that can be updated online.
type AtomicWriteSyncer struct {
	sync.RWMutex
	output closableSyncer
}

// Rebuild creates a new output and replaces the current one.
func (ws *AtomicWriteSyncer) Rebuild(cfg *config.Log) error {
	var output closableSyncer
	if len(cfg.LogFile.Filename) > 0 {
		fileLogger, err := initFileLog(&cfg.LogFile)
		if err != nil {
			return err
		}
		output = &rotateLogger{fileLogger}
	} else {
		stdLogger, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return err
		}
		output = &stdoutLogger{stdLogger}
	}
	return ws.setOutput(output)
}

// Write implements WriteSyncer.Write().
func (ws *AtomicWriteSyncer) Write(p []byte) (n int, err error) {
	ws.RLock()
	if ws.output != nil {
		n, err = ws.output.Write(p)
	}
	ws.RUnlock()
	return
}

// Sync implements WriteSyncer.Sync().
func (ws *AtomicWriteSyncer) Sync() error {
	var err error
	ws.RLock()
	if ws.output != nil {
		err = ws.output.Sync()
	}
	ws.RUnlock()
	return err
}

func (ws *AtomicWriteSyncer) setOutput(output closableSyncer) error {
	var err error
	ws.Lock()
	if ws.output != nil {
		err = ws.output.Close()
	}
	ws.output = output
	ws.Unlock()
	return err
}

// Close closes logger.
func (ws *AtomicWriteSyncer) Close() error {
	var err error
	ws.Lock()
	if ws.output != nil {
		err = ws.output.Close()
		ws.output = nil
	}
	ws.Unlock()
	return err
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *config.LogFile) (*lumberjack.Logger, error) {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}
