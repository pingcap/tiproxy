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
	"encoding/json"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerManager updates log configurations online.
type LoggerManager struct {
	// The logger used by LoggerManager itself to log.
	logger  *zap.Logger
	encoder zapcore.Encoder
	syncer  *AtomicWriteSyncer
	level   zap.AtomicLevel
	cancel  context.CancelFunc
	wg      waitgroup.WaitGroup
}

// NewLoggerManager creates a new LoggerManager.
func NewLoggerManager(cfg *config.Log) (*LoggerManager, error) {
	lm := &LoggerManager{}
	var err error
	if lm.encoder, err = buildEncoder(cfg); err != nil {
		return nil, err
	}
	if lm.syncer, err = buildSyncer(cfg); err != nil {
		return nil, err
	}
	if lm.level, err = buildLevel(cfg); err != nil {
		return nil, err
	}
	return lm, nil
}

// BuildLogger returns a new logger with the same syncer.
func (lm *LoggerManager) BuildLogger() *zap.Logger {
	return zap.New(zapcore.NewCore(lm.encoder, lm.syncer, lm.level),
		zap.ErrorOutput(lm.syncer),
		zap.AddStacktrace(zapcore.FatalLevel))
}

// Init starts a goroutine to watch configuration.
func (lm *LoggerManager) Init(logger *zap.Logger, cfgCh chan *config.Log) {
	lm.logger = logger
	ctx, cancel := context.WithCancel(context.Background())
	lm.wg.Run(func() {
		lm.watchCfg(ctx, cfgCh)
	})
	lm.cancel = cancel
}

func (lm *LoggerManager) watchCfg(ctx context.Context, cfgCh chan *config.Log) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg := <-cfgCh:
			err := lm.updateLoggerCfg(cfg)
			if err != nil {
				bytes, merr := json.Marshal(cfg)
				if merr != nil {
					lm.logger.Error("update logger configuration failed", zap.NamedError("marshal_err", merr), zap.Error(err))
					continue
				}
				lm.logger.Error("update logger configuration failed", zap.String("cfg", string(bytes)), zap.Error(err))
			}
		}
	}
}

func (lm *LoggerManager) updateLoggerCfg(cfg *config.Log) error {
	// encoder cannot be configured dynamically, because Core.With always clones the encoder.
	if err := lm.syncer.Rebuild(cfg); err != nil {
		return err
	}
	if level, err := zapcore.ParseLevel(cfg.Level); err != nil {
		return err
	} else {
		lm.level.SetLevel(level)
	}
	return nil
}

// Close releases all resources.
func (lm *LoggerManager) Close() error {
	if lm.cancel != nil {
		lm.cancel()
	}
	lm.wg.Wait()
	return lm.syncer.Close()
}
