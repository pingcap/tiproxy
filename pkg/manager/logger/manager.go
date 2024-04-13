// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/cmd"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerManager updates log configurations online.
type LoggerManager struct {
	// The logger used by LoggerManager itself to log.
	logger *zap.Logger
	syncer *cmd.AtomicWriteSyncer
	level  zap.AtomicLevel
	cancel context.CancelFunc
	wg     waitgroup.WaitGroup
}

// NewLoggerManager creates a new LoggerManager.
func NewLoggerManager(cfg *config.Log) (*LoggerManager, *zap.Logger, error) {
	lm := &LoggerManager{}
	var err error
	if cfg == nil {
		cfg = &config.NewConfig().Log
	}
	mainLogger, syncer, level, err := cmd.BuildLogger(cfg)
	if err != nil {
		return nil, nil, err
	}
	lm.syncer = syncer
	lm.level = level
	mainLogger = mainLogger.Named("main")
	lm.logger = mainLogger.Named("lgmgr")
	return lm, mainLogger, nil
}

// Init starts a goroutine to watch configuration.
func (lm *LoggerManager) Init(cfgch <-chan *config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	lm.cancel = cancel

	lm.wg.RunWithRecover(func() {
		lm.watchCfg(ctx, cfgch)
	}, nil, lm.logger)
}

func (lm *LoggerManager) SetLoggerLevel(l zapcore.Level) {
	lm.level.SetLevel(l)
}

func (lm *LoggerManager) watchCfg(ctx context.Context, cfgch <-chan *config.Config) {
	for {
		select {
		case <-ctx.Done():
			return
		case acfg := <-cfgch:
			if acfg == nil {
				// prevent panic on closing chan
				return
			}

			cfg := &acfg.Log.LogOnline
			err := lm.updateLoggerCfg(cfg)
			if err != nil {
				bytes, merr := json.Marshal(cfg)
				lm.logger.Error("update logger configuration failed",
					zap.NamedError("update error", err),
					zap.String("cfg", string(bytes)),
					zap.NamedError("cfg marshal error", merr),
				)
			}
		}
	}
}

func (lm *LoggerManager) updateLoggerCfg(cfg *config.LogOnline) error {
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
