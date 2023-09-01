// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func buildEncoder(cfg *config.Log) (zapcore.Encoder, error) {
	encfg := zap.NewProductionEncoderConfig()
	encfg.EncodeTime = func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
		pae.AppendString(t.Format("2006/01/02 15:04:05.000 -07:00"))
	}
	encfg.EncodeLevel = func(l zapcore.Level, pae zapcore.PrimitiveArrayEncoder) {
		pae.AppendString(l.CapitalString())
	}
	switch cfg.Encoder {
	case "json":
		return zapcore.NewJSONEncoder(encfg), nil
	case "console":
		return zapcore.NewConsoleEncoder(encfg), nil
	default:
		return NewTiDBEncoder(encfg), nil
	}
}

func buildLevel(cfg *config.Log) (zap.AtomicLevel, error) {
	return zap.ParseAtomicLevel(cfg.Level)
}

func buildSyncer(cfg *config.Log) (*AtomicWriteSyncer, error) {
	syncer := &AtomicWriteSyncer{}
	if err := syncer.Rebuild(&cfg.LogOnline); err != nil {
		return nil, err
	}
	return syncer, nil
}

func BuildLogger(cfg *config.Log) (*zap.Logger, *AtomicWriteSyncer, zap.AtomicLevel, error) {
	level, err := buildLevel(cfg)
	if err != nil {
		return nil, nil, level, err
	}
	encoder, err := buildEncoder(cfg)
	if err != nil {
		return nil, nil, level, err
	}
	syncer, err := buildSyncer(cfg)
	if err != nil {
		return nil, nil, level, err
	}
	return zap.New(zapcore.NewCore(encoder, syncer, level), zap.ErrorOutput(syncer), zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller()), syncer, level, nil
}
