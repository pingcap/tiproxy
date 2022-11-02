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

package cmd

import (
	"time"

	"github.com/pingcap/TiProxy/lib/config"
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
