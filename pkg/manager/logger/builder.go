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
	"sync"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/cmd"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var registerEncoders sync.Once

func buildEncoder(cfg *config.Log) (zapcore.Encoder, error) {
	zapcfg := zap.NewDevelopmentConfig()
	zapcfg.Encoding = cfg.Encoder
	zapcfg.DisableStacktrace = true
	encoder, err := log.NewTextEncoder(&log.Config{})
	if err != nil {
		return nil, err
	}
	registerEncoders.Do(func() {
		zap.RegisterEncoder("tidb", func(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
			return encoder, nil
		})
		zap.RegisterEncoder("newtidb", func(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
			return cmd.NewTiDBEncoder(cfg), nil
		})
	})
	return encoder, nil
}

func buildLevel(cfg *config.Log) (zap.AtomicLevel, error) {
	return zap.ParseAtomicLevel(cfg.Level)
}

func buildSyncer(cfg *config.Log) (*AtomicWriteSyncer, error) {
	syncer := &AtomicWriteSyncer{}
	if err := syncer.Rebuild(cfg); err != nil {
		return nil, err
	}
	return syncer, nil
}
