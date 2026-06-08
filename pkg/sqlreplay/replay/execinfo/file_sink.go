// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"go.uber.org/zap"
)

type fileSink struct {
	lg *zap.Logger
}

func newFileSink(outputPath string) (*fileSink, error) {
	lg, _, _, err := logger.BuildLogger(&config.Log{
		Encoder: "json",
		Simple:  true,
		LogOnline: config.LogOnline{
			LogFile: config.LogFile{
				Filename: outputPath,
				MaxSize:  100,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &fileSink{lg: lg}, nil
}

func (s *fileSink) Write(rec Record) error {
	s.lg.Info("exec info",
		zap.String("sql", rec.SQL),
		zap.String("db", rec.DB),
		zap.Int64("cost", rec.Cost),
		zap.String("ex_time", rec.ExTime))
	return nil
}

func (s *fileSink) Close() error {
	return nil
}
