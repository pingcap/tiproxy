// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type testingLog struct {
	*testing.T
}

func (t *testingLog) Write(b []byte) (int, error) {
	t.Logf("%s", b)
	return len(b), nil
}

// CreateLoggerForTest creates a logger for unit tests.
func CreateLoggerForTest(t *testing.T) *zap.Logger {
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(&testingLog{t}),
		zap.InfoLevel,
	)).Named(t.Name())
}
