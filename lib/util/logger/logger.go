// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"bytes"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type testingLog struct {
	*testing.T
	buf bytes.Buffer
}

func (t *testingLog) Write(b []byte) (int, error) {
	t.Logf("%s", b)
	return t.buf.Write(b)
}

func (t *testingLog) String() string {
	return t.buf.String()
}

// CreateLoggerForTest returns both the logger and its content.
func CreateLoggerForTest(t *testing.T) (*zap.Logger, fmt.Stringer) {
	log := &testingLog{T: t}
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(log),
		zap.InfoLevel,
	)).Named(t.Name()), log
}
