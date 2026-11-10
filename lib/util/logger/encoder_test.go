// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestEncoder(t *testing.T) {
	enc := NewTiDBEncoder(zapcore.EncoderConfig{})

	// test escape
	enc.AddString("ff", "\n\r\"")
	require.Equal(t, "[ff=\"\\n\\r\\\"\"]", enc.line.String())

	// test quotes
	for _, ch := range []rune{'\\', '"', '[', ']', '='} {
		enc = enc.clone(false)
		enc.AddString("ff", fmt.Sprintf("ff%c", ch))
		escape := ""
		if ch == '"' {
			escape = "\\"
		}
		require.Equal(t, fmt.Sprintf("[ff=\"ff%s%c\"]", escape, ch), enc.line.String())
	}

	// test append bytes
	enc = enc.clone(false)
	enc.AddByteString("ff", []byte{0x33, 0x22})
	require.Equal(t, "[ff=0x3322]", enc.line.String())

	// test append reflected
	enc = enc.clone(false)
	require.NoError(t, enc.AddReflected("ff", struct{ A string }{"\""}))
	require.Equal(t, "[ff=\"{\\\"A\\\":\\\"\\\\\"\\\"}\"]", enc.line.String())
}

func TestRedactedConfigIsOneField(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Metering.Endpoint = "https://bucket.s3.amazonaws.com/obj?X-Amz-Signature=sigsecret&X-Amz-Security-Token=tokensecret"
	enc := NewTiDBEncoder(zap.NewProductionEncoderConfig())
	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "current config", Time: time.Now()}, []zapcore.Field{
		zap.Any("cfg", cfg),
	})
	require.NoError(t, err)
	defer buf.Free()
	line := buf.String()
	require.Contains(t, line, `[cfg="`)
	require.NotContains(t, line, "[cfg={")
	require.NotContains(t, line, "sigsecret")
	require.NotContains(t, line, "tokensecret")
}
