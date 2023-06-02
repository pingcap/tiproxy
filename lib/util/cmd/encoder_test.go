// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
