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
	enc.AddReflected("ff", struct{ A string }{"\""})
	require.Equal(t, "[ff=\"{\\\"A\\\":\\\"\\\\\"\\\"}\"]", enc.line.String())
}
