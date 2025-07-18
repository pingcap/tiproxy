// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutput(t *testing.T) {
	str, err := ConfigInfo("json")
	require.NoError(t, err)
	require.NotEmpty(t, str)
	require.NoError(t, json.Unmarshal([]byte(str), &Config{}))
}
