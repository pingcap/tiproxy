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

	var configOutput output
	require.NoError(t, json.Unmarshal([]byte(str), &configOutput))
	for _, parameter := range configOutput.Parameters {
		if parameter.Name == "security.redact-info-log" {
			require.Equal(t, "string", parameter.Type)
			require.Equal(t, "true", parameter.DefaultValue)
			require.True(t, parameter.HotReloadable)
			return
		}
	}
	require.Fail(t, "security.redact-info-log is missing")
}
