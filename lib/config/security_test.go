// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestRedactInfoLogDefault(t *testing.T) {
	require.Equal(t, RedactInfoLogOn, NewConfig().Security.RedactInfoLog)
}

func TestRedactInfoLogTOML(t *testing.T) {
	testcases := []struct {
		name      string
		value     string
		expected  RedactInfoLogType
		canonical string
	}{
		{name: "false", value: "false", expected: RedactInfoLogOff, canonical: "false"},
		{name: "true", value: "true", expected: RedactInfoLogOn, canonical: "true"},
		{name: "string false", value: `"false"`, expected: RedactInfoLogOff, canonical: "false"},
		{name: "uppercase string false", value: `"FALSE"`, expected: RedactInfoLogOff, canonical: "false"},
		{name: "mixed-case string false", value: `"False"`, expected: RedactInfoLogOff, canonical: "false"},
		{name: "string true", value: `"true"`, expected: RedactInfoLogOn, canonical: "true"},
		{name: "uppercase string true", value: `"TRUE"`, expected: RedactInfoLogOn, canonical: "true"},
		{name: "mixed-case string true", value: `"True"`, expected: RedactInfoLogOn, canonical: "true"},
		{name: "lowercase marker", value: `"marker"`, expected: RedactInfoLogMarker, canonical: `"MARKER"`},
		{name: "uppercase marker", value: `"MARKER"`, expected: RedactInfoLogMarker, canonical: `"MARKER"`},
		{name: "mixed-case marker", value: `"Marker"`, expected: RedactInfoLogMarker, canonical: `"MARKER"`},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			cfg := NewConfig()
			require.NoError(t, toml.Unmarshal([]byte("security.redact-info-log = "+testcase.value), cfg))
			require.Equal(t, testcase.expected, cfg.Security.RedactInfoLog)

			data, err := cfg.ToBytes()
			require.NoError(t, err)
			require.Contains(t, string(data), "redact-info-log = "+testcase.canonical)

			decoded := NewConfig()
			require.NoError(t, toml.Unmarshal(data, decoded))
			require.Equal(t, testcase.expected, decoded.Security.RedactInfoLog)
		})
	}
}

func TestInvalidRedactInfoLogTOML(t *testing.T) {
	for _, value := range []string{`"on"`, `"off"`, `""`, "1", "1.0"} {
		t.Run(value, func(t *testing.T) {
			cfg := NewConfig()
			err := toml.Unmarshal([]byte("security.redact-info-log = "+value), cfg)
			require.ErrorContains(t, err, invalidRedactInfoLogTypeErrMsg)
			require.Equal(t, RedactInfoLogOn, cfg.Security.RedactInfoLog)
		})
	}
}

func TestRedactInfoLogJSON(t *testing.T) {
	testcases := []struct {
		value     string
		expected  RedactInfoLogType
		canonical string
	}{
		{value: "false", expected: RedactInfoLogOff, canonical: "false"},
		{value: "true", expected: RedactInfoLogOn, canonical: "true"},
		{value: `"false"`, expected: RedactInfoLogOff, canonical: "false"},
		{value: `"FALSE"`, expected: RedactInfoLogOff, canonical: "false"},
		{value: `"False"`, expected: RedactInfoLogOff, canonical: "false"},
		{value: `"true"`, expected: RedactInfoLogOn, canonical: "true"},
		{value: `"TRUE"`, expected: RedactInfoLogOn, canonical: "true"},
		{value: `"True"`, expected: RedactInfoLogOn, canonical: "true"},
		{value: `"marker"`, expected: RedactInfoLogMarker, canonical: `"MARKER"`},
		{value: `"Marker"`, expected: RedactInfoLogMarker, canonical: `"MARKER"`},
	}

	for _, testcase := range testcases {
		t.Run(testcase.value, func(t *testing.T) {
			value := RedactInfoLogOn
			require.NoError(t, json.Unmarshal([]byte(testcase.value), &value))
			require.Equal(t, testcase.expected, value)

			data, err := json.Marshal(value)
			require.NoError(t, err)
			require.JSONEq(t, testcase.canonical, string(data))
		})
	}
}

func TestInvalidRedactInfoLogJSON(t *testing.T) {
	for _, data := range []string{`"on"`, `"off"`, `""`, "1", "null", `{}`} {
		t.Run(data, func(t *testing.T) {
			value := RedactInfoLogOn
			err := json.Unmarshal([]byte(data), &value)
			require.EqualError(t, err, invalidRedactInfoLogTypeErrMsg)
			require.Equal(t, RedactInfoLogOn, value)
		})
	}
}
