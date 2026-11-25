// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSQL(t *testing.T) {
	const sql = "select * from t where name = 'alice' and id = 10"
	require.Equal(t, sql, NormalizeSQL(sql, config.RedactInfoLogOff))
	require.Equal(t, "select * from `t` where `name` = ? and `id` = ?", NormalizeSQL(sql, config.RedactInfoLogOn))
	require.Equal(t, "select * from `t` where `name` = ? and `id` = ?", NormalizeSQL(sql, config.RedactInfoLogType(99)))
	require.Equal(t, "select * from `t` where `name` = ‹alice› and `id` = ‹10›", NormalizeSQL(sql, config.RedactInfoLogMarker))
}

func TestParamsText(t *testing.T) {
	params := []any{int64(7), "alice", nil}
	require.Equal(t, "[7 alice <nil>]", ParamsText(params, config.RedactInfoLogOff))
	require.Equal(t, "[? ? ?]", ParamsText(params, config.RedactInfoLogOn))
	require.Equal(t, "[‹7› ‹alice› ‹<nil>›]", ParamsText(params, config.RedactInfoLogMarker))
	require.Equal(t, "[‹a‹‹b››c›]", ParamsText([]any{"a‹b›c"}, config.RedactInfoLogMarker))
	require.Equal(t, "[]", ParamsText(nil, config.RedactInfoLogOn))
}

func TestErrorMessage(t *testing.T) {
	tests := []struct {
		name   string
		msg    string
		mode   config.RedactInfoLogType
		output string
	}{
		{
			name:   "off",
			msg:    "ERROR 1062 (23000): Duplicate entry 'alice@example.com' for key 'users.email'",
			mode:   config.RedactInfoLogOff,
			output: "ERROR 1062 (23000): Duplicate entry 'alice@example.com' for key 'users.email'",
		},
		{
			name:   "mysql error on",
			msg:    "ERROR 1062 (23000): Duplicate entry 'alice@example.com' for key 'users.email'",
			mode:   config.RedactInfoLogOn,
			output: "ERROR 1062 (23000): ?",
		},
		{
			name:   "mysql error marker",
			msg:    "ERROR 1062 (23000): Duplicate entry 'alice@example.com' for key 'users.email'",
			mode:   config.RedactInfoLogMarker,
			output: "ERROR 1062 (23000): ‹Duplicate entry 'alice@example.com' for key 'users.email'›",
		},
		{
			name:   "unquoted sensitive value",
			msg:    "Table has no partition for value customer-secret",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "unquoted sensitive value marker",
			msg:    "Table has no partition for value customer-secret",
			mode:   config.RedactInfoLogMarker,
			output: "‹Table has no partition for value customer-secret›",
		},
		{
			name:   "unclosed quote",
			msg:    "syntax error near 'customer-secret\\' at line 1",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "apostrophe between word characters",
			msg:    "value x'customer-secret'",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "network address",
			msg:    "dial tcp 10.0.0.1:4000: connect: connection refused",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "invalid mysql state",
			msg:    "ERROR 1062 (secret): customer-secret",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "invalid mysql code",
			msg:    "ERROR customer-secret (23000): detail",
			mode:   config.RedactInfoLogOn,
			output: "?",
		},
		{
			name:   "marker escaping",
			msg:    "value ‹customer-secret›",
			mode:   config.RedactInfoLogMarker,
			output: "‹value ‹‹customer-secret›››",
		},
		{
			name:   "invalid mode is on",
			msg:    "customer-secret",
			mode:   config.RedactInfoLogType(99),
			output: "?",
		},
		{
			name:   "empty",
			mode:   config.RedactInfoLogOn,
			output: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.output, ErrorMessage(test.msg, test.mode))
		})
	}
}
