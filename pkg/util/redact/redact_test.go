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
	msg := "ERROR 1062 (23000): Duplicate entry 'alice@example.com' for key 'users.email'"
	require.Equal(t, msg, ErrorMessage(msg, config.RedactInfoLogOff))
	require.Equal(t, "ERROR 1062 (23000): Duplicate entry '?' for key '?'", ErrorMessage(msg, config.RedactInfoLogOn))
	require.Equal(t, "ERROR 1062 (23000): Duplicate entry '?' for key '?'", ErrorMessage(msg, config.RedactInfoLogType(99)))
	require.Equal(t, "ERROR 1062 (23000): Duplicate entry '‹alice@example.com›' for key '‹users.email›'", ErrorMessage(msg, config.RedactInfoLogMarker))

	plain := "Table 'test.users' doesn't exist"
	require.Equal(t, "Table '?' doesn't exist", ErrorMessage(plain, config.RedactInfoLogOn))
	require.Equal(t, "Table '‹test.users›' doesn't exist", ErrorMessage(plain, config.RedactInfoLogMarker))

	require.Equal(t, "dial tcp 10.0.0.1:4000: connect: connection refused", ErrorMessage("dial tcp 10.0.0.1:4000: connect: connection refused", config.RedactInfoLogOn))
	require.Equal(t, `value "?"`, ErrorMessage(`value "secret"`, config.RedactInfoLogOn))
	require.Equal(t, "value '?'", ErrorMessage(`value 'a''b'`, config.RedactInfoLogOn))
	require.Equal(t, "value '?'", ErrorMessage(`value 'a\'b'`, config.RedactInfoLogOn))
	require.Equal(t, `value '‹a\'b›'`, ErrorMessage(`value 'a\'b'`, config.RedactInfoLogMarker))
	require.Equal(t, "syntax error near '?' at line 1", ErrorMessage("syntax error near 'SELEC 'customer-secret'' at line 1", config.RedactInfoLogOn))
	require.NotContains(t, ErrorMessage("syntax error near 'SELEC 'customer-secret'' at line 1", config.RedactInfoLogOn), "customer-secret")
	nestedEscapedQuote := "syntax error near 'SELEC 'a'' customer-secret'' at line 1"
	require.Equal(t, "syntax error near '?' at line 1", ErrorMessage(nestedEscapedQuote, config.RedactInfoLogOn))
	require.NotContains(t, ErrorMessage(nestedEscapedQuote, config.RedactInfoLogOn), "customer-secret")
	require.Equal(t, "syntax error near '‹SELEC 'a'' customer-secret'›' at line 1", ErrorMessage(nestedEscapedQuote, config.RedactInfoLogMarker))
	require.Equal(t, "Can't DROP '?'; check that column/key exists", ErrorMessage("Can't DROP 'private_column'; check that column/key exists", config.RedactInfoLogOn))
	require.Equal(t, "Can't DROP '‹private_column›'; check that column/key exists", ErrorMessage("Can't DROP 'private_column'; check that column/key exists", config.RedactInfoLogMarker))
	require.Equal(t, "it is alice's", ErrorMessage("it is alice's", config.RedactInfoLogOn))
	require.Equal(t, "unknown", ErrorMessage("unknown", config.RedactInfoLogType(99)))
	require.Equal(t, "", ErrorMessage("", config.RedactInfoLogOn))
}
