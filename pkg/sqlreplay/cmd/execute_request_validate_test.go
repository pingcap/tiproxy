// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCountSQLPlaceholders(t *testing.T) {
	tests := []struct {
		sql      string
		expected int
	}{
		{sql: "SELECT * FROM t WHERE id = ?", expected: 1},
		{sql: "/*tag*/ SELECT * FROM t WHERE id = ? AND name = ?", expected: 2},
		{sql: "SELECT * FROM t WHERE col = '?' AND id = ?", expected: 1},
		{sql: "SELECT * FROM t LIMIT ?, ?", expected: 2},
	}
	for _, tt := range tests {
		require.Equal(t, tt.expected, countSQLPlaceholders(tt.sql), "sql: %s", tt.sql)
	}
}

func TestValidateExecuteRequest(t *testing.T) {
	sql := "SELECT * FROM t WHERE id = ? AND site = ? LIMIT ?, ?"
	args := []any{"1", "site", int64(0), int64(20)}
	executeReq, err := pnet.MakeExecuteStmtRequest(1, args, true)
	require.NoError(t, err)
	require.NoError(t, validateExecuteRequest(sql, args, executeReq))

	mismatchArgs := []any{"1", "site", int64(0)}
	mismatchReq, err := pnet.MakeExecuteStmtRequest(1, mismatchArgs, true)
	require.NoError(t, err)
	err = validateExecuteRequest(sql, mismatchArgs, mismatchReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), "param count mismatch")
}

func TestValidateExecuteRequestRecoversPanic(t *testing.T) {
	sql := "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?"
	args := []any{"1", "2"}
	executeReq, err := pnet.MakeExecuteStmtRequest(1, args, true)
	require.NoError(t, err)

	// count mismatch is caught before parse; use matching count but truncated packet to force parse panic path.
	args = []any{"1", "2", "3"}
	executeReq, err = pnet.MakeExecuteStmtRequest(1, args, true)
	require.NoError(t, err)
	executeReq = executeReq[:len(executeReq)-5]
	err = validateExecuteRequest(sql, args, executeReq)
	require.Error(t, err)
}

func TestLogAndSkipInvalidExecuteRequest(t *testing.T) {
	core, observed := observer.New(zap.ErrorLevel)
	lg := zap.New(core)
	logAndSkipInvalidExecuteRequest(lg, "audit-line", "select ?", []any{"1"}, errors.Errorf("test error"))
	entries := observed.All()
	require.Len(t, entries, 1)
	require.Equal(t, "invalid COM_STMT_EXECUTE request, skipping audit log line", entries[0].Message)
	require.Equal(t, "audit-line", entries[0].ContextMap()["audit_line"])
}
