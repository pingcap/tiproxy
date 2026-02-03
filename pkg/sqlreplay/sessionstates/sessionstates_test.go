// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sessionstates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func escapeSessionStatesJSON(raw string) string {
	escaped := strings.ReplaceAll(raw, "\\", "\\\\")
	return strings.ReplaceAll(escaped, "'", "\\'")
}

func TestParseFromSetSessionStatesSQL(t *testing.T) {
	ss, err := ParseFromSetSessionStatesSQL("select 1")
	require.NoError(t, err)
	require.Empty(t, ss.PreparedStmts)

	jsonStr := `{"prepared-stmts":{"7":{"text":"select ?","types":"AAE="}}}`
	sql := "SET SESSION_STATES '" + jsonStr + "'"
	ss, err = ParseFromSetSessionStatesSQL(sql)
	require.NoError(t, err)
	require.Equal(t, "select ?", ss.PreparedStmts[7].StmtText)

	escapedJSON := `{"prepared-stmts":{"9":{"text":"select \\ 'x'"}}}`
	escapedSQL := "SET SESSION_STATES '" + escapeSessionStatesJSON(escapedJSON) + "'"
	ss, err = ParseFromSetSessionStatesSQL(escapedSQL)
	require.NoError(t, err)
	require.Equal(t, "select \\ 'x'", ss.PreparedStmts[9].StmtText)

	_, err = ParseFromSetSessionStatesSQL("SET SESSION_STATES 'invalid-json'")
	require.Error(t, err)
}

func TestExtractPreparedStmtTextsFromSetSessionStatesSQL(t *testing.T) {
	jsonStr := `{"prepared-stmts":{"7":{"text":"select ?","types":"AAE="},"8":{"text":""}}}`
	sql := "SET SESSION_STATES '" + jsonStr + "'"
	require.Equal(t, map[uint32]string{7: "select ?"}, ExtractPreparedStmtTextsFromSetSessionStatesSQL(sql))

	require.Nil(t, ExtractPreparedStmtTextsFromSetSessionStatesSQL("select 1"))
	require.Nil(t, ExtractPreparedStmtTextsFromSetSessionStatesSQL("SET SESSION_STATES 'invalid'"))
}
