// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextToken(t *testing.T) {
	tests := []struct {
		sql    string
		tokens []string
	}{
		{
			sql:    `SELECT * FROM table_name`,
			tokens: []string{"SELECT", "FROM", "TABLE_NAME"},
		},
		{
			sql: `-- comment
			/* comment * / "
			*/ SELECT
	  		-- comment
			(*)
			FROM table_name`,
			tokens: []string{"SELECT", "FROM", "TABLE_NAME"},
		},
		{
			sql: ` SELECT
			"string /* */
			" * 'string'
			FROM '"' "'" '\\' '\'' (table_name)`,
			tokens: []string{"SELECT", "FROM", "TABLE_NAME"},
		},
		{
			sql:    ` select 123.4e-5 / (1 - 0.9) + @@hello_world 中文`,
			tokens: []string{"SELECT", "E", "HELLO_WORLD"},
		},
		{
			sql:    `sEleCt ** from; t5ble_name`,
			tokens: []string{"SELECT", "FROM", "T", "BLE_NAME"},
		},
		{
			sql:    `set @@session.autocommit = 0`,
			tokens: []string{"SET", "SESSION", "AUTOCOMMIT"},
		},
		{
			sql:    `select "for update"`,
			tokens: []string{"SELECT"},
		},
	}

	for i, test := range tests {
		l := NewLexer(test.sql)
		tokens := make([]string, 0, len(test.tokens))
		for {
			token := l.NextToken()
			if len(token) == 0 {
				break
			}
			tokens = append(tokens, token)
		}
		require.Equal(t, test.tokens, tokens, "case %d", i)
	}
}
