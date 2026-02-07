// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyComQuerySQLType(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{query: "select 1", expected: sqlTypeSelect},
		{query: "UPDATE t SET a=1", expected: sqlTypeUpdate},
		{query: "insert into t values (1)", expected: sqlTypeInsert},
		{query: "delete from t", expected: sqlTypeDelete},
		{query: "replace into t values (1)", expected: sqlTypeReplace},
		{query: "begin", expected: sqlTypeBegin},
		{query: "START TRANSACTION", expected: sqlTypeBegin},
		{query: "commit", expected: sqlTypeCommit},
		{query: "rollback", expected: sqlTypeRollback},
		{query: "set autocommit=0", expected: sqlTypeSet},
		{query: "use test", expected: sqlTypeUse},
		{query: "  # c1\n -- c2\n /* c3 */ select * from t", expected: sqlTypeSelect},
		{query: "", expected: sqlTypeOther},
		{query: ";  ", expected: sqlTypeOther},
		{query: "show tables", expected: sqlTypeOther},
		{query: "/* unterminated", expected: sqlTypeOther},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, classifyComQuerySQLType([]byte(test.query)), test.query)
	}
}
