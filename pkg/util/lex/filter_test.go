// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSenstiveSQL(t *testing.T) {
	tests := []struct {
		sql       string
		sensitive bool
	}{
		{`SELECT * FROM table_name`, false},
		{`grant ALL PRIVILEGES ON database_name.* TO 'username'@'localhost' IDENTIFIED BY 'password'`, true},
		{`CREATE USER 'new_user'@'localhost' IDENTIFIED BY 'secure_password';`, true},
		{` ALTER USER 'existing_user'@'localhost' IDENTIFIED BY 'new_password'`, true},
		{`/*hello */set PASSWORD FOR 'username'@'localhost' = PASSWORD('new_password');`, true},
		{`set global anything = 'hello' `, false},
		{``, false},
		{`set`, false},
	}

	for _, test := range tests {
		require.Equal(t, test.sensitive, IsSensitiveSQL(test.sql), test.sql)
	}
}

func TestReadOnlySQL(t *testing.T) {
	tests := []struct {
		sql      string
		readOnly bool
	}{
		{`SELECT ? FROM table_name`, true},
		{`(select * from t1) union (select * from t2)`, true},
		{`WITH cte AS (SELECT 1, 2) SELECT * FROM cte t1, cte t2`, true},
		{`SELECT ? FROM table_name for update`, false},
		{`SELECT "for update"`, true},
		{`SET session_States ''`, true},
		{`SET @@session_variable=true`, true},
		{`SET @@global.variable=true`, false},
		{`set GLOBAL variable=false`, false},
		{`set password = 'hello'`, false},
		{`set	NAMES utf8`, true},
		{`set character utf8`, true},
		{`set transaction isolation_level = 'read committed`, true},
		{`SET @variable=true`, true},
		{`insert into table t value(1)`, false},
		{`desc table t`, true},
		{`describe select * from t`, true},
		{`show tables`, true},
		{`admin show ddl jobs`, false},
		{`explain select * from t`, false},
		{`explain analyze insert into table t value(1)`, false},
		{`use db`, true},
		{`TABLE t1`, true},
		{`do 1`, true},
		{`/*hello */select 1`, true},
		{`    select 1`, true},
		{`/**/ start transaction`, true},
		{`  COMMIT`, true},
	}

	for _, test := range tests {
		require.Equal(t, test.readOnly, IsReadOnly(test.sql), test.sql)
	}
}

func TestStartTxn(t *testing.T) {
	tests := []struct {
		stmt    string
		isBegin bool
	}{
		{
			stmt:    "begin",
			isBegin: true,
		},
		{
			stmt:    "BEGIN",
			isBegin: true,
		},
		{
			stmt:    "begin optimistic as of timestamp now()",
			isBegin: true,
		},
		{
			stmt:    "    begin",
			isBegin: true,
		},
		{
			stmt:    "start transaction",
			isBegin: true,
		},
		{
			stmt:    "START transaction",
			isBegin: true,
		},
		{
			stmt:    "start transaction with consistent snapshot",
			isBegin: true,
		},
		{
			stmt:    "begin; select 1",
			isBegin: true,
		},
		{
			stmt:    "/*+ some_hint */begin",
			isBegin: true,
		},
		{
			stmt:    "commit",
			isBegin: false,
		},
		{
			stmt:    "select 1; begin",
			isBegin: false,
		},
	}
	for _, test := range tests {
		require.Equal(t, test.isBegin, IsStartTxn(test.stmt), test.stmt)
	}
}
