// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

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

	for i, test := range tests {
		require.Equal(t, test.sensitive, IsSensitiveSQL(test.sql), "case %d", i)
	}
}
