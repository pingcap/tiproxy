// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDecodeMultiLinesForExtension(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		{
			// db is changed in the second sql
			lines: `[2026/01/08 19:44:11.099 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-0009] [EVENT="[QUERY,QUERY_DDL]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)"]
[2026/01/08 19:44:11.110 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-000a] [EVENT="[QUERY,QUERY_DML,INSERT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test1] [SQL_TEXT="INSERT IGNORE INTO test_table (id, name, age, salary, price, weight, is_active, created_at) VALUES (1, 'base_record', 25, 50000.00, 99.99, 123.456789, true, '2023-01-01 12:00:00')"] [AFFECTED_ROWS=1]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 99000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 99000000, time.Local),
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)")...),
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test1",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 110000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 110000000, time.Local),
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("INSERT IGNORE INTO test_table (id, name, age, salary, price, weight, is_active, created_at) VALUES (1, 'base_record', 25, 50000.00, 99.99, 123.456789, true, '2023-01-01 12:00:00')")...),
					Line:           2,
					Success:        true,
				},
			},
		},
		{
			// db stays the same in the second sql
			lines: `[2026/01/08 19:44:11.099 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-0009] [EVENT="[QUERY,QUERY_DDL]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)"]
[2026/01/08 19:44:11.110 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-000a] [EVENT="[QUERY,QUERY_DML,INSERT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="INSERT IGNORE INTO test_table (id, name, age, salary, price, weight, is_active, created_at) VALUES (1, 'base_record', 25, 50000.00, 99.99, 123.456789, true, '2023-01-01 12:00:00')"] [AFFECTED_ROWS=1]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 99000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 99000000, time.Local),
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)")...),
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 110000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 110000000, time.Local),
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("INSERT IGNORE INTO test_table (id, name, age, salary, price, weight, is_active, created_at) VALUES (1, 'base_record', 25, 50000.00, 99.99, 123.456789, true, '2023-01-01 12:00:00')")...),
					Line:           2,
					Success:        true,
				},
			},
		},
		{
			// new connection + quit connection
			lines: `[2026/01/08 19:44:27.746 +08:00] [INFO] [ID=2eda2307-54c1-49e5-9e65-eebf340b6d8f-0002] [EVENT="[CONNECTION,CONNECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047206] [SESSION_ALIAS=] [TABLES="[]"] [STATUS_CODE=1] [CURRENT_DB=test] [CONNECTION_TYPE=TCP] [PID=454064] [SERVER_VERSION=v9.0.0-beta.2.pre-1017-gbedae51b62] [SSL_VERSION=] [HOST_IP=127.0.0.1] [HOST_PORT=4000] [CLIENT_IP=127.0.0.1] [CLIENT_PORT=49366] [AUTH_METHOD=mysql_native_password] [CONN_ATTRS="{\"_client_name\":\"Go-MySQL-Driver\",\"_os\":\"linux\",\"_pid\":\"460879\",\"_platform\":\"amd64\",\"_server_host\":\"127.0.0.1\"}"]
[2026/01/08 19:44:27.782 +08:00] [INFO] [ID=2eda2307-54c1-49e5-9e65-eebf340b6d8f-0003] [EVENT="[QUERY,QUERY_DDL]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047206] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)"]
[2026/01/08 19:44:27.963 +08:00] [INFO] [ID=2eda2307-54c1-49e5-9e65-eebf340b6d8f-000e] [EVENT="[CONNECTION,DISCONNECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047206] [SESSION_ALIAS=] [TABLES="[]"] [STATUS_CODE=1] [CONNECTION_TYPE=TCP] [PID=454064] [SERVER_VERSION=v9.0.0-beta.2.pre-1017-gbedae51b62] [SSL_VERSION=] [HOST_IP=127.0.0.1] [HOST_PORT=4000] [CLIENT_IP=127.0.0.1] [CLIENT_PORT=49366] [AUTH_METHOD=mysql_native_password] [CONN_ATTRS="{\"_client_name\":\"Go-MySQL-Driver\",\"_os\":\"linux\",\"_pid\":\"460879\",\"_platform\":\"amd64\",\"_server_host\":\"127.0.0.1\"}"]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 27, 782000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 27, 782000000, time.Local),
					ConnID:         260047206,
					UpstreamConnID: 260047206,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("CREATE TABLE IF NOT EXISTS test_table (id BIGINT PRIMARY KEY, name VARCHAR(255), age INT, salary DECIMAL(10,2), price FLOAT, weight DOUBLE, is_active BOOLEAN, data BLOB, created_at TIMESTAMP)")...),
					Line:           2,
					Success:        true,
				},
				{
					CurDB:          "",
					StartTs:        time.Date(2026, 1, 8, 19, 44, 27, 963000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 27, 963000000, time.Local),
					ConnID:         260047206,
					UpstreamConnID: 260047206,
					Type:           pnet.ComQuit,
					Payload:        []byte{pnet.ComQuit.Byte()},
					Line:           3,
					Success:        true,
				},
			},
		},
		{
			// 2 prepared statements
			lines: `[2026/01/08 19:44:11.114 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[1]"]
[2026/01/08 19:44:11.379 +08:00] [INFO] [ID=187143c0-a8ea-44ec-b28d-5af6129fb3f9-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[0]"]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT * FROM test_table WHERE is_active = ?")...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x31},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"1"},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   2,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT * FROM test_table WHERE is_active = ?")...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   2,
					Payload:        []byte{0x17, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x30},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"0"},
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   2,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{2, 0, 0, 0}...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           2,
					Success:        true,
				},
			},
		},
		{
			// 2 different connections
			lines: `[2026/01/08 19:44:11.114 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[1]"]
[2026/01/08 19:44:11.379 +08:00] [INFO] [ID=187143c0-a8ea-44ec-b28d-5af6129fb3f9-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047063] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[0]"]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT * FROM test_table WHERE is_active = ?")...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x31},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"1"},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         260047063,
					UpstreamConnID: 260047063,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT * FROM test_table WHERE is_active = ?")...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047063,
					UpstreamConnID: 260047063,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   1,
					Payload:        []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x30},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"0"},
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         260047063,
					UpstreamConnID: 260047063,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           2,
					Success:        true,
				},
			},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogExtensionDecoder(zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyAlways)
		mr := mockReader{data: append([]byte(test.lines), '\n'), filename: "my/file"}
		cmds, err := decodeCmds(decoder, &mr)
		if err != nil {
			require.ErrorContains(t, err, "EOF", "case %d", i)
		}
		for _, cmd := range test.cmds {
			cmd.FileName = "my/file"
		}
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func TestDecodeAuditExtensionInNeverMode(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		{
			lines: `[2026/01/08 19:44:11.114 +08:00] [INFO] [ID=f1c681c2-8d80-4677-9dd4-6b7222610aa8-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[1]"]
[2026/01/08 19:44:11.379 +08:00] [INFO] [ID=187143c0-a8ea-44ec-b28d-5af6129fb3f9-000b] [EVENT="[QUERY,EXECUTE,SELECT]"] [USER=root] [ROLES="[]"] [CONNECTION_ID=260047062] [SESSION_ALIAS=] [TABLES="[` + "`" + `test` + "`" + `.` + "`" + `test_table` + "`" + `]"] [STATUS_CODE=1] [CURRENT_DB=test] [SQL_TEXT="SELECT * FROM test_table WHERE is_active = ?"] [EXECUTE_PARAMS="[0]"]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT * FROM test_table WHERE is_active = ?")...),
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 114000000, time.Local),
					CapturedPsID:   1,
					Payload:        []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x31},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"1"},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         260047062,
					UpstreamConnID: 260047062,
					StartTs:        time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					EndTs:          time.Date(2026, 1, 8, 19, 44, 11, 379000000, time.Local),
					CapturedPsID:   1,
					Payload:        []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xfe, 0x00, 0x01, 0x30},
					StmtType:       "",
					PreparedStmt:   "SELECT * FROM test_table WHERE is_active = ?",
					Params:         []any{"0"},
					Line:           2,
					Success:        true,
				},
			},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogExtensionDecoder(zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyNever)
		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds, err := decodeCmds(decoder, &mr)
		require.Error(t, err, "case %d", i)
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func TestParseExecuteParamsForExtension(t *testing.T) {
	tests := []struct {
		input    string
		expected []any
		hasError bool
	}{
		{
			input:    `"[]"`,
			expected: nil,
			hasError: false,
		},
		{
			input:    `"[1]"`,
			expected: []any{"1"},
			hasError: false,
		},
		{
			input:    `"[1,2,3]"`,
			expected: []any{"1", "2", "3"},
			hasError: false,
		},
		{
			input:    `"[abc]"`,
			expected: []any{"abc"},
			hasError: false,
		},
		{
			input:    `"['abc']"`,
			expected: []any{"'abc'"},
			hasError: false,
		},
		{
			input:    `"[NULL]"`,
			expected: []any{"NULL"},
			hasError: false,
		},
		{
			input:    `"[1,abc,NULL,\"test bytes\"]"`,
			expected: []any{"1", "abc", "NULL", "test bytes"},
			hasError: false,
		},
		{
			input:    `"[\"test\\\"escape\"]"`,
			expected: []any{"test\"escape"},
			hasError: false,
		},
		{
			input:    `"[\"hello world\"]"`,
			expected: []any{"hello world"},
			hasError: false,
		},
		{
			input:    `"[1, 2, 3]"`,
			expected: []any{"1", "2", "3"},
			hasError: false,
		},
		{
			input:    `"[\"hello, world\"]"`,
			expected: []any{"hello, world"},
			hasError: false,
		},
		{
			input:    `"[1.5,2.75,3.14]"`,
			expected: []any{"1.5", "2.75", "3.14"},
			hasError: false,
		},
		{
			input:    `"[true,false]"`,
			expected: []any{"true", "false"},
			hasError: false,
		},
		{
			input:    `"[1,2,3"`,
			expected: nil,
			hasError: true,
		},
		{
			input:    `"1,2,3]"`,
			expected: nil,
			hasError: true,
		},
		{
			input:    `"[\"abc ]"`,
			expected: nil,
			hasError: true,
		},
		{
			input:    `[1,2,3]`,
			expected: nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		result, err := parseExecuteParamsForExtension(tt.input)
		if tt.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		}
	}
}
