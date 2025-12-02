// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"io"
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSkipQuotes(t *testing.T) {
	tests := []struct {
		line        string
		singleQuote bool
		endIdx      int
	}{
		{
			line:        "",
			singleQuote: false,
			endIdx:      -1,
		},
		{
			line:        "\"",
			singleQuote: true,
			endIdx:      -1,
		},
		{
			line:        "'",
			singleQuote: false,
			endIdx:      -1,
		},
		{
			line:        "\"",
			singleQuote: false,
			endIdx:      0,
		},
		{
			line:        "'",
			singleQuote: true,
			endIdx:      0,
		},
		{
			line:        "abc'abc",
			singleQuote: true,
			endIdx:      3,
		},
		{
			line:        "\\'",
			singleQuote: true,
			endIdx:      -1,
		},
		{
			line:        "\\'",
			singleQuote: true,
			endIdx:      -1,
		},
		{
			line:        "\\\\'",
			singleQuote: true,
			endIdx:      2,
		},
	}

	for i, test := range tests {
		endIdx := skipQuotes(test.line, test.singleQuote)
		require.Equal(t, test.endIdx, endIdx, "case %d", i)
	}
}

func TestParseInBracket(t *testing.T) {
	tests := []struct {
		line   string
		key    string
		val    string
		endIdx int
		hasErr bool
	}{
		{
			line:   "",
			key:    "",
			val:    "",
			endIdx: 0,
			hasErr: true,
		},
		{
			line:   "]",
			key:    "",
			val:    "",
			endIdx: 0,
			hasErr: false,
		},
		{
			line:   "a=b]",
			key:    "a",
			val:    "b",
			endIdx: 3,
			hasErr: false,
		},
		{
			line:   "a=b",
			key:    "",
			val:    "",
			endIdx: 2,
			hasErr: true,
		},
		{
			line:   "abc]",
			key:    "",
			val:    "abc",
			endIdx: 3,
			hasErr: false,
		},
		{
			line:   "abc=]",
			key:    "abc",
			val:    "",
			endIdx: 4,
			hasErr: false,
		},
		{
			line:   "=abc]",
			key:    "",
			val:    "abc",
			endIdx: 4,
			hasErr: true,
		},
		{
			line:   "a\"]",
			key:    "",
			val:    "a\"",
			endIdx: 2,
			hasErr: true,
		},
		{
			line:   "a\"]\"",
			key:    "",
			val:    "",
			endIdx: 3,
			hasErr: true,
		},
		{
			line:   "a\"]\"]",
			key:    "",
			val:    "a\"]\"",
			endIdx: 4,
			hasErr: false,
		},
		{
			line:   "a\"]\"a\"]",
			key:    "",
			val:    "",
			endIdx: 6,
			hasErr: true,
		},
		{
			line:   "a\"]\"]abc",
			key:    "",
			val:    "a\"]\"",
			endIdx: 4,
			hasErr: false,
		},
	}

	for i, test := range tests {
		key, val, endIdx, err := parseInBracket(test.line)
		if test.hasErr {
			require.Error(t, err, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		require.Equal(t, test.key, key, "case %d", i)
		require.Equal(t, test.val, val, "case %d", i)
		require.Equal(t, test.endIdx, endIdx, "case %d", i)
	}
}

func TestParseLog(t *testing.T) {
	tests := []struct {
		line   string
		kvs    map[string]string
		hasErr bool
	}{
		{
			line:   "",
			hasErr: false,
		},
		{
			line:   "[abc]",
			hasErr: false,
		},
		{
			line:   "[a=b",
			hasErr: true,
		},
		{
			line:   "[abc",
			hasErr: true,
		},
		{
			line:   "[abc=def",
			hasErr: true,
		},
		{
			line:   "[=def",
			hasErr: true,
		},
		{
			line:   "[=def]",
			hasErr: true,
		},
		{
			line:   "[abc=def]",
			kvs:    map[string]string{"abc": "def"},
			hasErr: false,
		},
		{
			line:   "[abc=def=ghi",
			hasErr: true,
		},
		{
			line:   "[abc=def=ghi]",
			kvs:    map[string]string{"abc": "def=ghi"},
			hasErr: false,
		},
		{
			line:   "[a=\"b\"]",
			kvs:    map[string]string{"a": "\"b\""},
			hasErr: false,
		},
		{
			line:   "[a=\"b]",
			hasErr: true,
		},
		{
			line:   "[abc][a=b]",
			kvs:    map[string]string{"a": "b"},
			hasErr: false,
		},
		{
			line:   "[abc][a=b",
			hasErr: true,
		},
		{
			line:   "a[abc]a",
			hasErr: false,
		},
		{
			line:   "a[a=b]a",
			kvs:    map[string]string{"a": "b"},
			hasErr: false,
		},
		{
			line:   "a[a=b]a[c=d]",
			kvs:    map[string]string{"a": "b", "c": "d"},
			hasErr: false,
		},
		{
			line:   "a[a=b]a[c=d",
			hasErr: true,
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			kvs:    map[string]string{"ID": "17571494330", "TIMESTAMP": "2025/09/06 17:03:53.720 +08:00", "EVENT_CLASS": "GENERAL", "EVENT_SUBCLASS": "", "STATUS_CODE": "0", "COST_TIME": "1336.083", "HOST": "127.0.0.1", "CLIENT_IP": "127.0.0.1", "USER": "root", "DATABASES": "\"[]\"", "TABLES": "\"[]\"", "SQL_TEXT": "\"select \\\"[=]\\\"\"", "ROWS": "0", "CONNECTION_ID": "3695181836", "CLIENT_PORT": "63912", "PID": "61215", "COMMAND": "Query", "SQL_STATEMENTS": "Select"},
			hasErr: false,
		},
		{
			line:   `[2025/09/06 17:03:53.717 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.717 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=824806376.375] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"\n\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			kvs:    map[string]string{"ID": "17571494330", "TIMESTAMP": "2025/09/06 17:03:53.717 +08:00", "EVENT_CLASS": "GENERAL", "EVENT_SUBCLASS": "", "STATUS_CODE": "0", "COST_TIME": "824806376.375", "HOST": "127.0.0.1", "CLIENT_IP": "127.0.0.1", "USER": "root", "DATABASES": "\"[]\"", "TABLES": "\"[]\"", "SQL_TEXT": "\"select \\\"\\n\\\"\"", "ROWS": "0", "CONNECTION_ID": "3695181836", "CLIENT_PORT": "63912", "PID": "61215", "COMMAND": "Query", "SQL_STATEMENTS": "Select"},
			hasErr: false,
		},
		{
			line:   `[2025/09/06 16:50:08.917 +08:00] [INFO] [logger.go:77] [ID=17571486080] [TIMESTAMP=2025/09/06 16:50:08.917 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=2442.333] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"\n\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			kvs:    map[string]string{"ID": "17571486080", "TIMESTAMP": "2025/09/06 16:50:08.917 +08:00", "EVENT_CLASS": "GENERAL", "EVENT_SUBCLASS": "", "STATUS_CODE": "0", "COST_TIME": "2442.333", "HOST": "127.0.0.1", "CLIENT_IP": "127.0.0.1", "USER": "root", "DATABASES": "\"[]\"", "TABLES": "\"[]\"", "SQL_TEXT": "\"select \\\"\\n\\\"\"", "ROWS": "0", "CONNECTION_ID": "3695181836", "CLIENT_PORT": "63912", "PID": "61215", "COMMAND": "Query", "SQL_STATEMENTS": "Select"},
			hasErr: false,
		},
	}

	for i, test := range tests {
		var err error
		kvs := make(map[string]string)
		err = parseLog(kvs, test.line)
		if test.hasErr {
			require.Error(t, err, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		if len(test.kvs) == 0 && len(kvs) == 0 {
			continue
		}
		require.EqualValues(t, test.kvs, kvs, "case %d", i)
	}
}

func TestCommand(t *testing.T) {
	tests := []struct {
		s      string
		expect string
	}{
		{
			s:      ``,
			expect: "",
		},
		{
			s:      `"Init DB"`,
			expect: "Init DB",
		},
		{
			s:      `Query`,
			expect: "Query",
		},
	}
	for i, test := range tests {
		cmd := parseCommand(test.s)
		require.EqualValues(t, test.expect, cmd, "case %d", i)
	}
}

func TestParseStartTs(t *testing.T) {
	tests := []struct {
		kvs     map[string]string
		startTS time.Time
		endTS   time.Time
		errMsg  string
	}{
		{
			kvs: map[string]string{
				auditPluginKeyTimeStamp: "2025/09/06 17:03:50.888 +08:10",
				auditPluginKeyCostTime:  "666000",
			},
			startTS: time.Date(2025, 9, 6, 17, 3, 50, 222000000, time.FixedZone("", 8*3600+600)),
			endTS:   time.Date(2025, 9, 6, 17, 3, 50, 888000000, time.FixedZone("", 8*3600+600)),
		},
		{
			kvs: map[string]string{
				auditPluginKeyTimeStamp: "2025/09/06 17:03:53.717 +08:10",
				auditPluginKeyCostTime:  "",
			},
			startTS: time.Date(2025, 9, 6, 17, 3, 53, 717000000, time.FixedZone("", 8*3600+600)),
			endTS:   time.Date(2025, 9, 6, 17, 3, 53, 717000000, time.FixedZone("", 8*3600+600)),
		},
		{
			kvs: map[string]string{
				auditPluginKeyTimeStamp: "2025/09/06 17:03:53.717 +08:10",
			},
			startTS: time.Date(2025, 9, 6, 17, 3, 53, 717000000, time.FixedZone("", 8*3600+600)),
			endTS:   time.Date(2025, 9, 6, 17, 3, 53, 717000000, time.FixedZone("", 8*3600+600)),
		},
		{
			kvs: map[string]string{
				auditPluginKeyTimeStamp: "2025/09/06",
			},
			errMsg: "parsing timestamp failed",
		},
	}

	for i, test := range tests {
		startTS, endTS, err := parseStartAndEndTs(test.kvs)
		if test.errMsg != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.errMsg, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		require.EqualValues(t, test.startTS, startTS, "case %d", i)
		require.EqualValues(t, test.endTS, endTS, "case %d", i)
	}
}

func TestParseSQL(t *testing.T) {
	tests := []struct {
		value  string
		sql    string
		errMsg string
	}{
		{
			value: `COMMIT`,
			sql:   `COMMIT`,
		},
		{
			value: `"SELECT 1"`,
			sql:   `SELECT 1`,
		},
		{
			value:  ``,
			errMsg: "empty",
		},
	}

	for i, test := range tests {
		sql, err := parseSQL(test.value)
		if test.errMsg != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.errMsg, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		require.EqualValues(t, test.sql, sql, "case %d", i)
	}
}

func TestParseParams(t *testing.T) {
	tests := []struct {
		s      string
		expect []any
		errMsg string
	}{
		{
			s:      ``,
			errMsg: "no quotes in params",
		},
		{
			s:      `""`,
			expect: []any{},
		},
		{
			s:      `"[]"`,
			expect: []any{},
		},
		{
			s:      `"[\"KindInt64\"]"`,
			errMsg: "no space in param",
		},
		{
			s:      `"[\"KindInt64 \"]"`,
			errMsg: "invalid syntax",
		},
		{
			s:      `"[\"1\"]"`,
			errMsg: "no space in param",
		},
		{
			s:      `"[\"Unknown 1\"]"`,
			errMsg: "unknown param type",
		},
		{
			s: `"[\"KindInt64 1\"]"`,
			expect: []any{
				int64(1),
			},
		},
		{
			s: `"[\"KindInt64 -9223372036854775808\"]"`,
			expect: []any{
				int64(-9223372036854775808),
			},
		},
		{
			s: `"[\"KindFloat64 -123.45600128173828\"]"`,
			expect: []any{
				float64(-123.45600128173828),
			},
		},
		{
			s: `"[\"KindString \"]"`,
			expect: []any{
				"",
			},
		},
		{
			s: `"[\"KindString '单引号' \\\\\\\"双引号\\\\\\\"\\\\\\\\n\\\\\\\\t😊\\\\x00\\\\x00\"]"`,
			expect: []any{
				"'单引号' \"双引号\"\\n\\t😊\x00\x00",
			},
		},
		{
			s: `"[\"KindString {\\\\\\\"key\\\\\\\": \\\\\\\"value\\\\\\\\nwith\\\\\\\\tescaped\\\\\\\\\\\\\\\"chars\\\\\\\"}\"]"`,
			expect: []any{
				`{"key": "value\nwith\tescaped\"chars"}`,
			},
		},
		{
			s: `"[\"KindString hello\\\\nworld\\\\t\\\\\\\"quoted\\\\\\\"\"]"`,
			expect: []any{
				"hello\nworld\t\"quoted\"",
			},
		},
		{
			s: `"[\"KindBytes hello\\\\nworld\\\\t\\\\\\\"quoted\\\\\\\"\"]"`,
			expect: []any{
				[]byte("hello\nworld\t\"quoted\""),
			},
		},
		{
			s: `"[\"KindNull <nil>\"]"`,
			expect: []any{
				nil,
			},
		},
		{
			s: `"[\"KindString 37\",\"KindInt64 2\",\"KindString user_5556\",\"KindInt64 1\"]"`,
			expect: []any{
				string("37"),
				int64(2),
				string("user_5556"),
				int64(1),
			},
		},
	}

	for i, test := range tests {
		params, err := parseExecuteParams(test.s)
		if test.errMsg != "" {
			require.Error(t, err, "case %d", i)
			require.Contains(t, err.Error(), test.errMsg, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		if len(params) == 0 && len(test.expect) == 0 {
			continue
		}
		require.Equal(t, test.expect, params, "case %d", i)
	}
}

func TestDecodeSingleLine(t *testing.T) {
	tests := []struct {
		line   string
		cmds   []*Command
		errMsg string
	}{
		{
			// sql with current db
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComQuery,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Success:        true,
				},
			},
		},
		{
			// sql without current database
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComQuery,
					CurDB:          "",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Success:        true,
				},
			},
		},
		{
			// prepared statement
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"?\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 1\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select \"?\"")...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Params:         []any{int64(1)},
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Success:        true,
				},
			},
		},
		{
			// prepared statement without params field, ignore
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"?\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Execute] [SQL_STATEMENTS=Select] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{},
		},
		{
			// ignore starting event
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=STARTING]`,
			cmds: []*Command{},
		},
		{
			// ignore new connection
			line: `[2025/09/08 21:15:12.904 +08:00] [INFO] [logger.go:77] [ID=17573373120] [TIMESTAMP=2025/09/08 21:15:12.904 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Connected] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=49278] [CONNECTION_ID=3552575510] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=0] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			cmds: []*Command{},
		},
		{
			// quit
			line: `[2025/09/08 21:15:35.621 +08:00] [INFO] [logger.go:77] [ID=17573373350] [TIMESTAMP=2025/09/08 21:15:35.621 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Disconnect] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=49278] [CONNECTION_ID=3552575510] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=22716.871792] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			cmds: []*Command{
				{
					Type:           pnet.ComQuit,
					ConnID:         3552575510,
					UpstreamConnID: 3552575510,
					StartTs:        time.Date(2025, 9, 8, 21, 15, 35, 621000000, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 8, 21, 15, 35, 621000000, time.FixedZone("", 8*3600+600)),
					Payload:        []byte{pnet.ComQuit.Byte()},
					Success:        true,
				},
			},
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			errMsg: "parsing timestamp failed",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			errMsg: "no connection id",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=abc] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			errMsg: "parsing connection id failed",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=HELLO] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			errMsg: "unknown event class",
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyAlways)
		mr := mockReader{data: append([]byte(test.line), '\n'), filename: "my/file"}
		cmds, err := decodeCmds(decoder, &mr)
		require.Error(t, err, "case %d", i)
		if len(test.errMsg) > 0 {
			require.ErrorContains(t, err, test.errMsg, "case %d", i)
			continue
		} else {
			require.ErrorIs(t, err, io.EOF, "case %d", i)
		}
		for _, cmd := range test.cmds {
			cmd.FileName = "my/file"
			cmd.Line = 1
		}
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func TestDecodeMultiLines(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		{
			// db is changed in the second sql
			lines: `[2025/09/08 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/06 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="set sql_mode=''"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=] [EVENT=COMPLETED]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=STARTING]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					StartTs:        time.Date(2025, 9, 6, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("set sql_mode=''")...),
					StmtType:       "Set",
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
		{
			// db stays the same in the second sql
			lines: `[2025/09/08 21:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/06 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="set sql_mode=''"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=STARTING]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 6, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("set sql_mode=''")...),
					StmtType:       "Set",
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
		{
			// new connection + quit connection
			lines: `[2025/09/08 17:23:58.279 +08:00] [INFO] [logger.go:77] [ID=17574098380] [TIMESTAMP=2025/09/08 17:23:58.277 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Connected] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=52797] [CONNECTION_ID=3552575570] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=0] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]
[2025/09/08 21:16:52.630 +08:00] [INFO] [logger.go:77] [ID=17573374120] [TIMESTAMP=2025/09/08 21:16:52.630 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Disconnect] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=52620] [CONNECTION_ID=3552575570] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=0.0445] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			cmds: []*Command{
				{
					StartTs:        time.Date(2025, 9, 8, 21, 16, 52, 630000000, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 8, 21, 16, 52, 630000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3552575570,
					UpstreamConnID: 3552575570,
					Payload:        []byte{pnet.ComQuit.Byte()},
					Type:           pnet.ComQuit,
					Line:           2,
					Success:        true,
				},
			},
		},
		{
			// 2 prepared statements
			lines: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"?\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 1\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"?\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 1\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select \"?\"")...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Params:         []any{int64(1)},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   2,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select \"?\"")...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   2,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Params:         []any{int64(1)},
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   2,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{2, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "select \"?\"",
					Line:           2,
					Success:        true,
				},
			},
		},
		{
			// 2 different connections
			lines: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181837] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComQuery,
					CurDB:          "test",
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComQuery,
					CurDB:          "test",
					ConnID:         3695181837,
					UpstreamConnID: 3695181837,
					StartTs:        time.Date(2025, 9, 6, 17, 3, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           2,
					Success:        true,
				},
			},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
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

func TestDecodeAuditLogWithCommandStartTime(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		{
			// db is changed in the second sql
			lines: `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="set sql_mode=''"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/14 16:16:30.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=STARTING]
[2025/09/14 16:16:31.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 53, 720000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
		{
			// The latest DB is used
			lines: `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="set sql_mode=''"] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Set] [EXECUTE_PARAMS="[]"] [CURRENT_DB=a] [EVENT=COMPLETED]
[2025/09/14 16:16:30.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=b] [EVENT=STARTING]
[2025/09/14 16:16:31.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=b] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "b",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 53, 718663917, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 53, 720000000, time.FixedZone("", 8*3600+600)),
					ConnID:         3695181836,
					UpstreamConnID: 3695181836,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
	}

	commandStartTime := time.Date(2025, 9, 14, 16, 16, 53, 0, time.FixedZone("", 8*3600+600))
	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyAlways)
		decoder.SetCommandStartTime(commandStartTime)
		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds, err := decodeCmds(decoder, &mr)
		if err != nil {
			require.ErrorContains(t, err, "EOF", "case %d", i)
		}
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func BenchmarkAuditLogPluginDecoder(b *testing.B) {
	decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
	decoder.SetPSCloseStrategy(PSCloseStrategyAlways)
	reader := &endlessReader{
		line: `[2025/09/14 16:16:31.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=b] [EVENT=COMPLETED]`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := decoder.Decode(reader)
		require.NoError(b, err)
	}
}

func TestDecodeAuditLogInDirectedMode(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		// Simple case: PREPARE -> EXECUTE -> CLOSE
		{
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]
			[2025/09/18 17:51:56.999 +08:10] [INFO] [logger.go:77] [ID=175818911610038] [TIMESTAMP=2025/09/18 17:51:56.999 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=119.689] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND="Close stmt"] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 500350\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT c FROM sbtest1 WHERE id=?")...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xe8, 0xaf, 0x07, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(503784)},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 51, 56, 998880311, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 51, 56, 999000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "Select",
					Line:           2,
					Success:        true,
				},
			},
		},
		// EXECUTE the same PS twice
		{
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]
			[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 124153\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]
			[2025/09/18 17:51:56.999 +08:10] [INFO] [logger.go:77] [ID=175818911610038] [TIMESTAMP=2025/09/18 17:51:56.999 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=119.689] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND="Close stmt"] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 500350\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT c FROM sbtest1 WHERE id=?")...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xe8, 0xaf, 0x07, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(503784)},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xf9, 0xe4, 0x01, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(124153)},
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 51, 56, 998880311, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 51, 56, 999000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
		// EXECUTE -> CLOSE -> EXECUTE
		{
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]
			[2025/09/18 17:51:56.999 +08:10] [INFO] [logger.go:77] [ID=175818911610038] [TIMESTAMP=2025/09/18 17:51:56.999 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=119.689] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND="Close stmt"] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 500350\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]
			[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 124153\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT c FROM sbtest1 WHERE id=?")...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xe8, 0xaf, 0x07, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(503784)},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtClose,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 51, 56, 998880311, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 51, 56, 999000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtClose.Byte()}, []byte{1, 0, 0, 0}...),
					StmtType:       "Select",
					Line:           2,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT c FROM sbtest1 WHERE id=?")...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Line:           3,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xf9, 0xe4, 0x01, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(124153)},
					Line:           3,
					Success:        true,
				},
			},
		},
		{
			// CLOSE
			lines: `[2025/09/18 17:51:56.999 +08:10] [INFO] [logger.go:77] [ID=175818911610038] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND="Close stmt"] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 500350\"]"] [CURRENT_DB=test] [EVENT=COMPLETED] [PREPARED_STMT_ID=1]`,
			cmds:  []*Command{},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyDirected)
		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds, err := decodeCmds(decoder, &mr)
		require.Error(t, err, "case %d", i)
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func TestDecodeAuditLogInNeverMode(t *testing.T) {
	tests := []struct {
		lines string
		cmds  []*Command
	}{
		{
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=48.86] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT c FROM sbtest1 WHERE id=?"] [ROWS=0] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					Type:           pnet.ComStmtPrepare,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("SELECT c FROM sbtest1 WHERE id=?")...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xe8, 0xaf, 0x07, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(503784)},
					Line:           1,
					Success:        true,
				},
				{
					Type:           pnet.ComStmtExecute,
					CurDB:          "test",
					ConnID:         3807050215081378201,
					UpstreamConnID: 3807050215081378201,
					StartTs:        time.Date(2025, 9, 18, 17, 48, 20, 613951140, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 18, 17, 48, 20, 614000000, time.FixedZone("", 8*3600+600)),
					CapturedPsID:   1,
					Payload:        append([]byte{pnet.ComStmtExecute.Byte()}, []byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 0xe8, 0xaf, 0x07, 0, 0, 0, 0, 0}...),
					StmtType:       "Select",
					PreparedStmt:   "SELECT c FROM sbtest1 WHERE id=?",
					Params:         []any{int64(503784)},
					Line:           2,
					Success:        true,
				},
			},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyNever)
		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds, err := decodeCmds(decoder, &mr)
		require.Error(t, err, "case %d", i)
		require.Equal(t, test.cmds, cmds, "case %d", i)
	}
}

func TestStringUnquoteForParam(t *testing.T) {
	tests := []struct {
		input    string
		expected any
	}{
		{"KindString hello", "hello"},
		{"KindString hello\\nworld\\t\\\"quoted\\\"", "hello\nworld\t\"quoted\""},
		{`KindString {\"key\": \"value\\nwith\\tescaped\\\"chars\"}`, `{"key": "value\nwith\tescaped\"chars"}`},
		{"KindBytes hello\\nworld\\t\\\"quoted\\\"", []byte("hello\nworld\t\"quoted\"")},
	}

	for _, test := range tests {
		result, err := parseSingleParam(test.input)
		require.NoError(t, err)
		require.Equal(t, test.expected, result)
	}

	params, err := parseExecuteParams(`"[\"KindString {\\\\\\\"key\\\\\\\": \\\\\\\"value\\\\\\\"}\"]"`)
	require.NoError(t, err)
	expectedParams := []any{
		"{\"key\": \"value\"}",
	}
	require.Equal(t, expectedParams, params)
}

func TestAuditLogDecoderWithIDAllocator(t *testing.T) {
	tests := []struct {
		lines     string
		decoderID int
		cmds      []*Command
	}{
		// Case 0: Single connection with decoder ID 123
		{
			decoderID: 123,
			lines:     `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=1001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(123) << 54) + 1,
					UpstreamConnID: 1001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
					StmtType:       "Select",
					Line:           1,
					Success:        true,
				},
			},
		},
		// Case 1: Multiple lines with same upstream connection ID
		{
			decoderID: 456,
			lines: `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=2001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/14 16:16:30.585 +08:00] [INFO] [logger.go:77] [ID=17573373892] [TIMESTAMP=2025/09/14 16:16:30.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 2"] [ROWS=0] [CONNECTION_ID=2001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(456) << 54) + 1,
					UpstreamConnID: 2001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
					StmtType:       "Select",
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 30, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 30, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(456) << 54) + 1,
					UpstreamConnID: 2001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 2")...),
					StmtType:       "Select",
					Line:           2,
					Success:        true,
				},
			},
		},
		// Case 2: Different upstream connections get different allocated connection IDs
		{
			decoderID: 789,
			lines: `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=3001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/14 16:16:30.585 +08:00] [INFO] [logger.go:77] [ID=17573373892] [TIMESTAMP=2025/09/14 16:16:30.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 2"] [ROWS=0] [CONNECTION_ID=3002] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(789) << 54) + 1,
					UpstreamConnID: 3001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
					StmtType:       "Select",
					Line:           1,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 30, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 30, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(789) << 54) + 2,
					UpstreamConnID: 3002,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 2")...),
					StmtType:       "Select",
					Line:           2,
					Success:        true,
				},
			},
		},
		// Case 3: Same upstream connection ID after ComQuit should have different allocated connection ID
		{
			decoderID: 100,
			lines: `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17573373891] [TIMESTAMP=2025/09/14 16:16:29.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=4001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/14 16:16:30.585 +08:00] [INFO] [logger.go:77] [ID=17573373892] [TIMESTAMP=2025/09/14 16:16:30.585 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Disconnect] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [CONNECTION_ID=4001] [CLIENT_PORT=52611] [PID=89967] [EVENT=COMPLETED]
[2025/09/14 16:16:31.585 +08:00] [INFO] [logger.go:77] [ID=17573373893] [TIMESTAMP=2025/09/14 16:16:31.585 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1057.834] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select 1"] [ROWS=0] [CONNECTION_ID=4001] [CLIENT_PORT=52611] [PID=89967] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmds: []*Command{
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 29, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 29, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(100) << 54) + 1,
					UpstreamConnID: 4001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
					StmtType:       "Select",
					Line:           1,
					Success:        true,
				},
				{
					StartTs:        time.Date(2025, 9, 14, 16, 16, 30, 585000000, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 30, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(100) << 54) + 1,
					UpstreamConnID: 4001,
					Type:           pnet.ComQuit,
					Payload:        []byte{pnet.ComQuit.Byte()},
					Line:           2,
					Success:        true,
				},
				{
					CurDB:          "test",
					StartTs:        time.Date(2025, 9, 14, 16, 16, 31, 583942167, time.FixedZone("", 8*3600+600)),
					EndTs:          time.Date(2025, 9, 14, 16, 16, 31, 585000000, time.FixedZone("", 8*3600+600)),
					ConnID:         (uint64(100) << 54) + 2,
					UpstreamConnID: 4001,
					Type:           pnet.ComQuery,
					Payload:        append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
					StmtType:       "Select",
					Line:           3,
					Success:        true,
				},
			},
		},
	}

	for i, test := range tests {
		decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
		alloc, err := NewConnIDAllocator(test.decoderID)
		require.NoError(t, err)
		decoder.SetIDAllocator(alloc)

		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds, err := decodeCmds(decoder, &mr)
		if err != nil {
			require.ErrorContains(t, err, "EOF", "case %d", i)
		}
		require.Equal(t, test.cmds, cmds, "case %d", i)

		// Verify that the connection IDs have the correct decoder ID embedded
		for _, cmd := range cmds {
			actualDecoderID := cmd.ConnID >> 54
			require.Equal(t, test.decoderID, int(actualDecoderID), "case %d, decoder ID mismatch in command", i)
		}
	}
}

func TestConnIDAllocator(t *testing.T) {
	// Test valid decoder IDs
	alloc, err := NewConnIDAllocator(0)
	require.NoError(t, err)
	require.NotNil(t, alloc)

	// Test maximum valid decoder ID
	alloc, err = NewConnIDAllocator(1023)
	require.NoError(t, err)
	require.NotNil(t, alloc)

	// Test invalid decoder ID (too large)
	_, err = NewConnIDAllocator(1024)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoderID 1024 is too large")

	// Test allocation sequence
	alloc, err = NewConnIDAllocator(42)
	require.NoError(t, err)

	id1 := alloc.alloc()
	id2 := alloc.alloc()
	id3 := alloc.alloc()

	// Check that IDs are sequential
	require.Equal(t, id1+1, id2)
	require.Equal(t, id2+1, id3)
}

func TestDecoderCommandEndTimeFiltering(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		startTime string
		endTime   string
		shouldGet bool
	}{
		{
			name:      "command before endTime should be filtered out",
			line:      `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:29.585 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 1"] [ROWS=0] [CONNECTION_ID=1] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			startTime: "2025/09/14 16:16:28.585 +08:00",
			endTime:   "2025/09/14 16:16:30.585 +08:00",
			shouldGet: false,
		},
		{
			name:      "command ending after end time should be decoded",
			line:      `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:29.585 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 1"] [ROWS=0] [CONNECTION_ID=1] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			startTime: "2025/09/14 16:16:27.585 +08:00",
			endTime:   "2025/09/14 16:16:28.585 +08:00",
			shouldGet: true,
		},
		{
			name:      "if the endTime is not specified, command ending after end time should be decoded",
			line:      `[2025/09/14 16:16:29.585 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/14 16:16:29.585 +08:00] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 1"] [ROWS=0] [CONNECTION_ID=1] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			startTime: "",
			endTime:   "2025/09/14 16:16:28.585 +08:00",
			shouldGet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auditDecoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
			if len(tt.startTime) > 0 {
				startTime, err := time.Parse(timeLayout, tt.startTime)
				require.NoError(t, err)
				auditDecoder.SetCommandStartTime(startTime)
			}
			if len(tt.endTime) > 0 {
				endTime, err := time.Parse(timeLayout, tt.endTime)
				require.NoError(t, err)
				auditDecoder.SetCommandEndTime(endTime)
			}

			reader := mockReader{data: append([]byte(tt.line), '\n')}

			command, err := auditDecoder.Decode(&reader)
			if tt.shouldGet {
				require.NoError(t, err)
				require.NotNil(t, command)
			} else {
				// Should either return EOF or nil command due to filtering
				if err == nil {
					require.Nil(t, command)
				}
			}
		})
	}
}

func TestDeduplicateCommands(t *testing.T) {
	tests := []struct {
		lines      string
		cmdTypes   []pnet.Command
		duplicated bool
	}{
		{
			// duplicated insert
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
		[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
			},
			duplicated: true,
		},
		{
			// duplicated select for update
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT * FROM sbtest1 WHERE id = ? FOR UPDATE"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
		[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Select] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="SELECT * FROM sbtest1 WHERE id = ? FOR UPDATE"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
			},
			duplicated: true,
		},
		{
			// duplicated insert using Query
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(1)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
		[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(1)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComQuery,
			},
			duplicated: true,
		},
		{
			// time overlaps but the sql is different
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
		[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest2]"] [SQL_TEXT="INSERT INTO sbtest2 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
			},
		},
		{
			// time overlaps but the params are different
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503785\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
			},
		},
		{
			// time overlaps but the sql is different
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(1)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest2]"] [SQL_TEXT="INSERT INTO sbtest2 VALUES(1)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComQuery,
				pnet.ComQuery,
			},
		},
		{
			// same is sql but time does not overlap
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]
		[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.814 +08:10] [EVENT_CLASS=TABLE_ACCESS] [EVENT_SUBCLASS=Insert] [STATUS_CODE=0] [COST_TIME=100000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[sbtest1]"] [SQL_TEXT="INSERT INTO sbtest1 VALUES(?)"] [ROWS=1] [CONNECTION_ID=3807050215081378201] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Execute] [SQL_STATEMENTS=Insert] [EXECUTE_PARAMS="[\"KindInt64 503784\"]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			cmdTypes: []pnet.Command{
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
				pnet.ComStmtPrepare,
				pnet.ComStmtExecute,
				pnet.ComStmtClose,
			},
		},
	}

	for i, test := range tests {
		dedup := NewDeDup()
		decoder := NewAuditLogPluginDecoder(dedup, zap.NewNop())
		decoder.SetPSCloseStrategy(PSCloseStrategyAlways)
		mr := mockReader{data: append([]byte(test.lines), '\n')}
		cmds := make([]*Command, 0, 4)
		for {
			cmd, err := decoder.Decode(&mr)
			if cmd == nil {
				require.ErrorIs(t, err, io.EOF, "case %d", i)
				break
			}
			cmds = append(cmds, cmd)
		}
		require.Equal(t, len(test.cmdTypes), len(cmds), "case %d", i)
		for j, cmd := range cmds {
			require.Equal(t, test.cmdTypes[j], cmd.Type, "case %d", i)
		}
		if test.duplicated {
			require.NotEmpty(t, dedup.Items, "case %d", i)
		} else {
			require.Empty(t, dedup.Items, "case %d", i)
		}
	}
}

func decodeCmds(decoder CmdDecoder, reader LineReader) ([]*Command, error) {
	cmds := make([]*Command, 0, 4)
	var err error
	for {
		var cmd *Command
		cmd, err = decoder.Decode(reader)
		if cmd == nil {
			break
		}
		// do not compare kvs
		cmd.kvs = nil
		cmds = append(cmds, cmd)
	}
	return cmds, err
}

func TestFilterOutCommandsWithRetry(t *testing.T) {
	tests := []struct {
		name     string
		lines    string
		enable   bool
		cmdCount int
	}{
		{
			name:     "filter out retried query command",
			lines:    `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT * FROM test"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [RETRY=true] [EVENT=COMPLETED]`,
			enable:   true,
			cmdCount: 0,
		},
		{
			name:     "do not filter when this function is disabled",
			lines:    `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT * FROM test"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [RETRY=true] [EVENT=COMPLETED]`,
			enable:   false,
			cmdCount: 1,
		},
		{
			name:     "do not filter non-retried commands",
			lines:    `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT * FROM test"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			enable:   true,
			cmdCount: 1,
		},
		{
			name: "mixed retried and non-retried commands",
			lines: `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 1"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]
[2025/09/18 17:48:20.714 +08:10] [INFO] [logger.go:77] [ID=17581889006156] [TIMESTAMP=2025/09/18 17:48:20.714 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 2"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [RETRY=true] [EVENT=COMPLETED]
[2025/09/18 17:48:20.814 +08:10] [INFO] [logger.go:77] [ID=17581889006157] [TIMESTAMP=2025/09/18 17:48:20.814 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT 3"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [EVENT=COMPLETED]`,
			enable:   true,
			cmdCount: 2,
		},
		{
			name:     "retry flag with other value should not be filtered",
			lines:    `[2025/09/18 17:48:20.614 +08:10] [INFO] [logger.go:77] [ID=17581889006155] [TIMESTAMP=2025/09/18 17:48:20.614 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1000] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="SELECT * FROM test"] [ROWS=1] [CONNECTION_ID=1001] [CLIENT_PORT=50112] [PID=542193] [COMMAND=Query] [SQL_STATEMENTS=Select] [EXECUTE_PARAMS="[]"] [CURRENT_DB=test] [RETRY=false] [EVENT=COMPLETED]`,
			enable:   true,
			cmdCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := NewAuditLogPluginDecoder(NewDeDup(), zap.NewNop())
			if tt.enable {
				decoder.EnableFilterCommandWithRetry()
			}
			mr := mockReader{data: append([]byte(tt.lines), '\n')}
			cmds := make([]*Command, 0, 4)
			for {
				cmd, err := decoder.Decode(&mr)
				if cmd == nil {
					require.ErrorIs(t, err, io.EOF)
					break
				}
				cmds = append(cmds, cmd)
			}
			require.Equal(t, tt.cmdCount, len(cmds))
		})
	}
}
