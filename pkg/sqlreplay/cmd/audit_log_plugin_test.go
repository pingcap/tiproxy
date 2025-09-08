// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
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
		kvs, err := parseLog(test.line)
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

func TestParseDB(t *testing.T) {
	tests := []struct {
		s      string
		expect []string
	}{
		{
			s:      `""`,
			expect: nil,
		},
		{
			s:      `"[]"`,
			expect: nil,
		},
		{
			s:      `"[test]"`,
			expect: []string{"test"},
		},
		{
			s:      `"[hello,world]"`,
			expect: []string{"hello", "world"},
		},
	}
	for i, test := range tests {
		dbs := parseDB(test.s)
		if len(dbs) == 0 && len(test.expect) == 0 {
			continue
		}
		require.EqualValues(t, test.expect, parseDB(test.s), "case %d", i)
	}
}

func TestDecodeAuditLogPlugin(t *testing.T) {
	tests := []struct {
		line   string
		cmd    *Command
		errMsg string
	}{
		{
			line: `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			cmd: &Command{
				Type:     pnet.ComQuery,
				ConnID:   3695181836,
				StartTs:  time.Date(2025, 9, 6, 17, 3, 53, 720000000, time.FixedZone("", 8*3600+600)),
				Payload:  append([]byte{pnet.ComQuery.Byte()}, []byte("select \"[=]\"")...),
				StmtType: "Select",
				Succeess: true,
			},
		},
		{
			// connect with an initial database
			line: `[2025/09/08 21:15:12.904 +08:00] [INFO] [logger.go:77] [ID=17573373120] [TIMESTAMP=2025/09/08 21:15:12.904 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Connected] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=49278] [CONNECTION_ID=3552575510] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=0] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			cmd: &Command{
				Type:     pnet.ComInitDB,
				ConnID:   3552575510,
				StartTs:  time.Date(2025, 9, 8, 21, 15, 12, 904000000, time.FixedZone("", 8*3600+600)),
				Payload:  append([]byte{pnet.ComInitDB.Byte()}, []byte("test")...),
				Succeess: true,
			},
		},
		{
			// no initial database
			line:   `[2025/09/08 21:15:12.904 +08:00] [INFO] [logger.go:77] [ID=17573373120] [TIMESTAMP=2025/09/08 21:15:12.904 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Connected] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=49278] [CONNECTION_ID=3552575510] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=0] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			errMsg: "EOF",
		},
		{
			line: `[2025/09/08 21:15:35.621 +08:00] [INFO] [logger.go:77] [ID=17573373350] [TIMESTAMP=2025/09/08 21:15:35.621 +08:10] [EVENT_CLASS=CONNECTION] [EVENT_SUBCLASS=Disconnect] [STATUS_CODE=0] [COST_TIME=0] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[test]"] [TABLES="[]"] [SQL_TEXT=] [ROWS=0] [CLIENT_PORT=49278] [CONNECTION_ID=3552575510] [CONNECTION_TYPE=SSL/TLS] [SERVER_ID=1] [SERVER_PORT=4000] [DURATION=22716.871792] [SERVER_OS_LOGIN_USER=test] [OS_VERSION=darwin.arm64] [CLIENT_VERSION=] [SERVER_VERSION=v9.0.0] [AUDIT_VERSION=] [SSL_VERSION=TLSv1.3] [PID=89967] [Reason=]`,
			cmd: &Command{
				Type:     pnet.ComQuit,
				ConnID:   3552575510,
				StartTs:  time.Date(2025, 9, 8, 21, 15, 35, 621000000, time.FixedZone("", 8*3600+600)),
				Payload:  []byte{pnet.ComQuit.Byte()},
				Succeess: true,
			},
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			errMsg: "no timestamp",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			errMsg: "parsing timestamp failed",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			errMsg: "no connection id",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=GENERAL] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=abc] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			errMsg: "parsing connection id failed",
		},
		{
			line:   `[2025/09/06 17:03:53.720 +08:00] [INFO] [logger.go:77] [ID=17571494330] [TIMESTAMP=2025/09/06 17:03:53.720 +08:10] [EVENT_CLASS=HELLO] [EVENT_SUBCLASS=] [STATUS_CODE=0] [COST_TIME=1336.083] [HOST=127.0.0.1] [CLIENT_IP=127.0.0.1] [USER=root] [DATABASES="[]"] [TABLES="[]"] [SQL_TEXT="select \"[=]\""] [ROWS=0] [CONNECTION_ID=3695181836] [CLIENT_PORT=63912] [PID=61215] [COMMAND=Query] [SQL_STATEMENTS=Select]`,
			errMsg: "unknown event class",
		},
	}

	decoder := NewAuditLogPluginDecoder()
	for i, test := range tests {
		mr := mockReader{data: append([]byte(test.line), '\n')}
		cmd, err := decoder.Decode(&mr)
		if len(test.errMsg) > 0 {
			require.Error(t, err, "case %d", i)
			require.ErrorContains(t, err, test.errMsg, "case %d", i)
			continue
		} else {
			require.NoError(t, err, "case %d", i)
		}
		require.Equal(t, test.cmd, cmd, "case %d", i)
	}
}
