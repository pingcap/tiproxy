// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		payload []byte
		cmd     pnet.Command
	}{
		{
			cmd:     pnet.ComQuery,
			payload: []byte("select 1"),
		},
		{
			cmd:     pnet.ComStmtSendLongData,
			payload: []byte{0x01, 0x02, 0x03},
		},
		{
			cmd:     pnet.ComStmtSendLongData,
			payload: []byte("1\n2\n3"),
		},
		{
			cmd:     pnet.ComStmtExecute,
			payload: []byte("1\n2\n"),
		},
		{
			cmd: pnet.ComQuit,
		},
	}

	var buf bytes.Buffer
	cmds := make([]*Command, 0, len(tests))
	for i, test := range tests {
		packet := append([]byte{byte(test.cmd)}, test.payload...)
		now := time.Now()
		cmd := NewCommand(packet, now, 100)
		require.NoError(t, cmd.Encode(&buf), "case %d", i)
		cmds = append(cmds, cmd)
	}

	mr := mockReader{data: buf.Bytes()}
	for i := range tests {
		cmd := cmds[i]
		newCmd := &Command{}
		require.NoError(t, newCmd.Decode(&mr), "case %d, buf: %s", i, buf.String())
		require.True(t, cmd.Equal(newCmd), "case %d, buf: %s", i, buf.String())
	}
}

func TestDecodeError(t *testing.T) {
	tests := []string{
		`select 1`,
		`select 1
`,
		`# Time:2024-08-28T18:51:20.477067+08:00
`,
		`# Time: 100
# Conn_ID: 100
# Payload_len: 8
select 1`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: abc
# Payload_len: 8
select 1`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
# Type: abc
# Payload_len: 8
select 1`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
# Payload_len: 8
select 1`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Payload_len: 8
select 1
`,
		`# Conn_ID: 100
# Payload_len: 8
select 1
`,
		`# Conn_ID: 100
# Payload_len: 100
select 1
`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
# Payload_len: 100
select 1
`,
		`# Time: 
# Conn_ID: 100
# Payload_len: 8
select 1
`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
# Payload_len: 0
`,
		`# Time: 2024-08-28T18:51:20.477067+08:00
# Conn_ID: 100
# Payload_len: abc
`,
	}

	for _, test := range tests {
		mr := mockReader{data: []byte(test)}
		cmd := &Command{}
		require.Error(t, cmd.Decode(&mr), test)
	}
}

func TestDigest(t *testing.T) {
	cmd1 := NewCommand(append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), time.Time{}, 100)
	require.NotEmpty(t, cmd1.Digest())
	require.Equal(t, "select 1", cmd1.QueryText())

	cmd2 := NewCommand(append([]byte{pnet.ComQuery.Byte()}, []byte("select 2")...), time.Time{}, 100)
	require.Equal(t, cmd1.Digest(), cmd2.Digest())

	cmd3 := NewCommand(append([]byte{pnet.ComFieldList.Byte()}, []byte("xxx")...), time.Time{}, 100)
	require.Empty(t, cmd3.Digest())

	cmd4 := NewCommand(append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...), time.Time{}, 100)
	require.Equal(t, cmd1.Digest(), cmd4.Digest())
	require.Equal(t, "select ?", cmd4.QueryText())

	data, err := pnet.MakeExecuteStmtRequest(1, []any{1}, true)
	require.NoError(t, err)
	cmd5 := NewCommand(data, time.Time{}, 100)
	cmd5.PreparedStmt = "select ?"
	cmd5.Params = []any{1}
	require.Equal(t, cmd1.Digest(), cmd5.Digest())
	require.Equal(t, "select ? params=[1]", cmd5.QueryText())

	cmd6 := NewCommand([]byte{pnet.ComStmtFetch.Byte()}, time.Time{}, 100)
	cmd6.PreparedStmt = "select ?"
	require.Equal(t, cmd1.Digest(), cmd6.Digest())
	require.Equal(t, "select ?", cmd6.QueryText())
}

func TestReadOnly(t *testing.T) {
	tests := []struct {
		cmd         pnet.Command
		payload     []byte
		prepareStmt string
		readOnly    bool
	}{
		{
			cmd:      pnet.ComQuery,
			payload:  []byte("select 1"),
			readOnly: true,
		},
		{
			cmd:      pnet.ComQuery,
			payload:  []byte("insert into t value(1)"),
			readOnly: false,
		},
		{
			cmd:      pnet.ComStmtPrepare,
			payload:  []byte("select ?"),
			readOnly: true,
		},
		{
			cmd:      pnet.ComStmtPrepare,
			payload:  []byte("insert into t value(?)"),
			readOnly: true,
		},
		{
			cmd:         pnet.ComStmtExecute,
			prepareStmt: "select ?",
			readOnly:    true,
		},
		{
			cmd:         pnet.ComStmtExecute,
			prepareStmt: "insert into t value(?)",
			readOnly:    false,
		},
		{
			cmd:      pnet.ComStmtExecute,
			readOnly: false,
		},
		{
			cmd:         pnet.ComStmtClose,
			prepareStmt: "select ?",
			readOnly:    true,
		},
		{
			cmd:         pnet.ComStmtClose,
			prepareStmt: "insert into t value(?)",
			readOnly:    true,
		},
		{
			cmd:      pnet.ComQuit,
			readOnly: true,
		},
	}

	for i, test := range tests {
		packet := append([]byte{byte(test.cmd)}, test.payload...)
		cmd := NewCommand(packet, time.Time{}, 100)
		cmd.PreparedStmt = test.prepareStmt
		require.Equal(t, test.readOnly, cmd.ReadOnly(), "case %d", i)
	}
}
