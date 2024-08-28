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
