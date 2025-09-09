// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

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
