// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
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

func TestRedactedQueryText(t *testing.T) {
	query := NewCommand(append([]byte{pnet.ComQuery.Byte()}, []byte("select * from t where name = 'alice' and id = 10")...), time.Time{}, 1)
	require.Equal(t, "select * from t where name = 'alice' and id = 10", query.RedactedQueryText(config.RedactInfoLogOff))
	require.Equal(t, "select * from `t` where `name` = ? and `id` = ?", query.RedactedQueryText(config.RedactInfoLogOn))
	require.Equal(t, "select * from `t` where `name` = ? and `id` = ?", query.RedactedQueryText(config.RedactInfoLogType(99)))
	require.Equal(t, "select * from `t` where `name` = ‹alice› and `id` = ‹10›", query.RedactedQueryText(config.RedactInfoLogMarker))

	execute := NewCommand([]byte{pnet.ComStmtExecute.Byte()}, time.Time{}, 1)
	execute.PreparedStmt = "select * from t where id = ?"
	execute.Params = []any{int64(7), "alice", nil}
	require.Equal(t, "select * from t where id = ? params=[7 alice <nil>]", execute.RedactedQueryText(config.RedactInfoLogOff))
	require.Equal(t, "select * from `t` where `id` = ? params=[? ? ?]", execute.RedactedQueryText(config.RedactInfoLogOn))
	require.Equal(t, "select * from `t` where `id` = ? params=[‹7› ‹alice› ‹<nil>›]", execute.RedactedQueryText(config.RedactInfoLogMarker))

	execute.Params = []any{"a‹b›c"}
	require.Equal(t, "select * from `t` where `id` = ? params=[‹a‹‹b››c›]", execute.RedactedQueryText(config.RedactInfoLogMarker))

	prepare := NewCommand(append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select * from t where id = ?")...), time.Time{}, 1)
	require.Equal(t, "select * from `t` where `id` = ?", prepare.RedactedQueryText(config.RedactInfoLogOn))

	other := NewCommand([]byte{pnet.ComFieldList.Byte()}, time.Time{}, 1)
	require.Empty(t, other.RedactedQueryText(config.RedactInfoLogOn))
	require.Empty(t, (*Command)(nil).RedactedQueryText(config.RedactInfoLogOff))
}
