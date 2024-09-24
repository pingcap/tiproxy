// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestFailException(t *testing.T) {
	tests := []struct {
		cmd *cmd.Command
		key string
	}{
		{
			cmd: &cmd.Command{ConnID: 1, Type: pnet.ComFieldList},
			key: "\x04",
		},
		{
			cmd: &cmd.Command{ConnID: 1, Type: pnet.ComQuery, Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)},
			key: "\x03e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
		},
		{
			cmd: &cmd.Command{ConnID: 1, Type: pnet.ComStmtPrepare, Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)},
			key: "\x16e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
		},
		{
			cmd: &cmd.Command{ConnID: 1, Type: pnet.ComStmtExecute, Payload: []byte{pnet.ComStmtExecute.Byte()}, PreparedStmt: "select ?", Params: []any{uint64(100), "abc", nil}},
			key: "\x17e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
		},
	}
	for i, test := range tests {
		exception := NewFailException(errors.New("mock error"), test.cmd)
		require.Equal(t, Fail, exception.Type(), "case %d", i)
		require.Equal(t, uint64(1), exception.ConnID(), "case %d", i)
		require.Equal(t, "mock error", exception.Error(), "case %d", i)
		require.Equal(t, test.key, exception.Key(), "case %d", i)
		require.Greater(t, exception.Time(), time.Time{}, "case %d", i)
	}
}

func TestOtherException(t *testing.T) {
	errs := []error{
		errors.New("mock error"),
		errors.Wrapf(errors.New("mock error"), "wrap"),
		errors.WithStack(errors.New("mock error")),
		errors.WithStack(errors.Wrapf(errors.New("mock error"), "wrap")),
	}
	for i, err := range errs {
		exception := NewOtherException(err, 1)
		require.Equal(t, Other, exception.Type(), "case %d", i)
		require.Equal(t, uint64(1), exception.ConnID(), "case %d", i)
		require.Equal(t, "mock error", exception.Key(), "case %d", i)
	}
}
