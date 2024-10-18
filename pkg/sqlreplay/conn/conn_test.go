// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestConnectError(t *testing.T) {
	tests := []struct {
		connErr error
		execErr error
		tp      ExceptionType
	}{
		{
			connErr: errors.New("mock error"),
			tp:      Other,
		},
		{
			execErr: io.EOF,
			tp:      Other,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	for i, test := range tests {
		exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
		conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, &ReplayStats{})
		backendConn := &mockBackendConn{connErr: test.connErr, execErr: test.execErr}
		conn.backendConn = backendConn
		wg.RunWithRecover(func() {
			conn.Run(context.Background())
		}, nil, lg)
		if test.connErr == nil {
			conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComFieldList})
		}
		exp := <-exceptionCh
		require.Equal(t, test.tp, exp.Type(), "case %d", i)
		require.Equal(t, uint64(1), <-closeCh, "case %d", i)
		require.True(t, backendConn.close.Load(), "case %d", i)
		wg.Wait()
	}
}

func TestExecuteCmd(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, &ReplayStats{})
	backendConn := &mockBackendConn{}
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	for i := 0; i < 100; i++ {
		conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComFieldList})
	}
	require.Eventually(t, func() bool {
		return backendConn.cmds.Load() == 100
	}, 3*time.Second, time.Millisecond)
	cancel()
	wg.Wait()
}

func TestExecuteError(t *testing.T) {
	tests := []struct {
		prepare   func(*mockBackendConn) []byte
		digest    string
		queryText string
	}{
		{
			prepare: func(bc *mockBackendConn) []byte {
				return append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select 1",
		},
		{
			prepare: func(bc *mockBackendConn) []byte {
				request, err := pnet.MakeExecuteStmtRequest(1, []any{uint64(100), "abc", nil}, true)
				require.NoError(t, err)
				bc.prepareStmt = "select ?"
				bc.paramNum = 3
				return request
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select ? params=[100 abc <nil>]",
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, &ReplayStats{})
	backendConn := &mockBackendConn{execErr: errors.New("mock error")}
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	for i, test := range tests {
		request := test.prepare(backendConn)
		conn.ExecuteCmd(cmd.NewCommand(request, time.Now(), 100))
		exp := <-exceptionCh
		require.Equal(t, Fail, exp.Type(), "case %d", i)
		require.Equal(t, "mock error", exp.(*FailException).Error(), "case %d", i)
		require.Equal(t, test.digest, exp.(*FailException).command.Digest(), "case %d", i)
		require.Equal(t, test.queryText, exp.(*FailException).command.QueryText(), "case %d", i)
	}
	cancel()
	wg.Wait()
}
