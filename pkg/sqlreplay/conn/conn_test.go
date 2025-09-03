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
		conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, false, &ReplayStats{})
		backendConn := newMockBackendConn()
		backendConn.connErr, backendConn.execErr = test.connErr, test.execErr
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
	stats := &ReplayStats{}
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, false, stats)
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	cmds := 1000
	for range cmds {
		conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComFieldList})
	}
	require.Eventually(t, func() bool {
		return stats.ReplayedCmds.Load() == uint64(cmds)
	}, 3*time.Second, time.Millisecond)
	require.EqualValues(t, 0, stats.PendingCmds.Load())
	cancel()
	wg.Wait()
}

func TestStopExecution(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, false, &ReplayStats{})
	conn.backendConn = newMockBackendConn()
	wg.RunWithRecover(func() {
		conn.Run(context.Background())
	}, nil, lg)
	conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComFieldList})
	conn.Stop()
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
				return append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select ?",
		},
		{
			prepare: func(bc *mockBackendConn) []byte {
				request, err := pnet.MakeExecuteStmtRequest(1, []any{uint64(100), "abc", nil}, true)
				require.NoError(t, err)
				bc.prepared[1].paramNum = 3
				return request
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select ? params=[100 abc <nil>]",
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, false, &ReplayStats{})
	backendConn := newMockBackendConn()
	backendConn.execErr = errors.New("mock error")
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

func TestSkipReadOnly(t *testing.T) {
	tests := []struct {
		cmd      *cmd.Command
		readonly bool
	}{
		{
			cmd:      &cmd.Command{Type: pnet.ComQuery, Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComQuery, Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("insert into t value(1)")...)},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtPrepare, Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtExecute, Payload: []byte{pnet.ComStmtExecute.Byte(), 1, 0, 0, 0, 0, 0, 0, 0}},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtFetch, Payload: []byte{pnet.ComStmtFetch.Byte(), 1, 0, 0, 0}},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtPrepare, Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("insert into t value(?)")...)},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtExecute, Payload: []byte{pnet.ComStmtExecute.Byte(), 2, 0, 0, 0}},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtSendLongData, Payload: []byte{pnet.ComStmtFetch.Byte(), 2, 0, 0, 0, 0, 0, 0, 0}},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComQuit},
			readonly: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	stats := &ReplayStats{}
	conn := NewConn(lg, "u1", "", nil, nil, id.NewIDManager(), 1, &backend.BCConfig{}, exceptionCh, closeCh, true, stats)
	conn.backendConn = newMockBackendConn()
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	replayedCmds, filteredCmds := uint64(0), uint64(0)
	for i, test := range tests {
		if test.readonly {
			replayedCmds++
		} else {
			filteredCmds++
		}
		conn.ExecuteCmd(test.cmd)
		require.Eventually(t, func() bool {
			return stats.ReplayedCmds.Load()+stats.FilteredCmds.Load() == replayedCmds+filteredCmds
		}, 3*time.Second, 10*time.Millisecond, "case %d", i)
		require.EqualValues(t, replayedCmds, stats.ReplayedCmds.Load(), "case %d", i)
		require.EqualValues(t, filteredCmds, stats.FilteredCmds.Load(), "case %d", i)
	}
	cancel()
	wg.Wait()
}

func TestReadOnly(t *testing.T) {
	tests := []struct {
		cmd      pnet.Command
		stmt     string
		readOnly bool
	}{
		{
			cmd:      pnet.ComQuery,
			stmt:     "select 1",
			readOnly: true,
		},
		{
			cmd:      pnet.ComQuery,
			stmt:     "insert into t value(1)",
			readOnly: false,
		},
		{
			cmd:      pnet.ComStmtPrepare,
			stmt:     "select ?",
			readOnly: true,
		},
		{
			cmd:      pnet.ComStmtPrepare,
			stmt:     "insert into t value(?)",
			readOnly: true,
		},
		{
			cmd:      pnet.ComStmtExecute,
			stmt:     "select ?",
			readOnly: true,
		},
		{
			cmd:      pnet.ComStmtExecute,
			stmt:     "insert into t value(?)",
			readOnly: false,
		},
		{
			cmd:      pnet.ComStmtClose,
			stmt:     "insert into t value(?)",
			readOnly: true,
		},
		{
			cmd:      pnet.ComQuit,
			readOnly: true,
		},
		{
			cmd:      pnet.ComCreateDB,
			readOnly: false,
		},
	}

	conn := &conn{}
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	for i, test := range tests {
		var payload []byte
		switch test.cmd {
		case pnet.ComQuery:
			payload = append([]byte{test.cmd.Byte()}, []byte(test.stmt)...)
		default:
			backendConn.prepared[1] = &preparedStmt{text: test.stmt}
			payload = []byte{test.cmd.Byte(), 1, 0, 0, 0}
		}
		command := cmd.NewCommand(payload, time.Time{}, 100)
		require.Equal(t, test.readOnly, conn.isReadOnly(command), "case %d", i)
	}
}
