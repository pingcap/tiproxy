// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	"github.com/siddontang/go/hack"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
		exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 10)
		conn := NewConn(lg, ConnOpts{
			Username:       "u1",
			IdMgr:          id.NewIDManager(),
			ConnID:         1,
			UpstreamConnID: 555,
			BcConfig:       &backend.BCConfig{},
			ExceptionCh:    exceptionCh,
			CloseCh:        closeCh,
			ExecInfoCh:     execInfoCh,
			ReplayStats:    &ReplayStats{},
		})
		backendConn := newMockBackendConn()
		backendConn.setConnErr(test.connErr)
		backendConn.setExecErr(test.execErr)
		conn.backendConn = backendConn
		wg.RunWithRecover(func() {
			conn.Run(context.Background())
		}, nil, lg)
		command := &cmd.Command{ConnID: 1, UpstreamConnID: 555, Type: pnet.ComPing, Payload: []byte{pnet.ComPing.Byte()}}
		conn.ExecuteCmd(command)
		exp := <-exceptionCh
		require.Equal(t, test.tp, exp.Type(), "case %d", i)
		require.Equal(t, uint64(555), exp.ConnID(), "case %d: execution error should report UpstreamConnID", i)
		conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComQuit, Payload: []byte{pnet.ComQuit.Byte()}})

		require.Equal(t, uint64(1), <-closeCh, "case %d", i)
		require.True(t, backendConn.close.Load(), "case %d", i)
		wg.Wait()
	}
}

func TestExecuteCmd(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 2000)
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	cmds := 1000
	for range cmds {
		conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComFieldList, Payload: []byte{pnet.ComFieldList.Byte()}})
	}
	require.Eventually(t, func() bool {
		return stats.ReplayedCmds.Load() == uint64(cmds)
	}, 3*time.Second, time.Millisecond)
	require.EqualValues(t, 0, stats.PendingCmds.Load())
	require.EqualValues(t, 0, stats.ExceptionCmds.Load())
	require.Equal(t, 1000, len(execInfoCh))
	cancel()
	wg.Wait()
}

func TestStopExecution(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 10)
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    &ReplayStats{},
	})
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
		prepare   func(*conn) []byte
		digest    string
		queryText string
	}{
		{
			prepare: func(c *conn) []byte {
				return append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select 1",
		},
		{
			prepare: func(c *conn) []byte {
				return append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select ?",
		},
		{
			prepare: func(c *conn) []byte {
				request, err := pnet.MakeExecuteStmtRequest(1, []any{uint64(100), "abc", nil}, true)
				require.NoError(t, err)
				c.preparedStmts[1] = preparedStmt{text: "select ?", paramNum: 3, paramTypes: nil}
				return request
			},
			digest:    "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471",
			queryText: "select ? params=[100 abc <nil>]",
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 3*len(tests))
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	backendConn.setExecErr(errors.New("mock error"))
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	for i, test := range tests {
		request := test.prepare(conn)
		command := cmd.NewCommand(request, time.Now(), 100)
		// Set UpstreamConnID to test that exceptions report it correctly
		command.UpstreamConnID = 999
		conn.ExecuteCmd(command)
		exp := <-exceptionCh
		require.Equal(t, Fail, exp.Type(), "case %d", i)
		require.Equal(t, "mock error", exp.(*FailException).Error(), "case %d", i)
		require.Equal(t, test.digest, exp.(*FailException).command.Digest(), "case %d", i)
		require.Equal(t, test.queryText, exp.(*FailException).command.QueryText(), "case %d", i)
		require.EqualValues(t, i+1, stats.ExceptionCmds.Load(), "case %d", i)
		require.Equal(t, command, (<-execInfoCh).Command)
		// Verify exception reports UpstreamConnID (999) instead of ConnID (100)
		require.Equal(t, uint64(999), exp.ConnID(), "case %d: exception should report UpstreamConnID", i)
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
			cmd:      &cmd.Command{Type: pnet.ComStmtPrepare, CapturedPsID: 1, Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("select ?")...)},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtExecute, CapturedPsID: 1, Payload: []byte{pnet.ComStmtExecute.Byte(), 1, 0, 0, 0, 0, 0, 0, 0}},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtFetch, CapturedPsID: 1, Payload: []byte{pnet.ComStmtFetch.Byte(), 1, 0, 0, 0}},
			readonly: true,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtPrepare, CapturedPsID: 2, Payload: append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("insert into t value(?)")...)},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtExecute, CapturedPsID: 2, Payload: []byte{pnet.ComStmtExecute.Byte(), 2, 0, 0, 0}},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtSendLongData, CapturedPsID: 2, Payload: []byte{pnet.ComStmtFetch.Byte(), 2, 0, 0, 0, 0, 0, 0, 0}},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComStmtClose, CapturedPsID: 2, Payload: []byte{pnet.ComStmtClose.Byte(), 2, 0, 0, 0}},
			readonly: false,
		},
		{
			cmd:      &cmd.Command{Type: pnet.ComQuit, Payload: []byte{pnet.ComQuit.Byte()}},
			readonly: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 3*len(tests))
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
		Readonly:       true,
	})
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
			readOnly: false,
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
			cmd:      pnet.ComStmtExecute,
			stmt:     "",
			readOnly: false,
		},
		{
			cmd:      pnet.ComStmtClose,
			stmt:     "insert into t value(?)",
			readOnly: false,
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

	conn := &conn{preparedStmts: map[uint32]preparedStmt{}, psIDMapping: map[uint32]uint32{}}
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	for i, test := range tests {
		clear(conn.psIDMapping)
		var payload []byte
		switch test.cmd {
		case pnet.ComQuery, pnet.ComStmtPrepare:
			payload = append([]byte{test.cmd.Byte()}, []byte(test.stmt)...)
		case pnet.ComStmtExecute, pnet.ComStmtClose, pnet.ComStmtFetch, pnet.ComStmtReset, pnet.ComStmtSendLongData:
			prepare := cmd.NewCommand(append([]byte{pnet.ComStmtPrepare.Byte()}, []byte(test.stmt)...), time.Time{}, 100)
			if conn.isReadOnly(prepare) {
				conn.psIDMapping[1] = 1
			}
			payload = []byte{test.cmd.Byte(), 1, 0, 0, 0}
		default:
			payload = []byte{test.cmd.Byte()}
		}
		command := cmd.NewCommand(payload, time.Time{}, 100)
		require.Equal(t, test.readOnly, conn.isReadOnly(command), "case %d", i)
	}
}

func TestPreparedStmt(t *testing.T) {
	ss := sessionStates{
		PreparedStmts: map[uint32]*preparedStmtInfo{
			1: {
				StmtText:   "select ?",
				ParamTypes: []byte{0x08, 0x00},
			},
		},
	}
	b, err := json.Marshal(ss)
	require.NoError(t, err)

	tests := []struct {
		request       []byte
		response      ExecuteResp
		preparedStmts map[uint32]preparedStmt
	}{
		{
			request:       append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
			response:      ExecuteResp{},
			preparedStmts: map[uint32]preparedStmt{},
		},
		{
			request:  append([]byte{pnet.ComQuery.Byte()}, fmt.Appendf(nil, `set session_states '%s'`, string(b))...),
			response: ExecuteResp{},
			preparedStmts: map[uint32]preparedStmt{
				1: {
					text:       "select ?",
					paramNum:   1,
					paramTypes: []byte{0x08, 0x00},
				},
			},
		},
		{
			request:  append([]byte{pnet.ComStmtPrepare.Byte()}, []byte("insert into t values(?), (?)")...),
			response: ExecuteResp{StmtID: 2, ParamNum: 2},
			preparedStmts: map[uint32]preparedStmt{
				1: {
					text:       "select ?",
					paramNum:   1,
					paramTypes: []byte{0x08, 0x00},
				},
				2: {
					text:     "insert into t values(?), (?)",
					paramNum: 2,
				},
			},
		},
		{
			request:  pnet.MakeCloseStmtRequest(1),
			response: ExecuteResp{},
			preparedStmts: map[uint32]preparedStmt{
				2: {
					text:     "insert into t values(?), (?)",
					paramNum: 2,
				},
			},
		},
		{
			request:       []byte{pnet.ComResetConnection.Byte()},
			response:      ExecuteResp{},
			preparedStmts: map[uint32]preparedStmt{},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 3*len(tests))
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
		Readonly:       true,
	})
	for i, test := range tests {
		conn.updatePreparedStmts(uint32(i), test.request, test.response)
		require.Equal(t, test.preparedStmts, conn.preparedStmts, "case %d", i)
		for stmtID, ps := range test.preparedStmts {
			actual := conn.preparedStmts[stmtID]
			require.Equal(t, ps.text, actual.text, "case %d", i)
			require.Equal(t, ps.paramNum, actual.paramNum, "case %d", i)
		}
	}
}

func TestPreparedStmtMapId(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 10)
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	var wg waitgroup.WaitGroup
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	// The mapping is updated after the PREPARE statement.
	conn.ExecuteCmd(&cmd.Command{
		CapturedPsID: 100,
		Type:         pnet.ComStmtPrepare,
		Payload:      pnet.MakePrepareStmtRequest("select ?"),
	})
	require.Eventually(t, func() bool {
		return stats.PendingCmds.Load() == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, uint32(1), conn.psIDMapping[100])

	// The stmtID in the EXECUTE command is updated.
	execReq, err := pnet.MakeExecuteStmtRequest(100, []any{1}, true)
	require.NoError(t, err)
	conn.ExecuteCmd(&cmd.Command{
		CapturedPsID: 100,
		Type:         pnet.ComStmtExecute,
		Payload:      execReq,
	})
	require.Eventually(t, func() bool {
		return stats.PendingCmds.Load() == 0
	}, 3*time.Second, 10*time.Millisecond)
	stmtID := binary.LittleEndian.Uint32(backendConn.lastReq()[1:5])
	require.Equal(t, uint32(1), stmtID)

	// The stmtID in the CLOSE command is updated and the mapping is empty.
	conn.ExecuteCmd(&cmd.Command{
		CapturedPsID: 100,
		Type:         pnet.ComStmtClose,
		Payload:      pnet.MakeCloseStmtRequest(100),
	})
	require.Eventually(t, func() bool {
		return stats.PendingCmds.Load() == 0
	}, 3*time.Second, 10*time.Millisecond)
	stmtID = binary.LittleEndian.Uint32(backendConn.lastReq()[1:5])
	require.Equal(t, uint32(1), stmtID)
	require.Len(t, conn.psIDMapping, 0)

	cancel()
	wg.Wait()
}

func TestPrepareOnDemandForExecute(t *testing.T) {
	execReq, err := pnet.MakeExecuteStmtRequest(100, []any{1}, true)
	require.NoError(t, err)

	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 10)
	stats := &ReplayStats{}
	conn := NewConn(zap.NewNop(), ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 1,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn

	var wg waitgroup.WaitGroup
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, zap.NewNop())
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Send EXECUTE directly without PREPARE. It should PREPARE on demand using PreparedStmt.
	conn.ExecuteCmd(&cmd.Command{
		CapturedPsID:  100,
		Type:          pnet.ComStmtExecute,
		Payload:       execReq,
		PreparedStmt:  "select ?",
		ConnID:        1,
		UpstreamConnID: 1,
	})
	require.Eventually(t, func() bool {
		return stats.PendingCmds.Load() == 0
	}, 3*time.Second, 10*time.Millisecond)

	reqs := backendConn.allRequests()
	require.GreaterOrEqual(t, len(reqs), 2)
	require.Equal(t, pnet.ComStmtPrepare.Byte(), reqs[0][0])
	require.Equal(t, "select ?", hack.String(reqs[0][1:]))
	require.Equal(t, pnet.ComStmtExecute.Byte(), reqs[1][0])
	stmtID := binary.LittleEndian.Uint32(reqs[1][1:5])
	require.Equal(t, uint32(1), stmtID)
}

func TestReconnect(t *testing.T) {
	req, err := pnet.MakeExecuteStmtRequest(1, []any{}, true)
	require.NoError(t, err)
	tests := []struct {
		connErr    bool
		command    *cmd.Command
		exceptions int
		replayed   int
		requests   []pnet.Command
	}{
		{
			connErr: true,
			command: &cmd.Command{
				ConnID:         1,
				UpstreamConnID: 555,
				CapturedPsID:   1,
				Type:           pnet.ComStmtPrepare,
				Payload:        pnet.MakePrepareStmtRequest("select 1"),
				CurDB:          "db1",
			},
			exceptions: 1,
			requests:   []pnet.Command{},
		},
		{
			command: &cmd.Command{
				ConnID:         1,
				UpstreamConnID: 555,
				CapturedPsID:   1,
				Type:           pnet.ComStmtExecute,
				Payload:        req,
				CurDB:          "db1",
			},
			exceptions: 2,
			requests:   []pnet.Command{},
		},
		{
			command: &cmd.Command{
				ConnID:         1,
				UpstreamConnID: 555,
				Type:           pnet.ComQuery,
				Payload:        pnet.MakeQueryPacket("select 1"),
				CurDB:          "db2",
			},
			exceptions: 2,
			replayed:   1,
			requests:   []pnet.Command{pnet.ComInitDB, pnet.ComQuery},
		},
		{
			command: &cmd.Command{
				ConnID:         1,
				UpstreamConnID: 555,
				Type:           pnet.ComQuit,
				Payload:        []byte{pnet.ComQuit.Byte()},
			},
			exceptions: 2,
			replayed:   2,
			requests:   []pnet.Command{pnet.ComInitDB, pnet.ComQuery, pnet.ComQuit},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 10), make(chan uint64, 1), make(chan ExecInfo, 3*len(tests))
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 555,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	wg.RunWithRecover(func() {
		conn.Run(context.Background())
	}, nil, lg)

	for i, test := range tests {
		var connErr error
		if test.connErr {
			connErr = errors.New("conn err")
		}
		backendConn.setConnErr(connErr)
		conn.ExecuteCmd(test.command)

		require.Eventually(t, func() bool {
			return stats.ReplayedCmds.Load() == uint64(test.replayed) && stats.ExceptionCmds.Load() == uint64(test.exceptions)
		}, 3*time.Second, 10*time.Millisecond, "test %d", i)
		requests := backendConn.allRequests()
		require.Equal(t, len(test.requests), len(requests), "test %d", i)
		for j, req := range requests {
			require.Equal(t, test.requests[j].Byte(), req[0], "test %d %d", i, j)
		}
	}

	require.EqualValues(t, 1, <-closeCh)
	require.True(t, backendConn.close.Load())
	wg.Wait()
}

func TestQuitInConnect(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 10), make(chan uint64, 1), make(chan ExecInfo, 10)
	stats := &ReplayStats{}
	conn := NewConn(lg, ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 555,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn
	backendConn.connErr, backendConn.execErr = errors.New("conn err"), errors.New("exec err")
	wg.RunWithRecover(func() {
		conn.Run(context.Background())
	}, nil, lg)

	// Send a QUIT at the beginning, the QUIT is not replayed at all.
	conn.ExecuteCmd(&cmd.Command{
		ConnID:         1,
		UpstreamConnID: 555,
		Type:           pnet.ComQuit,
		Payload:        []byte{pnet.ComQuit.Byte()},
	})
	require.EqualValues(t, 1, <-closeCh)
	require.Empty(t, backendConn.allRequests())
	require.Empty(t, exceptionCh)
	require.Empty(t, execInfoCh)
	require.EqualValues(t, 0, stats.ReplayedCmds.Load())
	require.EqualValues(t, 0, stats.ExceptionCmds.Load())
	require.True(t, backendConn.close.Load())
	wg.Wait()
}

func TestExceptionUsesUpstreamConnID(t *testing.T) {
	tests := []struct {
		name           string
		connID         uint64
		upstreamConnID uint64
		setupError     func(*mockBackendConn)
		expectedType   ExceptionType
	}{
		{
			name:           "connection error with different IDs",
			connID:         100,
			upstreamConnID: 200,
			setupError: func(bc *mockBackendConn) {
				bc.setConnErr(errors.New("connection failed"))
			},
			expectedType: Other,
		},
		{
			name:           "execution error with different IDs",
			connID:         300,
			upstreamConnID: 400,
			setupError: func(bc *mockBackendConn) {
				bc.setExecErr(errors.New("execution failed"))
			},
			expectedType: Fail,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lg, _ := logger.CreateLoggerForTest(t)
			var wg waitgroup.WaitGroup
			exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 10)
			stats := &ReplayStats{}
			conn := NewConn(lg, ConnOpts{
				Username:       "u1",
				IdMgr:          id.NewIDManager(),
				ConnID:         test.connID,
				UpstreamConnID: test.upstreamConnID,
				BcConfig:       &backend.BCConfig{},
				ExceptionCh:    exceptionCh,
				CloseCh:        closeCh,
				ExecInfoCh:     execInfoCh,
				ReplayStats:    stats,
			})
			backendConn := newMockBackendConn()
			test.setupError(backendConn)
			conn.backendConn = backendConn

			childCtx, cancel := context.WithCancel(context.Background())
			wg.RunWithRecover(func() {
				conn.Run(childCtx)
			}, nil, lg)

			command := &cmd.Command{
				ConnID:         test.connID,
				UpstreamConnID: test.upstreamConnID,
				Type:           pnet.ComPing,
				Payload:        []byte{pnet.ComPing.Byte()},
			}

			conn.ExecuteCmd(command)

			exp := <-exceptionCh
			require.Equal(t, test.expectedType, exp.Type())
			require.Equal(t, test.upstreamConnID, exp.ConnID(), "Exception should report UpstreamConnID, not the allocated ConnID")

			cancel()
			wg.Wait()
		})
	}
}

func TestExecInfo(t *testing.T) {
	req, err := pnet.MakeExecuteStmtRequest(1, []any{1}, true)
	require.NoError(t, err)
	tests := []struct {
		cmd     *cmd.Command
		execErr error
		sqlText string
	}{
		{
			cmd: &cmd.Command{
				Type:    pnet.ComStmtPrepare,
				Payload: pnet.MakePrepareStmtRequest("select ?"),
			},
		},
		{
			cmd: &cmd.Command{
				Type:    pnet.ComStmtExecute,
				Payload: req,
			},
			sqlText: "select ?",
		},
		{
			cmd: &cmd.Command{
				Type:    pnet.ComQuery,
				Payload: pnet.MakeQueryPacket("select 1"),
			},
			sqlText: "select 1",
		},
		{
			cmd: &cmd.Command{
				Type:    pnet.ComQuery,
				Payload: pnet.MakeQueryPacket("select 2"),
			},
			execErr: errors.New("execution failed"),
			sqlText: "select 2",
		},
	}

	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 1)
	stats := &ReplayStats{}
	conn := NewConn(zap.NewNop(), ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 555,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	backendConn := newMockBackendConn()
	conn.backendConn = backendConn

	var wg waitgroup.WaitGroup
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, zap.NewNop())
	defer func() {
		cancel()
		wg.Wait()
	}()

	for i, test := range tests {
		backendConn.setExecErr(test.execErr)
		conn.ExecuteCmd(test.cmd)
		execInfo := <-execInfoCh
		switch test.cmd.Type {
		case pnet.ComStmtExecute:
			require.Equal(t, test.sqlText, execInfo.Command.PreparedStmt, "case %d", i)
		case pnet.ComQuery:
			require.Equal(t, test.sqlText, hack.String(test.cmd.Payload[1:]), "case %d", i)
		}
	}
}

func TestNoBlockReplay(t *testing.T) {
	exceptionCh, closeCh, execInfoCh := make(chan Exception, 1), make(chan uint64, 1), make(chan ExecInfo, 1)
	stats := &ReplayStats{}
	conn := NewConn(zap.NewNop(), ConnOpts{
		Username:       "u1",
		IdMgr:          id.NewIDManager(),
		ConnID:         1,
		UpstreamConnID: 555,
		BcConfig:       &backend.BCConfig{},
		ExceptionCh:    exceptionCh,
		CloseCh:        closeCh,
		ExecInfoCh:     execInfoCh,
		ReplayStats:    stats,
	})
	for range 100 {
		conn.onDisconnected(errors.New("conn err"))
		conn.onExecuteFailed(&cmd.Command{}, errors.New("exec err"))
	}
}
