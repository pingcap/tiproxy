// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBackendConn(t *testing.T) {
	backendConn := NewBackendConn(zap.NewNop(), 1, nil, &backend.BCConfig{}, nil, "u1", "")
	backendConnMgr := &mockBackendConnMgr{}
	backendConn.backendConnMgr = backendConnMgr
	require.NoError(t, backendConn.Connect(context.Background()))
	require.NoError(t, backendConn.ExecuteCmd(context.Background(), []byte{pnet.ComQuit.Byte()}))
	require.NoError(t, backendConn.Query(context.Background(), "select 1"))
	require.NoError(t, backendConnMgr.clientIO.WritePacket([]byte{pnet.OKHeader.Byte(), 1, 0, 0, 0}, true))
	stmtID, err := backendConn.PrepareStmt(context.Background(), "select ?, ?, ?")
	require.NoError(t, err)
	require.Equal(t, uint32(1), stmtID)
	require.NoError(t, backendConn.ExecuteStmt(context.Background(), 1, []any{uint64(1), "abc", float64(1.0)}))
	backendConn.ConnID()
	backendConn.Close()
	require.True(t, backendConnMgr.closed)
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
		response      []byte
		preparedStmts map[uint32]preparedStmt
	}{
		{
			request:       append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
			response:      pnet.MakeOKPacket(0, pnet.OKHeader),
			preparedStmts: map[uint32]preparedStmt{},
		},
		{
			request:  append([]byte{pnet.ComQuery.Byte()}, fmt.Appendf(nil, `set session_states '%s'`, string(b))...),
			response: pnet.MakeOKPacket(0, pnet.OKHeader),
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
			response: pnet.MakePrepareStmtResp(2, 2),
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
			response: pnet.MakeOKPacket(0, pnet.OKHeader),
			preparedStmts: map[uint32]preparedStmt{
				2: {
					text:     "insert into t values(?), (?)",
					paramNum: 2,
				},
			},
		},
		{
			request:       []byte{pnet.ComResetConnection.Byte()},
			response:      pnet.MakeOKPacket(0, pnet.OKHeader),
			preparedStmts: map[uint32]preparedStmt{},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	backendConn := NewBackendConn(lg, 1, nil, &backend.BCConfig{}, nil, "u1", "")
	for i, test := range tests {
		backendConn.updatePreparedStmts(test.request, test.response)
		require.Equal(t, test.preparedStmts, backendConn.preparedStmts, "case %d", i)
		for stmtID, ps := range test.preparedStmts {
			actual := backendConn.preparedStmts[stmtID]
			require.Equal(t, ps.text, actual.text, "case %d", i)
			require.Equal(t, ps.paramNum, actual.paramNum, "case %d", i)
		}
	}
}
