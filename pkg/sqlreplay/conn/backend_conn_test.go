// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestBackendConn(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	backendConn := NewBackendConn(lg, 1, nil, &backend.BCConfig{}, nil, "u1", "")
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
}
