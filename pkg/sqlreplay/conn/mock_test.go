// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"sync/atomic"

	"github.com/pingcap/tiproxy/pkg/proxy/net"
)

var _ BackendConnManager = (*mockBackendConnMgr)(nil)

type mockBackendConnMgr struct {
	clientIO net.PacketIO
	closed   bool
}

func (m *mockBackendConnMgr) Connect(ctx context.Context, clientIO net.PacketIO, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config, username string, password string) error {
	m.clientIO = clientIO
	return nil
}

func (m *mockBackendConnMgr) ConnectionID() uint64 {
	return 1
}

func (m *mockBackendConnMgr) ExecuteCmd(ctx context.Context, request []byte) error {
	return nil
}

func (m *mockBackendConnMgr) Close() error {
	m.closed = true
	return m.clientIO.Close()
}

var _ BackendConn = (*mockBackendConn)(nil)

type mockBackendConn struct {
	cmds        atomic.Int32
	connErr     error
	execErr     error
	close       atomic.Bool
	prepareStmt string
	paramNum    int
}

func (c *mockBackendConn) Connect(ctx context.Context) error {
	return c.connErr
}

func (c *mockBackendConn) ConnID() uint64 {
	return 1
}

func (c *mockBackendConn) ExecuteCmd(ctx context.Context, request []byte) (err error) {
	c.cmds.Add(1)
	return c.execErr
}

func (c *mockBackendConn) Query(ctx context.Context, stmt string) error {
	c.cmds.Add(1)
	return c.execErr
}

func (c *mockBackendConn) PrepareStmt(ctx context.Context, stmt string) (uint32, error) {
	c.cmds.Add(1)
	return 1, c.execErr
}

func (c *mockBackendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	c.cmds.Add(1)
	return c.execErr
}

func (c *mockBackendConn) GetPreparedStmt(stmtID uint32) (string, int, []byte) {
	return c.prepareStmt, c.paramNum, nil
}

func (c *mockBackendConn) Close() {
	c.close.Store(true)
}
