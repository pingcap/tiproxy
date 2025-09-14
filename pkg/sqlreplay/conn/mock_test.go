// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"sync/atomic"

	"github.com/pingcap/tiproxy/pkg/proxy/net"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
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
	connErr error
	execErr error
	close   atomic.Bool
	stmtID  uint32
	lastReq []byte
}

func newMockBackendConn() *mockBackendConn {
	return &mockBackendConn{}
}

func (c *mockBackendConn) Connect(ctx context.Context) error {
	return c.connErr
}

func (c *mockBackendConn) ConnID() uint64 {
	return 1
}

func (c *mockBackendConn) ExecuteCmd(ctx context.Context, request []byte) (resp []byte, err error) {
	c.lastReq = request
	switch request[0] {
	case byte(pnet.ComStmtPrepare):
		c.stmtID++
		return pnet.MakePrepareStmtResp(c.stmtID, 1), c.execErr
	}
	return pnet.MakeOKPacket(0, pnet.OKHeader), c.execErr
}

func (c *mockBackendConn) Query(ctx context.Context, stmt string) error {
	return c.execErr
}

func (c *mockBackendConn) PrepareStmt(ctx context.Context, stmt string) (uint32, error) {
	c.stmtID++
	return c.stmtID, c.execErr
}

func (c *mockBackendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	return c.execErr
}

func (c *mockBackendConn) Close() {
	c.close.Store(true)
}
