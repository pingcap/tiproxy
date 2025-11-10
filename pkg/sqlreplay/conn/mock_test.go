// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tiproxy/pkg/proxy/net"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

var _ BackendConnManager = (*mockBackendConnMgr)(nil)

type mockBackendConnMgr struct {
	clientIO net.PacketIO
	closed   bool
}

func (m *mockBackendConnMgr) Connect(ctx context.Context, clientIO net.PacketIO, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config, username, password, dbName string) error {
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
	sync.Mutex
	connErr  error
	execErr  error
	close    atomic.Bool
	stmtID   uint32
	requests [][]byte
}

func newMockBackendConn() *mockBackendConn {
	return &mockBackendConn{
		requests: make([][]byte, 0, 10),
	}
}

func (c *mockBackendConn) Connect(ctx context.Context, dbName string) error {
	c.Lock()
	defer c.Unlock()
	return c.connErr
}

func (c *mockBackendConn) ConnID() uint64 {
	return 1
}

func (c *mockBackendConn) ExecuteCmd(ctx context.Context, request []byte) (resp ExecuteResp) {
	c.Lock()
	defer c.Unlock()
	c.requests = append(c.requests, request)
	switch request[0] {
	case pnet.ComStmtPrepare.Byte():
		c.stmtID++
		resp.StmtID = c.stmtID
		resp.ParamNum = 1
	}
	resp.Err = c.execErr
	return
}

func (c *mockBackendConn) Query(ctx context.Context, stmt string) error {
	c.Lock()
	defer c.Unlock()
	return c.execErr
}

func (c *mockBackendConn) PrepareStmt(ctx context.Context, stmt string) (uint32, error) {
	c.Lock()
	defer c.Unlock()
	c.stmtID++
	return c.stmtID, c.execErr
}

func (c *mockBackendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	c.Lock()
	defer c.Unlock()
	return c.execErr
}

func (c *mockBackendConn) Close() {
	c.close.Store(true)
}

func (c *mockBackendConn) setConnErr(err error) {
	c.Lock()
	defer c.Unlock()
	c.connErr = err
}

func (c *mockBackendConn) setExecErr(err error) {
	c.Lock()
	defer c.Unlock()
	c.execErr = err
}

func (c *mockBackendConn) lastReq() []byte {
	c.Lock()
	defer c.Unlock()
	if len(c.requests) == 0 {
		return nil
	}
	return c.requests[len(c.requests)-1]
}

func (c *mockBackendConn) allRequests() [][]byte {
	c.Lock()
	defer c.Unlock()
	return c.requests
}
