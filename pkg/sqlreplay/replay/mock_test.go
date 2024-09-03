// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

var _ BackendConn = (*mockBackendConn)(nil)

type mockBackendConn struct {
	connErr error
}

func (c *mockBackendConn) Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error {
	return c.connErr
}

func (c *mockBackendConn) Close() error {
	return nil
}

var _ Conn = (*mockConn)(nil)

type mockConn struct {
	cmdCh       chan *cmd.Command
	exceptionCh chan Exception
	connID      uint64
}

func (c *mockConn) Run(ctx context.Context) {
}

func (c *mockConn) Close() {
}
