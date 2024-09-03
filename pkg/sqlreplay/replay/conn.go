// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type BackendConn interface {
	Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error
	Close() error
}

var _ BackendConn = (*backend.BackendConnManager)(nil)

type Conn interface {
	Run(ctx context.Context)
	Close()
}

var _ Conn = (*conn)(nil)

type conn struct {
	username string
	password string
	wg       waitgroup.WaitGroup
	cancel   context.CancelFunc
	// cli2SrvCh: packets sent from client to server.
	// The channel is closed when the peer is closed.
	cli2SrvCh chan []byte
	// srv2CliCh: packets sent from server to client.
	srv2CliCh        chan []byte
	cmdCh            <-chan *cmd.Command
	exceptionCh      chan<- Exception
	pkt              pnet.PacketIO
	lg               *zap.Logger
	backendTLSConfig *tls.Config
	backendConn      BackendConn
}

func newConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, connID uint64,
	bcConfig *backend.BCConfig, cmdCh <-chan *cmd.Command, exceptionCh chan<- Exception) *conn {
	cli2SrvCh, srv2CliCh := make(chan []byte, 1), make(chan []byte, 1)
	return &conn{
		username:         username,
		password:         password,
		lg:               lg,
		backendTLSConfig: backendTLSConfig,
		pkt:              newPacketIO(cli2SrvCh, srv2CliCh),
		cli2SrvCh:        cli2SrvCh,
		srv2CliCh:        srv2CliCh,
		cmdCh:            cmdCh,
		exceptionCh:      exceptionCh,
		backendConn:      backend.NewBackendConnManager(lg.Named("be"), hsHandler, nil, connID, bcConfig),
	}
}

func (c *conn) Run(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	c.wg.RunWithRecover(func() {
		select {
		case <-childCtx.Done():
			return
		case command := <-c.cmdCh:
			c.executeCmd(childCtx, command)
		}
	}, nil, c.lg)
	c.wg.RunWithRecover(func() {
		if err := c.backendConn.Connect(childCtx, c.pkt, nil, c.backendTLSConfig, c.username, c.password); err != nil {
			c.exceptionCh <- otherException{err: err}
		}
	}, nil, c.lg)
	c.cancel = cancel
}

func (c *conn) executeCmd(ctx context.Context, command *cmd.Command) {
}

func (c *conn) Close() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.wg.Wait()
	if err := c.backendConn.Close(); err != nil {
		c.lg.Warn("failed to close backend connection", zap.Error(err))
	}
	_ = c.pkt.Close()
}
