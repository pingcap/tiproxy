// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

const (
	maxPendingCommands = 100 // pending commands for each connection
)

type BackendConn interface {
	Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error
	ExecuteCmd(ctx context.Context, request []byte) (err error)
	Close() error
}

var _ BackendConn = (*backend.BackendConnManager)(nil)

type Conn interface {
	Run(ctx context.Context)
	ExecuteCmd(command *cmd.Command)
}

var _ Conn = (*conn)(nil)

type conn struct {
	username         string
	password         string
	cmdCh            chan *cmd.Command
	exceptionCh      chan<- Exception
	closeCh          chan<- uint64
	pkt              pnet.PacketIO
	lg               *zap.Logger
	backendTLSConfig *tls.Config
	backendConn      BackendConn
	connID           uint64
}

func newConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, connID uint64,
	bcConfig *backend.BCConfig, exceptionCh chan<- Exception, closeCh chan<- uint64) *conn {
	return &conn{
		username:         username,
		password:         password,
		lg:               lg.With(zap.Uint64("connID", connID)),
		connID:           connID,
		backendTLSConfig: backendTLSConfig,
		pkt:              newPacketIO(),
		cmdCh:            make(chan *cmd.Command, maxPendingCommands),
		exceptionCh:      exceptionCh,
		closeCh:          closeCh,
		backendConn:      backend.NewBackendConnManager(lg.Named("be"), hsHandler, nil, connID, bcConfig),
	}
}

func (c *conn) Run(ctx context.Context) {
	defer c.close()
	if err := c.backendConn.Connect(ctx, c.pkt, nil, c.backendTLSConfig, c.username, c.password); err != nil {
		c.exceptionCh <- otherException{err: err, connID: c.connID}
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case command := <-c.cmdCh:
			if err := c.backendConn.ExecuteCmd(ctx, command.Payload); err != nil {
				c.exceptionCh <- failException{err: err, command: command}
				if pnet.IsDisconnectError(err) {
					return
				}
			}
		}
	}
}

// ExecuteCmd executes a command asynchronously.
func (c *conn) ExecuteCmd(command *cmd.Command) {
	select {
	case c.cmdCh <- command:
	default:
		// Discard this command to avoid block due to a bug.
		// If the discarded command is a COMMIT, let the next COMMIT finish the transaction.
		select {
		case c.exceptionCh <- otherException{err: errors.New("too many pending commands, discard command"), connID: c.connID}:
		default:
			c.lg.Warn("too many pending errors, discard error")
		}
	}
}

func (c *conn) close() {
	if err := c.backendConn.Close(); err != nil {
		c.lg.Warn("failed to close backend connection", zap.Error(err))
	}
	_ = c.pkt.Close()
	c.closeCh <- c.connID
}
