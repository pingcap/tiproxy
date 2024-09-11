// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

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

type Conn interface {
	Run(ctx context.Context)
	ExecuteCmd(command *cmd.Command)
}

type ConnCreator func(connID uint64) Conn

var _ Conn = (*conn)(nil)

type conn struct {
	cmdCh       chan *cmd.Command
	exceptionCh chan<- Exception
	closeCh     chan<- uint64
	lg          *zap.Logger
	backendConn BackendConn
	connID      uint64 // frontend connection id
}

func NewConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, connID uint64,
	bcConfig *backend.BCConfig, exceptionCh chan<- Exception, closeCh chan<- uint64) *conn {
	return &conn{
		lg:          lg.With(zap.Uint64("connID", connID)),
		connID:      connID,
		cmdCh:       make(chan *cmd.Command, maxPendingCommands),
		exceptionCh: exceptionCh,
		closeCh:     closeCh,
		backendConn: NewBackendConn(lg.Named("be"), connID, hsHandler, bcConfig, backendTLSConfig, username, password),
	}
}

func (c *conn) Run(ctx context.Context) {
	defer c.close()
	if err := c.backendConn.Connect(ctx); err != nil {
		c.exceptionCh <- NewOtherException(err, c.connID)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case command := <-c.cmdCh:
			if err := c.backendConn.ExecuteCmd(ctx, command.Payload); err != nil {
				c.exceptionCh <- NewFailException(err, command)
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
		case c.exceptionCh <- NewOtherException(errors.New("too many pending commands, discard command"), c.connID):
		default:
			c.lg.Warn("too many pending errors, discard error")
		}
	}
}

func (c *conn) close() {
	c.backendConn.Close()
	c.closeCh <- c.connID
}
