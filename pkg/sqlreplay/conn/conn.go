// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/id"
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
	connID      uint64 // capture ID, not replay ID
}

func NewConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler,
	idMgr *id.IDManager, connID uint64, bcConfig *backend.BCConfig, exceptionCh chan<- Exception, closeCh chan<- uint64) *conn {
	backendConnID := idMgr.NewID()
	lg = lg.With(zap.Uint64("captureID", connID), zap.Uint64("replayID", backendConnID))
	return &conn{
		lg:          lg,
		connID:      connID,
		cmdCh:       make(chan *cmd.Command, maxPendingCommands),
		exceptionCh: exceptionCh,
		closeCh:     closeCh,
		backendConn: NewBackendConn(lg.Named("be"), backendConnID, hsHandler, bcConfig, backendTLSConfig, username, password),
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
				if c.updateCmdForExecuteStmt(command) {
					c.exceptionCh <- NewFailException(err, command)
				}
				if pnet.IsDisconnectError(err) {
					return
				}
			}
		}
	}
}

func (c *conn) updateCmdForExecuteStmt(command *cmd.Command) bool {
	if command.Type == pnet.ComStmtExecute && len(command.Payload) >= 5 {
		stmtID := binary.LittleEndian.Uint32(command.Payload[1:5])
		text, paramNum := c.backendConn.GetPreparedStmt(stmtID)
		if len(text) == 0 {
			c.lg.Error("prepared stmt not found", zap.Uint32("stmt_id", stmtID))
			return false
		}
		_, args, err := pnet.ParseExecuteStmtRequest(command.Payload, paramNum)
		if err != nil {
			c.lg.Error("parse execute stmt request failed", zap.Uint32("stmt_id", stmtID), zap.Error(err))
		}
		command.PreparedStmt = text
		command.Params = args
	}
	return true
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
