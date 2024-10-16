// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"

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
			// ctx is canceled when the replay is finished
			return
		case command := <-c.cmdCh:
			c.updateCmdForExecuteStmt(command)
			c.lg.Info("execute cmd", zap.String("cmd", command.QueryText()))
			err := c.backendConn.ExecuteCmd(ctx, command.Payload)
			if err != nil {
				if pnet.IsDisconnectError(err) {
					c.exceptionCh <- NewOtherException(err, c.connID)
					return
				}
				if c.updateCmdForExecuteStmt(command) {
					c.exceptionCh <- NewFailException(err, command)
				}
			}
			c.lg.Info("execute cmd finished", zap.String("cmd", command.QueryText()), zap.Error(err))
			if command.Type == pnet.ComQuit {
				return
			}
		}
	}
}

func (c *conn) updateCmdForExecuteStmt(command *cmd.Command) bool {
	if command.Type == pnet.ComStmtExecute && len(command.Payload) >= 5 {
		stmtID := binary.LittleEndian.Uint32(command.Payload[1:5])
		text, paramNum, paramTypes := c.backendConn.GetPreparedStmt(stmtID)
		if len(text) == 0 {
			c.lg.Error("prepared stmt not found", zap.Uint32("stmt_id", stmtID))
			return false
		}
		_, args, _, err := pnet.ParseExecuteStmtRequest(command.Payload, paramNum, paramTypes)
		if err != nil {
			c.lg.Error("parsing ComExecuteStmt request failed", zap.Uint32("stmt_id", stmtID), zap.Error(err))
		}
		command.PreparedStmt = text
		command.Params = args
	}
	return true
}

// ExecuteCmd executes a command asynchronously.
func (c *conn) ExecuteCmd(command *cmd.Command) {
	c.cmdCh <- command
}

func (c *conn) close() {
	c.backendConn.Close()
	c.closeCh <- c.connID
}
