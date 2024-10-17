// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"sync"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type Conn interface {
	Run(ctx context.Context)
	ExecuteCmd(command *cmd.Command)
}

type ConnCreator func(connID uint64) Conn

var _ Conn = (*conn)(nil)

type conn struct {
	cmdLock     sync.Mutex
	cmdCh       chan struct{}
	cmdList     *glist.List[*cmd.Command]
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
		cmdList:     glist.New[*cmd.Command](),
		cmdCh:       make(chan struct{}),
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
	pendingCmdNum := 0
	defer func() {
		c.lg.Info("pending cmd num", zap.Int("num", pendingCmdNum))
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.cmdCh:
			for ctx.Err() == nil {
				c.cmdLock.Lock()
				if c.cmdList.Len() > pendingCmdNum {
					pendingCmdNum = c.cmdList.Len()
				}
				command := c.cmdList.Back()
				if command != nil {
					c.cmdList.Remove(command)
				}
				c.cmdLock.Unlock()
				if command == nil {
					break
				}
				c.updateCmdForExecuteStmt(command.Value)
				if text := command.Value.QueryText(); len(text) > 0 {
					c.lg.Info("execute cmd", zap.String("cmd", text))
				}
				if err := c.backendConn.ExecuteCmd(ctx, command.Value.Payload); err != nil {
					if pnet.IsDisconnectError(err) {
						c.exceptionCh <- NewOtherException(err, c.connID)
						return
					}
					if c.updateCmdForExecuteStmt(command.Value) {
						c.exceptionCh <- NewFailException(err, command.Value)
					}
				}
				if command.Value.Type == pnet.ComQuit {
					return
				}
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
	c.cmdLock.Lock()
	defer c.cmdLock.Unlock()
	c.cmdList.PushFront(command)
	select {
	case c.cmdCh <- struct{}{}:
	default:
	}
}

func (c *conn) close() {
	c.backendConn.Close()
	c.closeCh <- c.connID
}
