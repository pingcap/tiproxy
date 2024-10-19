// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"sync"
	"sync/atomic"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type ReplayStats struct {
	ReplayedCmds atomic.Uint64
	PendingCmds  atomic.Int64
}

func (s *ReplayStats) Reset() {
	s.ReplayedCmds.Store(0)
	s.PendingCmds.Store(0)
}

type Conn interface {
	Run(ctx context.Context)
	ExecuteCmd(command *cmd.Command)
	Stop()
}

type ConnCreator func(connID uint64) Conn

var _ Conn = (*conn)(nil)

type conn struct {
	cmdLock         sync.Mutex
	cmdCh           chan struct{}
	cmdList         *glist.List[*cmd.Command]
	exceptionCh     chan<- Exception
	closeCh         chan<- uint64
	lg              *zap.Logger
	backendConn     BackendConn
	connID          uint64 // capture ID, not replay ID
	replayStats     *ReplayStats
	lastPendingCmds int // last pending cmds reported to the stats
}

func NewConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler,
	idMgr *id.IDManager, connID uint64, bcConfig *backend.BCConfig, exceptionCh chan<- Exception, closeCh chan<- uint64, replayStats *ReplayStats) *conn {
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
		replayStats: replayStats,
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
		case _, ok := <-c.cmdCh:
			if !ok {
				return
			}
			for ctx.Err() == nil {
				c.cmdLock.Lock()
				pendingCmds := c.cmdList.Len()
				command := c.cmdList.Back()
				if command != nil {
					c.cmdList.Remove(command)
				}
				c.cmdLock.Unlock()
				c.updatePendingCmds(pendingCmds)
				if command == nil {
					break
				}
				if err := c.backendConn.ExecuteCmd(ctx, command.Value.Payload); err != nil {
					if pnet.IsDisconnectError(err) {
						c.exceptionCh <- NewOtherException(err, c.connID)
						c.lg.Info("backend connection disconnected", zap.Error(err))
						return
					}
					if c.updateCmdForExecuteStmt(command.Value) {
						c.exceptionCh <- NewFailException(err, command.Value)
					}
				}
				c.replayStats.ReplayedCmds.Add(1)
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

// ExecuteCmd executes a command asynchronously by adding it to the list.
// Adding commands should never block because it may cause cycle wait.
// Conn A: wait for the lock held by conn B, and then its list becomes full and blocks the replay
// Conn B: wait for next command, but the replay is blocked, so the lock won't be released
func (c *conn) ExecuteCmd(command *cmd.Command) {
	c.cmdLock.Lock()
	c.cmdList.PushFront(command)
	pendingCmds := c.cmdList.Len()
	c.cmdLock.Unlock()
	c.updatePendingCmds(pendingCmds)
	select {
	case c.cmdCh <- struct{}{}:
	default:
	}
}

func (c *conn) Stop() {
	close(c.cmdCh)
}

func (c *conn) updatePendingCmds(pendingCmds int) {
	diff := pendingCmds - c.lastPendingCmds
	c.lastPendingCmds = pendingCmds
	if diff != 0 {
		c.replayStats.PendingCmds.Add(int64(diff))
	}
}

func (c *conn) close() {
	c.cmdLock.Lock()
	if c.cmdList.Len() > 0 {
		c.lg.Warn("backend connection closed while there are still pending commands", zap.Int("pending_cmds", c.cmdList.Len()))
	}
	c.cmdLock.Unlock()
	c.updatePendingCmds(0)
	c.backendConn.Close()
	c.closeCh <- c.connID
}
