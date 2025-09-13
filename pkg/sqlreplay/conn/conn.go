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
	"github.com/pingcap/tiproxy/pkg/util/lex"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

// ReplayStats record the statistics during replay. All connections share one ReplayStats and update it concurrently.
type ReplayStats struct {
	// ReplayedCmds is the number of executed commands.
	ReplayedCmds atomic.Uint64
	// PendingCmds is the number of decoded but not executed commands.
	PendingCmds atomic.Int64
	// FilteredCmds is the number of filtered commands.
	FilteredCmds atomic.Uint64
}

func (s *ReplayStats) Reset() {
	s.ReplayedCmds.Store(0)
	s.PendingCmds.Store(0)
	s.FilteredCmds.Store(0)
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
	readonly        bool
}

func NewConn(lg *zap.Logger, username, password string, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler,
	idMgr *id.IDManager, connID uint64, bcConfig *backend.BCConfig, exceptionCh chan<- Exception, closeCh chan<- uint64,
	readonly bool, replayStats *ReplayStats) *conn {
	backendConnID := idMgr.NewID()
	lg = lg.With(zap.Uint64("captureID", connID), zap.Uint64("replayID", backendConnID))
	return &conn{
		lg:          lg,
		connID:      connID,
		cmdList:     glist.New[*cmd.Command](),
		cmdCh:       make(chan struct{}, 1),
		exceptionCh: exceptionCh,
		closeCh:     closeCh,
		backendConn: NewBackendConn(lg.Named("be"), backendConnID, hsHandler, bcConfig, backendTLSConfig, username, password),
		replayStats: replayStats,
		readonly:    readonly,
	}
}

func (c *conn) Run(ctx context.Context) {
	defer c.close()
	if err := c.backendConn.Connect(ctx); err != nil {
		c.exceptionCh <- NewOtherException(err, c.connID)
		return
	}
	// context is canceled when the replay is interrupted.
	// cmdCh is closed when the replay is finished.
	finished := false
	for !finished {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-c.cmdCh:
			if !ok {
				finished = true
			}
		}
		for ctx.Err() == nil {
			c.cmdLock.Lock()
			pendingCmds := c.cmdList.Len()
			command := c.cmdList.Back()
			if command != nil {
				c.cmdList.Remove(command)
			}
			c.updatePendingCmds(pendingCmds)
			c.cmdLock.Unlock()
			if command == nil {
				break
			}
			if c.readonly {
				if !c.isReadOnly(command.Value) {
					c.replayStats.FilteredCmds.Add(1)
					continue
				}
			}
			if err := c.backendConn.ExecuteCmd(ctx, command.Value.Payload); err != nil {
				if pnet.IsDisconnectError(err) {
					c.exceptionCh <- NewOtherException(err, c.connID)
					c.lg.Debug("backend connection disconnected", zap.Error(err))
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

func (c *conn) isReadOnly(command *cmd.Command) bool {
	switch command.Type {
	case pnet.ComQuery:
		return lex.IsReadOnly(hack.String(command.Payload[1:]))
	case pnet.ComStmtExecute, pnet.ComStmtSendLongData, pnet.ComStmtReset, pnet.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(command.Payload[1:5])
		text, _, _ := c.backendConn.GetPreparedStmt(stmtID)
		if len(text) == 0 {
			c.lg.Error("prepared stmt not found", zap.Uint32("stmt_id", stmtID), zap.Stringer("cmd_type", command.Type))
			return false
		}
		return lex.IsReadOnly(text)
	case pnet.ComCreateDB, pnet.ComDropDB, pnet.ComDelayedInsert:
		return false
	}
	// Treat ComStmtPrepare and ComStmtClose as read-only to make prepared stmt IDs in capture and replay phases the same.
	// The problem is that it still requires write privilege. Better solutions are much more complex:
	// - Replace all prepared DML statements with `SELECT 1`, including ComStmtPrepare and `SET SESSION_STATES`.
	// - Remove all prepared DML statements and map catpure prepared stmt ID to replay prepared stmt ID, including ComStmtPrepare and `SET SESSION_STATES`.
	return true
}

// update the params and sql text for the ComStmtExecute for recording errors.
func (c *conn) updateCmdForExecuteStmt(command *cmd.Command) bool {
	// updated before
	if command.PreparedStmt != "" {
		return true
	}
	switch command.Type {
	case pnet.ComStmtExecute, pnet.ComStmtClose, pnet.ComStmtSendLongData, pnet.ComStmtReset, pnet.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(command.Payload[1:5])
		text, paramNum, paramTypes := c.backendConn.GetPreparedStmt(stmtID)
		if len(text) == 0 {
			c.lg.Error("prepared stmt not found", zap.Uint32("stmt_id", stmtID), zap.Stringer("cmd_type", command.Type))
			return false
		}
		if command.Type == pnet.ComStmtExecute {
			_, args, _, err := pnet.ParseExecuteStmtRequest(command.Payload, paramNum, paramTypes)
			if err != nil {
				// Failing to parse the request is not critical, so don't return false.
				c.lg.Error("parsing ComExecuteStmt request failed", zap.Uint32("stmt_id", stmtID), zap.String("sql", text),
					zap.Int("param_num", paramNum), zap.ByteString("param_types", paramTypes), zap.Error(err))
			}
			command.Params = args
		}
		command.PreparedStmt = text
	}
	return true
}

// ExecuteCmd executes a command asynchronously by adding it to the list.
// Adding commands should never block because it may cause cycle wait, so we don't use channels.
// Conn A: wait for the lock held by conn B, and then its list becomes full and blocks the replay
// Conn B: wait for next command, but the replay is blocked, so the lock won't be released
func (c *conn) ExecuteCmd(command *cmd.Command) {
	c.cmdLock.Lock()
	c.cmdList.PushFront(command)
	pendingCmds := c.cmdList.Len()
	c.updatePendingCmds(pendingCmds)
	c.cmdLock.Unlock()
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
		c.lg.Debug("backend connection closed while there are still pending commands", zap.Int("pending_cmds", c.cmdList.Len()))
	}
	c.updatePendingCmds(0)
	c.cmdLock.Unlock()
	c.backendConn.Close()
	c.closeCh <- c.connID
}
