// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/util/errors"
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
	// TotalWaitTime is the total wait time (ns) of all commands.
	TotalWaitTime atomic.Int64
	// ExtraWaitTime is the extra wait time (ns) of replay.
	ExtraWaitTime atomic.Int64
	// ReplayStartTs is the start time (ns) of replay.
	ReplayStartTs atomic.Int64
	// The timestamp (ns) of the first command.
	FirstCmdTs atomic.Int64
	// The current decoded command timestamp.
	CurCmdTs atomic.Int64
	// The end timestamp of the current decoded command.
	CurCmdEndTs atomic.Int64
	// The number of exception commands.
	ExceptionCmds atomic.Uint64
}

func (s *ReplayStats) Reset() {
	s.ReplayedCmds.Store(0)
	s.PendingCmds.Store(0)
	s.FilteredCmds.Store(0)
	s.TotalWaitTime.Store(0)
	s.ExtraWaitTime.Store(0)
	s.ReplayStartTs.Store(0)
	s.FirstCmdTs.Store(0)
	s.CurCmdTs.Store(0)
	s.ExceptionCmds.Store(0)
}

type ExecInfo struct {
	Command   *cmd.Command
	StartTime time.Time
	CostTime  time.Duration
}

type Conn interface {
	Run(ctx context.Context)
	ExecuteCmd(command *cmd.Command)
	Stop()
}

type ConnCreator func(connID uint64, upstreamConnID uint64) Conn

var _ Conn = (*conn)(nil)

type conn struct {
	cmdLock sync.Mutex
	cmdCh   chan struct{}
	cmdList *glist.List[*cmd.Command]
	// Only stores binary encoded prepared statements. The id is the replayed ps id.
	preparedStmts map[uint32]preparedStmt
	// map capture prepared stmt ID to replay prepared stmt ID
	psIDMapping     map[uint32]uint32
	exceptionCh     chan<- Exception
	closeCh         chan<- uint64
	execInfoCh      chan<- ExecInfo
	lg              *zap.Logger
	backendConn     BackendConn
	connID          uint64 // logical connection ID, not replay ID and also not capture ID. It's the same with the `ConnID` of the first command.
	upstreamConnID  uint64 // the original upstream connection ID in capture
	replayStats     *ReplayStats
	lastPendingCmds int // last pending cmds reported to the stats
	readonly        bool
}

type ConnOpts struct {
	Username         string
	Password         string
	BackendTLSConfig *tls.Config
	HsHandler        backend.HandshakeHandler
	IdMgr            *id.IDManager
	ConnID           uint64
	UpstreamConnID   uint64
	BcConfig         *backend.BCConfig
	ExceptionCh      chan<- Exception
	CloseCh          chan<- uint64
	ExecInfoCh       chan<- ExecInfo
	ReplayStats      *ReplayStats
	Readonly         bool
}

func NewConn(lg *zap.Logger, opts ConnOpts) *conn {
	backendConnID := opts.IdMgr.NewID()
	lg = lg.With(zap.Uint64("captureID", opts.ConnID), zap.Uint64("replayID", backendConnID))
	c := &conn{
		lg:             lg,
		execInfoCh:     opts.ExecInfoCh,
		connID:         opts.ConnID,
		upstreamConnID: opts.UpstreamConnID,
		cmdList:        glist.New[*cmd.Command](),
		cmdCh:          make(chan struct{}, 1),
		preparedStmts:  make(map[uint32]preparedStmt),
		psIDMapping:    make(map[uint32]uint32),
		exceptionCh:    opts.ExceptionCh,
		closeCh:        opts.CloseCh,
		backendConn:    NewBackendConn(lg.Named("be"), backendConnID, opts.HsHandler, opts.BcConfig, opts.BackendTLSConfig, opts.Username, opts.Password),
		replayStats:    opts.ReplayStats,
		readonly:       opts.Readonly,
	}
	return c
}

func (c *conn) Run(ctx context.Context) {
	defer c.close()
	// context is canceled when the replay is interrupted.
	// cmdCh is closed when the replay is finished.
	finished := false
	connected := false
	var curDB string
	for !finished {
		select {
		case <-ctx.Done():
			// after the context is canceled, it's expected to close immediately. Don't need
			// to wait for `COM_QUIT` in this case.
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
			// Quit the connection in the next round no matter what exception happens (like disconnection).
			if command.Value.Type == pnet.ComQuit {
				finished = true
			}

			// Connect to the backend for the first time or after unexpected disconnection.
			// If the backend is upgrading, the connections may drop but the QPS should not drop too much.
			if !connected && command.Value.Type != pnet.ComQuit {
				if err := c.backendConn.Connect(ctx, command.Value.CurDB); err != nil {
					c.lg.Debug("failed to connect backend", zap.String("db", command.Value.CurDB), zap.Error(err))
					c.replayStats.ExceptionCmds.Add(1)
					c.exceptionCh <- NewOtherException(err, c.upstreamConnID)
					continue
				}
				connected = true
				curDB = command.Value.CurDB
			}

			if curDB != command.Value.CurDB && command.Value.CurDB != "" {
				// Maybe there's a USE statement already, never mind.
				if resp := c.backendConn.ExecuteCmd(ctx, pnet.MakeInitDBRequest(command.Value.CurDB)); resp.Err != nil {
					c.replayStats.ExceptionCmds.Add(1)
					c.exceptionCh <- NewFailException(resp.Err, command.Value)
					c.lg.Info("failed to use database", zap.String("db", command.Value.CurDB), zap.Error(resp.Err))
					continue
				}
				c.lg.Info("succeeded to use database", zap.String("db", command.Value.CurDB))
				curDB = command.Value.CurDB
			}
			if !c.updateExecuteStmt(command.Value) {
				c.replayStats.ExceptionCmds.Add(1)
				c.exceptionCh <- NewFailException(errors.Errorf("prepared statement ID %d not found", command.Value.CapturedPsID), command.Value)
				continue
			}
			startTime := time.Now()
			if resp := c.backendConn.ExecuteCmd(ctx, command.Value.Payload); resp.Err != nil {
				if errors.Is(resp.Err, backend.ErrClosing) || pnet.IsDisconnectError(resp.Err) {
					c.replayStats.ExceptionCmds.Add(1)
					c.exceptionCh <- NewOtherException(resp.Err, c.upstreamConnID)
					c.lg.Debug("backend connection disconnected", zap.Error(resp.Err))
					connected = false
					curDB = ""
					continue
				}
				if c.updateCmdForExecuteStmt(command.Value) {
					c.replayStats.ExceptionCmds.Add(1)
					c.exceptionCh <- NewFailException(resp.Err, command.Value)
				}
			} else {
				c.updatePreparedStmts(command.Value.CapturedPsID, command.Value.Payload, resp)
			}
			c.execInfoCh <- ExecInfo{
				Command:   command.Value,
				StartTime: startTime,
				CostTime:  time.Since(startTime),
			}
			c.replayStats.ReplayedCmds.Add(1)
		}
	}
}

func (c *conn) isReadOnly(command *cmd.Command) bool {
	switch command.Type {
	case pnet.ComQuery:
		return lex.IsReadOnly(hack.String(command.Payload[1:]))
	case pnet.ComStmtExecute, pnet.ComStmtSendLongData, pnet.ComStmtReset, pnet.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(command.Payload[1:5])
		ps := c.preparedStmts[stmtID]
		if len(ps.text) == 0 {
			// Maybe the connection is reconnected after disconnection and the prepared statements are lost.
			return false
		}
		return lex.IsReadOnly(ps.text)
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
		ps := c.preparedStmts[stmtID]
		if len(ps.text) == 0 {
			// Maybe the connection is reconnected after disconnection and the prepared statements are lost.
			return false
		}
		if command.Type == pnet.ComStmtExecute {
			_, args, _, err := pnet.ParseExecuteStmtRequest(command.Payload, ps.paramNum, ps.paramTypes)
			if err != nil {
				// Failing to parse the request is not critical, so don't return false.
				c.lg.Error("parsing ComExecuteStmt request failed", zap.Uint32("stmt_id", stmtID), zap.String("sql", ps.text),
					zap.Int("param_num", ps.paramNum), zap.ByteString("param_types", ps.paramTypes), zap.Error(err))
			}
			command.Params = args
		}
		command.PreparedStmt = ps.text
	}
	return true
}

// maintain prepared statement info so that we can find its info when:
// - Judge whether an EXECUTE command is readonly
// - Get the error message when an EXECUTE command fails
func (c *conn) updatePreparedStmts(capturedPsID uint32, request []byte, resp ExecuteResp) {
	switch request[0] {
	case pnet.ComStmtPrepare.Byte():
		stmt := hack.String(request[1:])
		c.preparedStmts[resp.StmtID] = preparedStmt{text: stmt, paramNum: resp.ParamNum}
		c.psIDMapping[capturedPsID] = resp.StmtID
	case pnet.ComStmtExecute.Byte():
		stmtID := binary.LittleEndian.Uint32(request[1:5])
		ps, ok := c.preparedStmts[stmtID]
		// paramNum is contained in the ComStmtPrepare while paramTypes is contained in the first ComStmtExecute.
		// Following ComStmtExecute requests will reuse the paramTypes from the first ComStmtExecute.
		if ok && ps.paramNum > 0 && len(ps.paramTypes) == 0 {
			_, _, paramTypes, err := pnet.ParseExecuteStmtRequest(request, ps.paramNum, ps.paramTypes)
			if err != nil {
				c.lg.Error("parsing ComExecuteStmt request failed", zap.Uint32("stmt_id", stmtID), zap.Error(err))
			} else {
				ps.paramTypes = paramTypes
				c.preparedStmts[stmtID] = ps
			}
		}
	case pnet.ComStmtClose.Byte():
		stmtID := binary.LittleEndian.Uint32(request[1:5])
		delete(c.preparedStmts, stmtID)
		delete(c.psIDMapping, capturedPsID)
	case pnet.ComChangeUser.Byte(), pnet.ComResetConnection.Byte():
		for stmtID := range c.preparedStmts {
			delete(c.preparedStmts, stmtID)
		}
	case pnet.ComQuery.Byte():
		if len(request[1:]) > len(setSessionStates) && strings.EqualFold(hack.String(request[1:len(setSessionStates)+1]), setSessionStates) {
			query := request[len(setSessionStates)+1:]
			query = bytes.TrimSpace(query)
			query = bytes.Trim(query, "'\"")
			query = bytes.ReplaceAll(query, []byte("\\\\"), []byte("\\"))
			query = bytes.ReplaceAll(query, []byte("\\'"), []byte("'"))
			var sessionStates sessionStates
			if err := json.Unmarshal(query, &sessionStates); err != nil {
				c.lg.Warn("failed to unmarshal session states", zap.Error(err))
			}
			for stmtID, stmt := range sessionStates.PreparedStmts {
				c.preparedStmts[stmtID] = preparedStmt{text: stmt.StmtText, paramNum: len(stmt.ParamTypes) >> 1, paramTypes: stmt.ParamTypes}
			}
		}
	}
}

// Update the prepared statement ID in the EXECUTE/FETCH/RESET/SEND_LONG_DATA/CLOSE command.
// If the prepared statement is not found, maybe the previous PREPARE failed or the connection
// is reconnected after disconnection, so return false and do not continue.
func (c *conn) updateExecuteStmt(command *cmd.Command) bool {
	// Native traffic replay doesn't set the CapturedPsID yet.
	if command.CapturedPsID == 0 {
		return true
	}
	switch command.Type {
	case pnet.ComStmtExecute, pnet.ComStmtFetch, pnet.ComStmtClose, pnet.ComStmtReset, pnet.ComStmtSendLongData:
		replayID, ok := c.psIDMapping[command.CapturedPsID]
		if !ok {
			return false
		}
		binary.LittleEndian.PutUint32(command.Payload[1:], replayID)
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
