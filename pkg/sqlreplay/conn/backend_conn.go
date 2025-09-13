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

	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

type BackendConnManager interface {
	Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error
	ExecuteCmd(ctx context.Context, request []byte) error
	ConnectionID() uint64
	Close() error
}

var _ BackendConnManager = (*backend.BackendConnManager)(nil)

type BackendConn interface {
	Connect(ctx context.Context) error
	ConnID() uint64
	ExecuteCmd(ctx context.Context, request []byte) error
	Query(ctx context.Context, stmt string) error
	PrepareStmt(ctx context.Context, stmt string) (stmtID uint32, err error)
	ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error
	GetPreparedStmt(stmtID uint32) (text string, paramNum int, paramTypes []byte)
	Close()
}

var _ BackendConn = (*backendConn)(nil)

type backendConn struct {
	// only stores binary encoded prepared statements
	preparedStmts    map[uint32]preparedStmt
	username         string
	password         string
	clientIO         *packetIO
	backendTLSConfig *tls.Config
	lg               *zap.Logger
	backendConnMgr   BackendConnManager
}

func NewBackendConn(lg *zap.Logger, connID uint64, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig,
	backendTLSConfig *tls.Config, username, password string) *backendConn {
	return &backendConn{
		preparedStmts:    make(map[uint32]preparedStmt),
		username:         username,
		password:         password,
		clientIO:         newPacketIO(),
		backendTLSConfig: backendTLSConfig,
		lg:               lg,
		backendConnMgr:   backend.NewBackendConnManager(lg.Named("be"), hsHandler, nil, connID, bcConfig, nil),
	}
}

func (bc *backendConn) Connect(ctx context.Context) error {
	err := bc.backendConnMgr.Connect(ctx, bc.clientIO, nil, bc.backendTLSConfig, bc.username, bc.password)
	bc.clientIO.Reset()
	return err
}

func (bc *backendConn) ConnID() uint64 {
	return bc.backendConnMgr.ConnectionID()
}

func (bc *backendConn) ExecuteCmd(ctx context.Context, request []byte) error {
	err := bc.backendConnMgr.ExecuteCmd(ctx, request)
	if err == nil {
		bc.updatePreparedStmts(request, bc.clientIO.GetResp())
	}
	bc.clientIO.Reset()
	return err
}

func (bc *backendConn) updatePreparedStmts(request, response []byte) {
	switch request[0] {
	case pnet.ComStmtPrepare.Byte():
		stmtID, paramNum := pnet.ParsePrepareStmtResp(response)
		stmt := hack.String(request[1:])
		bc.preparedStmts[stmtID] = preparedStmt{text: stmt, paramNum: paramNum}
	case pnet.ComStmtExecute.Byte():
		stmtID := binary.LittleEndian.Uint32(request[1:5])
		ps, ok := bc.preparedStmts[stmtID]
		// paramNum is contained in the ComStmtPrepare while paramTypes is contained in the first ComStmtExecute.
		// Following ComStmtExecute requests will reuse the paramTypes from the first ComStmtExecute.
		if ok && ps.paramNum > 0 && len(ps.paramTypes) == 0 {
			_, _, paramTypes, err := pnet.ParseExecuteStmtRequest(request, ps.paramNum, ps.paramTypes)
			if err != nil {
				bc.lg.Error("parsing ComExecuteStmt request failed", zap.Uint32("stmt_id", stmtID), zap.Error(err))
			} else {
				ps.paramTypes = paramTypes
				bc.preparedStmts[stmtID] = ps
			}
		}
	case pnet.ComStmtClose.Byte():
		stmtID := binary.LittleEndian.Uint32(request[1:5])
		delete(bc.preparedStmts, stmtID)
	case pnet.ComChangeUser.Byte(), pnet.ComResetConnection.Byte():
		for stmtID := range bc.preparedStmts {
			delete(bc.preparedStmts, stmtID)
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
				bc.lg.Warn("failed to unmarshal session states", zap.Error(err))
			}
			for stmtID, stmt := range sessionStates.PreparedStmts {
				bc.preparedStmts[stmtID] = preparedStmt{text: stmt.StmtText, paramNum: len(stmt.ParamTypes) >> 1, paramTypes: stmt.ParamTypes}
			}
		}
	}
}

func (bc *backendConn) GetPreparedStmt(stmtID uint32) (string, int, []byte) {
	ps := bc.preparedStmts[stmtID]
	return ps.text, ps.paramNum, ps.paramTypes
}

// ExecuteStmt is only used for reportDB now.
func (bc *backendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	request, err := pnet.MakeExecuteStmtRequest(stmtID, args, true)
	if err != nil {
		return err
	}
	err = bc.backendConnMgr.ExecuteCmd(ctx, request)
	bc.clientIO.Reset()
	return err
}

// PrepareStmt is only used for reportDB now.
func (bc *backendConn) PrepareStmt(ctx context.Context, stmt string) (stmtID uint32, err error) {
	request := pnet.MakePrepareStmtRequest(stmt)
	err = bc.backendConnMgr.ExecuteCmd(ctx, request)
	if err == nil {
		resp := bc.clientIO.GetResp()
		stmtID = binary.LittleEndian.Uint32(resp[1:5])
	}
	bc.clientIO.Reset()
	return
}

// Query is only used for reportDB now.
func (bc *backendConn) Query(ctx context.Context, stmt string) error {
	request := pnet.MakeQueryPacket(stmt)
	err := bc.backendConnMgr.ExecuteCmd(ctx, request)
	bc.clientIO.Reset()
	return err
}

func (bc *backendConn) Close() {
	if err := bc.clientIO.Close(); err != nil {
		bc.lg.Warn("failed to close client connection", zap.Error(err))
	}
	if err := bc.backendConnMgr.Close(); err != nil {
		bc.lg.Warn("failed to close backend connection", zap.Error(err))
	}
}
