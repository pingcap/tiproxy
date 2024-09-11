// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"

	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
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
	PrepareStmt(ctx context.Context, stmt string) (uint32, error)
	ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error
	Close()
}

var _ BackendConn = (*backendConn)(nil)

type backendConn struct {
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
		username:         username,
		password:         password,
		clientIO:         newPacketIO(),
		backendTLSConfig: backendTLSConfig,
		lg:               lg,
		backendConnMgr:   backend.NewBackendConnManager(lg.Named("be"), hsHandler, nil, connID, bcConfig),
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
	bc.clientIO.Reset()
	return err
}

func (bc *backendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	request, err := pnet.MakeExecuteStmtPacket(stmtID, args)
	if err != nil {
		return err
	}
	err = bc.backendConnMgr.ExecuteCmd(ctx, request)
	bc.clientIO.Reset()
	return err
}

func (bc *backendConn) PrepareStmt(ctx context.Context, stmt string) (stmtID uint32, err error) {
	request := pnet.MakePrepareStmtPacket(stmt)
	err = bc.backendConnMgr.ExecuteCmd(ctx, request)
	if err == nil {
		resp := bc.clientIO.GetResp()
		stmtID = binary.LittleEndian.Uint32(resp[1:5])
	}
	bc.clientIO.Reset()
	return
}

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
}
