// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type ClientConnection struct {
	logger            *zap.Logger
	frontendTLSConfig *tls.Config    // the TLS config to connect to clients.
	backendTLSConfig  *tls.Config    // the TLS config to connect to TiDB server.
	pkt               *pnet.PacketIO // a helper to read and write data in packet format.
	connMgr           *backend.BackendConnManager
}

func NewClientConnection(logger *zap.Logger, conn net.Conn, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config,
	hsHandler backend.HandshakeHandler, connID uint64, bcConfig *backend.BCConfig) *ClientConnection {
	bemgr := backend.NewBackendConnManager(logger.Named("be"), hsHandler, connID, bcConfig)
	opts := make([]pnet.PacketIOption, 0, 2)
	opts = append(opts, pnet.WithWrapError(backend.ErrClientConn))
	if bcConfig.ProxyProtocol {
		opts = append(opts, pnet.WithProxy)
	}
	pkt := pnet.NewPacketIO(conn, logger, opts...)
	return &ClientConnection{
		logger:            logger.With(zap.Bool("proxy-protocol", bcConfig.ProxyProtocol)),
		frontendTLSConfig: frontendTLSConfig,
		backendTLSConfig:  backendTLSConfig,
		pkt:               pkt,
		connMgr:           bemgr,
	}
}

func (cc *ClientConnection) Run(ctx context.Context) {
	var err error
	var msg string

	if err = cc.connMgr.Connect(ctx, cc.pkt, cc.frontendTLSConfig, cc.backendTLSConfig); err != nil {
		msg = "new connection failed"
		goto clean
	}
	if err = cc.processMsg(ctx); err != nil {
		msg = "fails to relay the connection"
		goto clean
	}

clean:
	src := cc.connMgr.QuitSource()
	switch src {
	case backend.SrcClientQuit, backend.SrcClientErr, backend.SrcProxyQuit:
	default:
		cc.logger.Info(msg, zap.String("backend_addr", cc.connMgr.ServerAddr()), zap.Stringer("quit source", src), zap.Error(err))
	}
}

func (cc *ClientConnection) processMsg(ctx context.Context) error {
	for {
		cc.pkt.ResetSequence()
		clientPkt, err := cc.pkt.ReadPacket()
		if err != nil {
			return err
		}
		err = cc.connMgr.ExecuteCmd(ctx, clientPkt)
		if err != nil {
			return err
		}
		if pnet.Command(clientPkt[0]) == pnet.ComQuit {
			return nil
		}
	}
}

func (cc *ClientConnection) GracefulClose() {
	cc.connMgr.GracefulClose()
}

func (cc *ClientConnection) Close() error {
	return errors.Collect(ErrCloseConn, cc.pkt.Close(), cc.connMgr.Close())
}
