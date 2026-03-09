// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
)

const frontendSlowPathThreshold = 100 * time.Millisecond

type ClientConnection struct {
	logger            *zap.Logger
	frontendTLSConfig *tls.Config    // the TLS config to connect to clients.
	backendTLSConfig  *tls.Config    // the TLS config to connect to TiDB server.
	pkt               *pnet.PacketIO // a helper to read and write data in packet format.
	connMgr           *backend.BackendConnManager
}

func NewClientConnection(logger *zap.Logger, conn net.Conn, frontendTLSConfig *tls.Config, backendTLSConfig *tls.Config,
	hsHandler backend.HandshakeHandler, connID uint64, addr string, frontendReadTimeout int, bcConfig *backend.BCConfig) *ClientConnection {
	bemgr := backend.NewBackendConnManager(logger.Named("be"), hsHandler, connID, bcConfig)
	bemgr.SetValue(backend.ConnContextKeyConnAddr, addr)
	opts := make([]pnet.PacketIOption, 0, 4)
	opts = append(opts, pnet.WithWrapError(backend.ErrClientConn))
	opts = append(opts, pnet.WithQuickAck())
	opts = append(opts, pnet.WithSlowLog("frontend", frontendSlowPathThreshold))
	if bcConfig.ProxyProtocol {
		opts = append(opts, pnet.WithProxy)
	}
	if frontendReadTimeout > 0 {
		opts = append(opts, pnet.WithReadTimeout(time.Duration(frontendReadTimeout)*time.Second))
	}
	pkt := pnet.NewPacketIO(conn, logger, bcConfig.ConnBufferSize, opts...)
	return &ClientConnection{
		logger:            logger,
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
	cc.logger.Debug("connected to backend", cc.connMgr.ConnInfo()...)
	if err = cc.processMsg(ctx); err != nil {
		msg = "fails to relay the connection"
		goto clean
	}

clean:
	src := cc.connMgr.QuitSource()
	if !src.Normal() {
		fields := cc.connMgr.ConnInfo()
		fields = append(fields, zap.Stringer("quit_source", src), zap.Error(err))
		cc.logger.Warn(msg, fields...)
	}
	metrics.DisConnCounter.WithLabelValues(src.String()).Inc()
}

func (cc *ClientConnection) processMsg(ctx context.Context) error {
	for {
		cc.pkt.ResetSequence()
		readStart := time.Now()
		clientPkt, err := cc.pkt.ReadPacket()
		readElapsed := time.Since(readStart)
		if err != nil {
			cc.connMgr.SetValue(backend.ConnContextKeyClientError, err)
			cc.connMgr.SetQuitSourceByErr(err)
			return err
		}
		execStart := time.Now()
		err = cc.connMgr.ExecuteCmd(ctx, clientPkt)
		execElapsed := time.Since(execStart)
		if err != nil {
			return err
		}
		totalElapsed := readElapsed + execElapsed
		if totalElapsed >= frontendSlowPathThreshold {
			cc.logger.Info("slow frontend command",
				zap.Stringer("cmd", pnet.Command(clientPkt[0])),
				zap.Duration("read_packet_elapsed", readElapsed),
				zap.Duration("execute_cmd_elapsed", execElapsed),
				zap.Duration("total_elapsed", totalElapsed))
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
