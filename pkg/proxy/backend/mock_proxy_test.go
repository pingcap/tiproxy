// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	handler           *CustomHandshakeHandler
	bcConfig          *BCConfig
	sessionToken      string
	capability        pnet.Capability
	waitRedirect      bool
	connectionID      uint64
}

func newProxyConfig() *proxyConfig {
	return &proxyConfig{
		handler:      &CustomHandshakeHandler{},
		capability:   defaultTestBackendCapability,
		sessionToken: mockToken,
		bcConfig:     &BCConfig{},
	}
}

type mockProxy struct {
	*BackendConnManager

	*proxyConfig
	// outputs that received from the server.
	rs *mysql.Resultset
	// execution results
	err         error
	logger      *zap.Logger
	text        fmt.Stringer
	holdRequest bool
}

func newMockProxy(t *testing.T, cfg *proxyConfig) *mockProxy {
	lg, text := logger.CreateLoggerForTest(t)
	mp := &mockProxy{
		proxyConfig:        cfg,
		logger:             lg.Named("mockProxy"),
		text:               text,
		BackendConnManager: NewBackendConnManager(lg, cfg.handler, cfg.connectionID, cfg.bcConfig),
	}
	mp.cmdProcessor.capability = cfg.capability
	return mp
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO *pnet.PacketIO) error {
	if err := mp.authenticator.handshakeFirstTime(context.Background(), mp.logger, mp, clientIO, mp.handshakeHandler,
		func(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (*pnet.PacketIO, error) {
			return backendIO, nil
		}, mp.frontendTLSConfig, mp.backendTLSConfig); err != nil {
		return err
	}
	mp.cmdProcessor.capability = mp.authenticator.capability
	return nil
}

func (mp *mockProxy) authenticateSecondTime(clientIO, backendIO *pnet.PacketIO) error {
	return mp.authenticator.handshakeSecondTime(mp.logger, clientIO, backendIO, mp.backendTLSConfig, mp.sessionToken)
}

func (mp *mockProxy) processCmd(clientIO, backendIO *pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	if err != nil {
		return err
	}
	if mp.holdRequest, err = mp.cmdProcessor.executeCmd(request, clientIO, backendIO, mp.waitRedirect); err != nil {
		return err
	}
	// Pretend to redirect the held request to the new backend. The backend must respond for another loop.
	if mp.holdRequest {
		_, err = mp.cmdProcessor.executeCmd(request, clientIO, backendIO, false)
	}
	return err
}

func (mp *mockProxy) directQuery(_, backendIO *pnet.PacketIO) error {
	rs, _, err := mp.cmdProcessor.query(backendIO, mockCmdStr)
	mp.rs = rs
	return err
}
