// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"crypto/tls"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/TiProxy/lib/util/logger"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type proxyConfig struct {
	frontendTLSConfig    *tls.Config
	backendTLSConfig     *tls.Config
	handler              *CustomHandshakeHandler
	checkBackendInterval time.Duration
	sessionToken         string
	capability           pnet.Capability
	waitRedirect         bool
}

func newProxyConfig() *proxyConfig {
	return &proxyConfig{
		handler:              &CustomHandshakeHandler{},
		capability:           defaultTestBackendCapability,
		sessionToken:         mockToken,
		checkBackendInterval: CheckBackendInterval,
	}
}

type mockProxy struct {
	*BackendConnManager

	*proxyConfig
	// outputs that received from the server.
	rs *gomysql.Result
	// execution results
	err         error
	logger      *zap.Logger
	holdRequest bool
}

func newMockProxy(t *testing.T, cfg *proxyConfig) *mockProxy {
	lg, _ := logger.CreateLoggerForTest(t)
	mp := &mockProxy{
		proxyConfig: cfg,
		logger:      lg.Named("mockProxy"),
		BackendConnManager: NewBackendConnManager(lg, cfg.handler, 0, &BCConfig{
			CheckBackendInterval: cfg.checkBackendInterval,
		}),
	}
	mp.cmdProcessor.capability = cfg.capability.Uint32()
	return mp
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO *pnet.PacketIO) error {
	if err := mp.authenticator.handshakeFirstTime(mp.logger, mp, clientIO, mp.handshakeHandler, func(ctx ConnContext, auth *Authenticator, resp *pnet.HandshakeResp, timeout time.Duration) (*pnet.PacketIO, error) {
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
