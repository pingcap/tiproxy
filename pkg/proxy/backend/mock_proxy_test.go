// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"go.uber.org/zap"
)

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	handler           *CustomHandshakeHandler
	bcConfig          *BCConfig
	meter             Meter
	capture           capture.Capture
	username          string
	password          string
	dbName            string
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
		BackendConnManager: NewBackendConnManager(lg, cfg.handler, cfg.capture, cfg.connectionID, cfg.bcConfig, cfg.meter),
	}
	mp.cmdProcessor.capability = cfg.capability
	return mp
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO pnet.PacketIO) error {
	if err := mp.authenticator.handshakeFirstTime(context.Background(), mp.logger, mp, clientIO, mp.handshakeHandler,
		func(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		}, mp.frontendTLSConfig, mp.backendTLSConfig); err != nil {
		return err
	}
	mp.cmdProcessor.capability = mp.authenticator.capability
	return nil
}

func (mp *mockProxy) authenticateSecondTime(clientIO, backendIO pnet.PacketIO) error {
	return mp.authenticator.handshakeSecondTime(mp.logger, clientIO, backendIO, mp.backendTLSConfig, mp.sessionToken)
}

func (mp *mockProxy) authenticateWithBackend(_, backendIO pnet.PacketIO) error {
	if err := mp.authenticator.handshakeWithBackend(context.Background(), mp.logger, mp, mp.handshakeHandler,
		mp.username, mp.password, mp.dbName, func(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		}, mp.backendTLSConfig); err != nil {
		return err
	}
	mp.cmdProcessor.capability = mp.authenticator.capability
	return nil
}

func (mp *mockProxy) processCmd(clientIO, backendIO pnet.PacketIO) error {
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

func (mp *mockProxy) directQuery(_, backendIO pnet.PacketIO) error {
	rs, _, err := mp.cmdProcessor.query(backendIO, mockCmdStr)
	mp.rs = rs
	return err
}

var _ capture.Capture = (*mockCapture)(nil)

type mockCapture struct {
	db        string
	initSql   string
	packet    []byte
	startTime time.Time
	connID    uint64
}

func (mc *mockCapture) Start(cfg capture.CaptureConfig) error {
	return nil
}

func (mc *mockCapture) Wait() {
}

func (mc *mockCapture) Stop(err error) {
}

func (mc *mockCapture) InitConn(startTime time.Time, connID uint64, dbname string) {
	mc.db = dbname
	mc.startTime = startTime
	mc.connID = connID
}

func (mc *mockCapture) Capture(packet []byte, startTime time.Time, connID uint64, initSession func() (string, error)) {
	mc.packet = packet
	mc.startTime = startTime
	mc.connID = connID
	if initSession != nil {
		mc.initSql, _ = initSession()
	}
}

func (mc *mockCapture) Progress() (float64, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}

func (mc *mockCapture) Close() {
}

var _ Meter = (*mockMeter)(nil)

type mockMeter struct {
	crossAZBytes map[string]int64
	respBytes    map[string]int64
}

func newMeter() *mockMeter {
	return &mockMeter{
		crossAZBytes: make(map[string]int64),
		respBytes:    make(map[string]int64),
	}
}

func (m *mockMeter) IncTraffic(clusterID string, respBytes, crossAZBytes int64) {
	m.crossAZBytes[clusterID] += crossAZBytes
	m.respBytes[clusterID] += respBytes
}
