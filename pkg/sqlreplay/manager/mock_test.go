// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"crypto/tls"
	"time"

	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
)

var _ CertManager = (*mockCertMgr)(nil)

type mockCertMgr struct {
}

func (mockCertMgr) SQLTLS() *tls.Config {
	return nil
}

var _ capture.Capture = (*mockCapture)(nil)

type mockCapture struct {
}

func (m *mockCapture) Capture(packet []byte, startTime time.Time, connID uint64) {
}

func (m *mockCapture) Close() {
}

func (m *mockCapture) Progress() (float64, error) {
	return 0, nil
}

func (m *mockCapture) Stop(err error) {
}

func (mockCapture) Start(capture.CaptureConfig) error {
	return nil
}

var _ replay.Replay = (*mockReplay)(nil)

type mockReplay struct {
}

func (m *mockReplay) Close() {
}

func (m *mockReplay) Progress() (float64, error) {
	return 0, nil
}

func (m *mockReplay) Start(cfg replay.ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error {
	return nil
}

func (m *mockReplay) Stop(err error) {
}
