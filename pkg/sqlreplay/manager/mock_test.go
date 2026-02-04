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
	progress float64
	err      error
	done     bool
}

func (m *mockCapture) InitConn(startTime time.Time, connID uint64, db string) {
}

func (m *mockCapture) Capture(stmtInfo capture.StmtInfo) {
}

func (m *mockCapture) Close() {
}

func (m *mockCapture) Progress() (float64, time.Time, bool, error) {
	return m.progress, time.Time{}, m.done, m.err
}

func (m *mockCapture) Wait() {
}

func (m *mockCapture) Stop(err error) {
	m.err = err
	m.done = true
}

func (m *mockCapture) Start(capture.CaptureConfig) error {
	m.progress = 0
	m.err = nil
	m.done = false
	return nil
}

var _ replay.Replay = (*mockReplay)(nil)

type mockReplay struct {
	progress float64
	err      error
	done     bool
}

func (m *mockReplay) Close() {
}

func (m *mockReplay) Progress() (float64, time.Time, time.Time, time.Time, bool, error) {
	return m.progress, time.Time{}, time.Time{}, time.Time{}, m.done, m.err
}

func (m *mockReplay) Wait() {
}

func (m *mockReplay) Start(cfg replay.ReplayConfig, backendTLSConfig *tls.Config, hsHandler backend.HandshakeHandler, bcConfig *backend.BCConfig) error {
	m.progress = 0
	m.err = nil
	m.done = false
	return nil
}

func (m *mockReplay) Stop(err error, _ bool) {
	m.err = err
	m.done = true
}
