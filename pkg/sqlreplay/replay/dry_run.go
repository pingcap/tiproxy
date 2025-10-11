// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
)

type nopConn struct {
	closeCh chan uint64
	connID  uint64
	stats   *conn.ReplayStats
}

func (c *nopConn) ExecuteCmd(command *cmd.Command) {
	c.stats.ReplayedCmds.Add(1)
}

func (c *nopConn) Run(ctx context.Context) {
}

func (c *nopConn) Stop() {
	c.closeCh <- c.connID
}

var _ report.Report = (*mockReport)(nil)

type mockReport struct {
	exceptionCh chan conn.Exception
}

func newMockReport(exceptionCh chan conn.Exception) *mockReport {
	return &mockReport{
		exceptionCh: exceptionCh,
	}
}

func (mr *mockReport) Start(ctx context.Context, cfg report.ReportConfig) error {
	return nil
}

func (mr *mockReport) Stop(err error) {
}

func (mr *mockReport) Close() {
}
