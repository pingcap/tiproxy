// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
)

type nopConn struct {
	closeCh    chan uint64
	execInfoCh chan<- conn.ExecInfo
	connID     uint64
	stats      *conn.ReplayStats
}

func (c *nopConn) ExecuteCmd(command *cmd.Command) {
	c.stats.ReplayedCmds.Add(1)
	c.execInfoCh <- conn.ExecInfo{
		Command: command,
	}
}

func (c *nopConn) Run(ctx context.Context) {
}

func (c *nopConn) Stop() {
	c.closeCh <- c.connID
}

var _ report.Report = (*nopReport)(nil)

type nopReport struct {
	exceptionCh chan conn.Exception
	wg          waitgroup.WaitGroup
	cancel      context.CancelFunc
}

func newMockReport(exceptionCh chan conn.Exception) *nopReport {
	return &nopReport{
		exceptionCh: exceptionCh,
	}
}

func (mr *nopReport) Start(ctx context.Context, cfg report.ReportConfig) error {
	childCtx, cancel := context.WithCancel(ctx)
	mr.cancel = cancel
	mr.wg.RunWithRecover(func() { mr.loop(childCtx) }, nil, nil)
	return nil
}

func (mr *nopReport) loop(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-mr.exceptionCh:
		}
	}
}

func (mr *nopReport) Close() {
	if mr.cancel != nil {
		mr.cancel()
		mr.cancel = nil
	}
	mr.wg.Wait()
}
