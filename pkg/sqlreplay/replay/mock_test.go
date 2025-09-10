// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/report"
)

var _ conn.Conn = (*mockConn)(nil)

type mockConn struct {
	closeCh chan uint64
	cmdCh   chan *cmd.Command
	connID  uint64
	closed  chan struct{}
}

func (c *mockConn) ExecuteCmd(command *cmd.Command) {
	if c.cmdCh != nil {
		c.cmdCh <- command
	}
}

func (c *mockConn) Run(ctx context.Context) {
	<-c.closed
	c.closeCh <- c.connID
}

func (c *mockConn) Stop() {
	c.closed <- struct{}{}
}

type mockPendingConn struct {
	closeCh     chan uint64
	connID      uint64
	closed      chan struct{}
	pendingCmds int64
	stats       *conn.ReplayStats
}

func (c *mockPendingConn) ExecuteCmd(command *cmd.Command) {
	c.pendingCmds++
	c.stats.PendingCmds.Add(1)
}

func (c *mockPendingConn) Run(ctx context.Context) {
	<-c.closed
	c.stats.PendingCmds.Add(-c.pendingCmds)
	c.closeCh <- c.connID
}

func (c *mockPendingConn) Stop() {
	c.closed <- struct{}{}
}

var _ cmd.LineReader = (*mockChLoader)(nil)

type mockChLoader struct {
	buf   bytes.Buffer
	cmdCh chan *cmd.Command
}

func newMockChLoader() *mockChLoader {
	return &mockChLoader{
		cmdCh: make(chan *cmd.Command, 1),
	}
}

func (m *mockChLoader) writeCommand(cmd *cmd.Command) {
	m.cmdCh <- cmd
}

func (m *mockChLoader) Read(data []byte) (string, int, error) {
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	for {
		_, err := m.buf.Read(data)
		if errors.Is(err, io.EOF) {
			command, ok := <-m.cmdCh
			if !ok {
				return "", 0, io.EOF
			}
			_ = encoder.Encode(command, &m.buf)
		} else {
			return "", 0, err
		}
	}
}

func (m *mockChLoader) ReadLine() ([]byte, string, int, error) {
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	for {
		line, err := m.buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			command, ok := <-m.cmdCh
			if !ok {
				return nil, "", 0, io.EOF
			}
			_ = encoder.Encode(command, &m.buf)
		} else {
			return line[:len(line)-1], "", 0, err
		}
	}
}

func (m *mockChLoader) Close() {
	close(m.cmdCh)
}

func (m *mockChLoader) String() string {
	return "mockChLoader"
}

var _ cmd.LineReader = (*mockNormalLoader)(nil)

type mockNormalLoader struct {
	buf bytes.Buffer
}

func newMockNormalLoader() *mockNormalLoader {
	return &mockNormalLoader{}
}

func (m *mockNormalLoader) writeCommand(command *cmd.Command) {
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	_ = encoder.Encode(command, &m.buf)
}

func (m *mockNormalLoader) Read(data []byte) (string, int, error) {
	_, err := m.buf.Read(data)
	return "", 0, err
}

func (m *mockNormalLoader) ReadLine() ([]byte, string, int, error) {
	line, err := m.buf.ReadBytes('\n')
	if err == nil {
		line = line[:len(line)-1]
	}
	return line, "", 0, err
}

func (m *mockNormalLoader) Close() {
}

func (m *mockNormalLoader) String() string {
	return "mockNormalLoader"
}

func newMockCommand(connID uint64) *cmd.Command {
	return &cmd.Command{
		ConnID:  connID,
		StartTs: time.Now(),
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
	}
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
