// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
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

type mockDelayConn struct {
	closeCh  chan uint64
	connID   uint64
	cmdCount atomic.Int64
	stats    *conn.ReplayStats
	stop     atomic.Bool
}

func (c *mockDelayConn) ExecuteCmd(command *cmd.Command) {
	c.cmdCount.Add(1)
	c.stats.PendingCmds.Add(1)
}

func (c *mockDelayConn) Run(ctx context.Context) {
	for {
		if c.cmdCount.Load() > 0 {
			c.cmdCount.Add(-1)
			c.stats.ReplayedCmds.Add(1)
			c.stats.PendingCmds.Add(-1)
		}
		if c.stop.Load() && c.cmdCount.Load() == 0 {
			break
		}
		// simulate execution delay
		time.Sleep(time.Microsecond)
	}
	c.closeCh <- c.connID
}

func (c *mockDelayConn) Stop() {
	c.stop.Store(true)
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

func (m *mockNormalLoader) writeCommand(command *cmd.Command, format string) {
	encoder := cmd.NewCmdEncoder(format)
	_ = encoder.Encode(command, &m.buf)
}

func (m *mockNormalLoader) write(data []byte) {
	_, _ = m.buf.Write(data)
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

// endlessReader always returns the same line.
// The `Read` implementations is not correct, so use it only with audit log format.
type endlessReader struct {
	line string
}

func (er *endlessReader) ReadLine() ([]byte, string, int, error) {
	return []byte(er.line), "", 0, nil
}

func (er *endlessReader) Read(data []byte) (string, int, error) {
	n := copy(data, []byte(er.line))
	return "", n, nil
}

func (er *endlessReader) Close() {
}

func (er *endlessReader) String() string {
	return "endlessReader"
}

type customizedReader struct {
	buf    bytes.Buffer
	getCmd func() *cmd.Command
}

func (cr *customizedReader) ReadLine() ([]byte, string, int, error) {
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	for {
		line, err := cr.buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			command := cr.getCmd()
			if command == nil {
				return nil, "", 0, io.EOF
			}
			_ = encoder.Encode(command, &cr.buf)
		} else {
			return line[:len(line)-1], "", 0, err
		}
	}
}

func (cr *customizedReader) Read(data []byte) (string, int, error) {
	encoder := cmd.NewCmdEncoder(cmd.FormatNative)
	for {
		_, err := cr.buf.Read(data)
		if errors.Is(err, io.EOF) {
			command := cr.getCmd()
			if command == nil {
				return "", 0, io.EOF
			}
			_ = encoder.Encode(command, &cr.buf)
		} else {
			return "", 0, err
		}
	}
}

func (cr *customizedReader) Close() {
}

func (cr *customizedReader) String() string {
	return "customizedReader"
}
