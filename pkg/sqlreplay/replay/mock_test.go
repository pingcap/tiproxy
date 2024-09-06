// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync/atomic"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

var _ BackendConn = (*mockBackendConn)(nil)

type mockBackendConn struct {
	cmds    atomic.Int32
	connErr error
	execErr error
	close   atomic.Bool
}

func (c *mockBackendConn) Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error {
	return c.connErr
}

func (c *mockBackendConn) ExecuteCmd(ctx context.Context, request []byte) (err error) {
	c.cmds.Add(1)
	return c.execErr
}

func (c *mockBackendConn) Close() error {
	c.close.Store(true)
	return nil
}

var _ Conn = (*mockConn)(nil)

type mockConn struct {
	exceptionCh chan Exception
	closeCh     chan uint64
	cmdCh       chan *cmd.Command
	connID      uint64
}

func (c *mockConn) ExecuteCmd(command *cmd.Command) {
	if c.cmdCh != nil {
		c.cmdCh <- command
	}
}

func (c *mockConn) Run(ctx context.Context) {
	<-ctx.Done()
	c.closeCh <- c.connID
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
	for {
		_, err := m.buf.Read(data)
		if errors.Is(err, io.EOF) {
			command, ok := <-m.cmdCh
			if !ok {
				return "", 0, io.EOF
			}
			_ = command.Encode(&m.buf)
		} else {
			return "", 0, err
		}
	}
}

func (m *mockChLoader) ReadLine() ([]byte, string, int, error) {
	for {
		line, err := m.buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			command, ok := <-m.cmdCh
			if !ok {
				return nil, "", 0, io.EOF
			}
			_ = command.Encode(&m.buf)
		} else {
			return line[:len(line)-1], "", 0, err
		}
	}
}

func (m *mockChLoader) Close() {
	close(m.cmdCh)
}

var _ cmd.LineReader = (*mockNormalLoader)(nil)

type mockNormalLoader struct {
	buf bytes.Buffer
}

func newMockNormalLoader() *mockNormalLoader {
	return &mockNormalLoader{}
}

func (m *mockNormalLoader) writeCommand(cmd *cmd.Command) {
	_ = cmd.Encode(&m.buf)
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

func newMockCommand(connID uint64) *cmd.Command {
	return &cmd.Command{
		ConnID:  connID,
		StartTs: time.Now(),
		Type:    pnet.ComQuery,
		Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
	}
}
