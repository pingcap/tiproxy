// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"net"
	"strconv"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func TestPipeConn(t *testing.T, a, b func(*testing.T, net.Conn), loop int) {
	var wg waitgroup.WaitGroup
	cli, srv := net.Pipe()
	if ddl, ok := t.Deadline(); ok {
		require.NoError(t, cli.SetDeadline(ddl))
		require.NoError(t, srv.SetDeadline(ddl))
	}
	for i := 0; i < loop; i++ {
		wg.Run(func() {
			a(t, cli)
			require.NoError(t, cli.Close())
		})
		wg.Run(func() {
			b(t, srv)
			require.NoError(t, srv.Close())
		})
		wg.Wait()
	}
}

func TestTCPConn(t *testing.T, a, b func(*testing.T, net.Conn), loop int) {
	TestTCPConnWithListener(t, func(t *testing.T, network, addr string) net.Listener {
		ln, err := net.Listen(network, addr)
		require.NoError(t, err)
		return ln
	}, a, b, loop)
}

func TestTCPConnWithListener(t *testing.T, listen func(*testing.T, string, string) net.Listener, a, b func(*testing.T, net.Conn), loop int) {
	listener := listen(t, "tcp", "localhost:0")
	defer func() {
		require.NoError(t, listener.Close())
	}()
	var wg waitgroup.WaitGroup
	for i := 0; i < loop; i++ {
		wg.Run(func() {
			cli, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)
			if ddl, ok := t.Deadline(); ok {
				require.NoError(t, cli.SetDeadline(ddl))
			}
			a(t, cli)
			if err := cli.Close(); err != nil {
				require.ErrorIs(t, err, net.ErrClosed)
			}
		})
		wg.Run(func() {
			srv, err := listener.Accept()
			require.NoError(t, err)
			if ddl, ok := t.Deadline(); ok {
				require.NoError(t, srv.SetDeadline(ddl))
			}
			b(t, srv)
			if err := srv.Close(); err != nil {
				require.ErrorIs(t, err, net.ErrClosed)
			}
		})
		wg.Wait()
	}
}

func StartListener(t *testing.T, addr string) (net.Listener, string) {
	if len(addr) == 0 {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return listener, listener.Addr().String()
}

func ParseHostPort(t *testing.T, addr string) (string, uint) {
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.ParseUint(port, 10, 32)
	require.NoError(t, err)
	return host, uint(p)
}
