// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"net"
	"syscall"
	"testing"

	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSetNoDelay(t *testing.T) {
	testkit.TestTCPConn(t,
		func(*testing.T, net.Conn) {},
		func(t *testing.T, conn net.Conn) {
			require.NoError(t, SetNoDelay(conn, true))

			tcpConn, ok := conn.(*net.TCPConn)
			require.True(t, ok)
			rawConn, err := tcpConn.SyscallConn()
			require.NoError(t, err)

			var (
				value int
				serr  error
				cerr  error
			)
			cerr = rawConn.Control(func(fd uintptr) {
				value, serr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
			})
			require.NoError(t, cerr)
			require.NoError(t, serr)
			require.NotZero(t, value)
		}, 1)
}
