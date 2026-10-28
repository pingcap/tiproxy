// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIsPeerActiveAfterReset(t *testing.T) {
	probed := make(chan struct{})
	reset := make(chan struct{})
	testkit.TestTCPConn(t,
		func(t *testing.T, cli net.Conn) {
			reader := bufio.NewReader(cli)
			require.True(t, IsPeerActive(cli, reader))
			close(probed)
			<-reset
			require.Eventually(t, func() bool {
				return !IsPeerActive(cli, reader)
			}, time.Second, 10*time.Millisecond)
		},
		func(t *testing.T, srv net.Conn) {
			tcpConn, ok := srv.(*net.TCPConn)
			require.True(t, ok)
			<-probed
			require.NoError(t, tcpConn.SetLinger(0))
			require.NoError(t, srv.Close())
			close(reset)
		},
		1,
	)
}

func TestIsPeerActiveFallback(t *testing.T) {
	cli, srv := net.Pipe()
	reader := bufio.NewReader(cli)
	require.True(t, IsPeerActive(cli, reader))
	require.NoError(t, srv.Close())
	require.Eventually(t, func() bool {
		return !IsPeerActive(cli, reader)
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, cli.Close())
}

func TestPeerActiveFromError(t *testing.T) {
	require.True(t, peerActiveFromError(nil))
	require.True(t, peerActiveFromError(os.ErrDeadlineExceeded))
	require.True(t, peerActiveFromError(errors.New("unknown error")))
	require.False(t, peerActiveFromError(io.EOF))
	require.False(t, peerActiveFromError(io.ErrClosedPipe))
	require.False(t, peerActiveFromError(net.ErrClosed))
}
