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
			active, err := IsPeerActive(cli, reader)
			require.NoError(t, err)
			require.True(t, active)
			close(probed)
			<-reset
			require.Eventually(t, func() bool {
				active, err = IsPeerActive(cli, reader)
				return !active
			}, time.Second, 10*time.Millisecond)
			require.Error(t, err)
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
	active, err := IsPeerActive(cli, reader)
	require.NoError(t, err)
	require.True(t, active)
	require.NoError(t, srv.Close())
	require.Eventually(t, func() bool {
		active, err = IsPeerActive(cli, reader)
		return !active
	}, time.Second, 10*time.Millisecond)
	require.Error(t, err)
	require.NoError(t, cli.Close())
}

func TestPeerActiveFromError(t *testing.T) {
	for _, err := range []error{nil, os.ErrDeadlineExceeded, &net.OpError{Err: os.ErrDeadlineExceeded}} {
		active, returnedErr := peerActiveFromError(err)
		require.NoError(t, returnedErr)
		require.True(t, active)
	}
	for _, err := range []error{io.EOF, io.ErrClosedPipe, net.ErrClosed} {
		active, returnedErr := peerActiveFromError(err)
		require.ErrorIs(t, returnedErr, err)
		require.False(t, active)
	}
	unknownErr := errors.New("unknown error")
	active, returnedErr := peerActiveFromError(unknownErr)
	require.True(t, active)
	require.ErrorIs(t, returnedErr, unknownErr)
}
