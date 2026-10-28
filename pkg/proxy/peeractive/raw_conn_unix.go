//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// isPeerActiveWithRawConn checks the socket state without consuming data or
// changing the connection deadline. MSG_DONTWAIT makes this probe return
// immediately when the socket has no pending data or error.
func isPeerActiveWithRawConn(conn net.Conn) (active bool, determined bool, probeErr error) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return false, false, nil
	}
	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return false, false, nil
	}

	active = true
	err = rawConn.Control(func(fd uintptr) {
		var buf [1]byte
		n, _, recvErr := syscall.Recvfrom(
			int(fd),
			buf[:],
			syscall.MSG_PEEK|syscall.MSG_DONTWAIT,
		)
		if n > 0 {
			active = true
		} else {
			switch recvErr {
			// n == 0 && recvErr == nil indicates EOF
			case nil, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ENOTCONN,
				syscall.ETIMEDOUT, syscall.EPIPE, syscall.ESHUTDOWN, syscall.ENETRESET,
				syscall.EBADF:
				active = false
				if recvErr == nil {
					probeErr = io.EOF
				} else {
					probeErr = recvErr
				}
			default:
				active = true
				if !errors.Is(recvErr, syscall.EAGAIN) && !errors.Is(recvErr, syscall.EWOULDBLOCK) {
					probeErr = recvErr
				}
			}
		}
	})
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			return false, true, err
		}
		return false, false, nil
	}
	return active, true, probeErr
}
