//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import (
	"net"
	"syscall"
)

// isPeerActiveWithRawConn checks the socket state without consuming data or
// changing the connection deadline. MSG_DONTWAIT makes this probe return
// immediately when the socket has no pending data or error.
func isPeerActiveWithRawConn(conn net.Conn) (active bool, supported bool) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return false, false
	}
	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return false, false
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
			case nil, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ENOTCONN,
				syscall.ETIMEDOUT, syscall.EPIPE, syscall.ESHUTDOWN:
				active = false
			default:
				active = true
			}
		}
	})
	if err != nil {
		return false, false
	}
	return active, true
}
