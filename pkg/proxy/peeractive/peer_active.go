// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import (
	"errors"
	"net"
	"os"
	"time"
)

// Peeker reads buffered data without consuming it.
type Peeker interface {
	Peek(n int) ([]byte, error)
}

// IsPeerActive reports whether the connection has no definitive peer-close
// event. The raw socket probe is used when the platform supports it; otherwise
// the function falls back to a bounded buffered peek.
//
// It must not be called concurrently with reads or deadline changes on conn.
func IsPeerActive(conn net.Conn, peeker Peeker) bool {
	if active, supported := isPeerActiveWithRawConn(conn); supported {
		return active
	}
	if peeker == nil {
		return true
	}
	// Use fallback for other platforms.
	// It normally costs 1ms, so don't call it too frequently.
	// It may incorrectly return true if the system is extremely slow.
	if err := conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return false
	}
	_, peekErr := peeker.Peek(1)
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return false
	}
	return peekErr == nil || errors.Is(peekErr, os.ErrDeadlineExceeded)
}
