// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import (
	"errors"
	"io"
	"net"
	"time"
)

// Peeker reads buffered data without consuming it.
type Peeker interface {
	Peek(n int) ([]byte, error)
}

// IsPeerActive reports whether the connection has no definitive peer-close
// event. An error means that the probe could not determine the state, in which
// case active remains true. The raw socket probe is used when the platform
// supports it; otherwise the function falls back to a bounded buffered peek.
//
// It must not be called concurrently with reads or deadline changes on conn.
func IsPeerActive(conn net.Conn, peeker Peeker) (bool, error) {
	if active, determined, err := isPeerActiveWithRawConn(conn); determined {
		return active, err
	}
	if peeker == nil {
		return true, nil
	}
	// Fallback when we can't determine the peer state from the raw socket.
	// It normally costs 1ms, so don't call it too frequently.
	// It may incorrectly return true if the system is extremely slow.
	if err := conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return peerActiveFromError(err)
	}
	_, peekErr := peeker.Peek(1)
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return peerActiveFromError(err)
	}
	return peerActiveFromError(peekErr)
}

func peerActiveFromError(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return false, nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true, nil
	}
	// An unknown error does not prove that the peer is gone.
	return true, err
}
