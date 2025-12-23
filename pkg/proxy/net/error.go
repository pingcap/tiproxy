// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"context"
	"io"
	"os"
	"syscall"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

var (
	ErrReadConn     = errors.New("failed to read the connection")
	ErrWriteConn    = errors.New("failed to write the connection")
	ErrFlushConn    = errors.New("failed to flush the connection")
	ErrCloseConn    = errors.New("failed to close the connection")
	ErrHandshakeTLS = errors.New("failed to complete tls handshake")
)

// IsDisconnectError returns whether the error is caused by peer disconnection.
func IsDisconnectError(err error) bool {
	switch {
	// Do not use os.Timeout(err) because it doesn't unwrap the error.
	case errors.Is(err, io.EOF), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED), errors.Is(err, syscall.ETIMEDOUT), errors.Is(err, os.ErrDeadlineExceeded),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}
