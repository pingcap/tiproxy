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

// IsDisconnectError returns whether the error is caused by peer disconnection.
func IsDisconnectError(err error) bool {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}
