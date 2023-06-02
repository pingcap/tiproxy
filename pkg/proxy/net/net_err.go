// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"io"
	"syscall"

	"github.com/pingcap/TiProxy/lib/util/errors"
)

// IsDisconnectError returns whether the error is caused by peer disconnection.
func IsDisconnectError(err error) bool {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNRESET):
		return true
	}
	return false
}
