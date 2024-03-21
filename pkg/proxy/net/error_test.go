// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"context"
	"os"
	"syscall"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestIsDisconnectErr(t *testing.T) {
	disConnErrors := []error{
		syscall.ETIMEDOUT,
		os.ErrDeadlineExceeded,
		context.DeadlineExceeded,
		errors.Wrap(errors.New("mock"), syscall.ETIMEDOUT),
		errors.Wrap(syscall.ETIMEDOUT, errors.New("mock")),
	}
	for _, err := range disConnErrors {
		require.True(t, IsDisconnectError(err))
	}

	otherErrors := []error{
		syscall.ENOENT,
		errors.New("mock"),
	}
	for _, err := range otherErrors {
		require.False(t, IsDisconnectError(err))
	}
}
