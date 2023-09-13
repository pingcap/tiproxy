// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func TestWithRecovery(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	ch := make(chan error, 1)
	go WithRecovery(func() {
		panic("mock panic")
	}, func(r interface{}) {
		if r != nil {
			ch <- errors.Errorf("%v", r)
		}
	}, lg)
	require.Error(t, <-ch)
	require.Eventually(t, func() bool {
		return strings.Contains(text.String(), "mock panic")
	}, time.Second, 10*time.Millisecond)
}
