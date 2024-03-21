// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package waitgroup

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

func TestWithRecoveryLog(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	var wg WaitGroup
	wg.RunWithRecover(func() {
		panic("mock panic")
	}, nil, lg)
	wg.Wait()
	require.Contains(t, text.String(), "mock panic")
}

func TestWithRecoveryPanic(t *testing.T) {
	var wg WaitGroup
	wg.RunWithRecover(func() {
		panic("mock panic1")
	}, func(r interface{}) {
		panic("mock panic2")
	}, nil)
	wg.Wait()
}
