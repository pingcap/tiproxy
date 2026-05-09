// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package waitgroup

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestWithRecoveryLog(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	metrics.ServerErrCounter.Reset()
	var wg WaitGroup
	wg.RunWithRecover(func() {
		panic("mock panic")
	}, nil, lg)
	wg.Wait()
	require.Contains(t, text.String(), "mock panic")
	counter, err := metrics.ReadCounter(metrics.ServerErrCounter.WithLabelValues("panic"))
	require.NoError(t, err)
	require.EqualValues(t, 1, counter)
}

func TestWaitGroupPool(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	metrics.ServerErrCounter.Reset()
	wgp := NewWaitGroupPool(5, time.Millisecond)
	wgp.RunWithRecover(func() {
		panic("mock panic")
	}, nil, lg)
	wgp.Wait()
	require.Contains(t, text.String(), "mock panic")
	wgp.Close()
	counter, err := metrics.ReadCounter(metrics.ServerErrCounter.WithLabelValues("panic"))
	require.NoError(t, err)
	require.EqualValues(t, 1, counter)
}
