// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package waitgroup

import (
	"testing"
	"time"

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

func TestWaitGroupPool(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	wgp := NewWaitGroupPool(5, time.Millisecond)
	for i := 0; i < 10; i++ {
		wgp.RunWithRecover(func() {
			time.Sleep(10 * time.Millisecond)
		}, nil, lg)
	}
	wgp.Wait()
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 10; i++ {
		wgp.RunWithRecover(func() {
			time.Sleep(10 * time.Millisecond)
		}, nil, lg)
	}
	wgp.Close()
}
