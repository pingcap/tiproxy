// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package systimemon

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestSystimeMonitor(t *testing.T) {
	errTriggered := atomic.NewBool(false)
	nowTriggered := atomic.NewBool(false)
	log, _ := logger.CreateLoggerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		StartMonitor(ctx, log,
			func() time.Time {
				if !nowTriggered.Load() {
					nowTriggered.Store(true)
					return time.Now()
				}
				return time.Now().Add(-2 * time.Second)
			}, func() {
				errTriggered.Store(true)
			}, func() {})
	})

	require.Eventually(t, errTriggered.Load, time.Second, 10*time.Millisecond)
	cancel()
	wg.Wait()
}
