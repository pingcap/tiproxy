// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package systimemon

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(ctx context.Context, logger *zap.Logger, now func() time.Time, systimeErrHandler func(), successCallback func()) {
	logger.Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	tickCount := 0
	for {
		last := now().UnixNano()
		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
		if now().UnixNano() < last {
			logger.Error("system time jump backward", zap.Int64("last", last))
			systimeErrHandler()
		}
		// call successCallback per second.
		tickCount++
		if tickCount >= 10 {
			tickCount = 0
			successCallback()
		}
	}
}
