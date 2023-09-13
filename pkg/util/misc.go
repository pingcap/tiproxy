// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"go.uber.org/zap"
)

func WithRecovery(exec func(), recoverFn func(r interface{}), logger *zap.Logger) {
	defer func() {
		r := recover()
		if recoverFn != nil {
			recoverFn(r)
		}
		if r != nil {
			logger.Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	exec()
}
