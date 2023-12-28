// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
)

func TestStartMetricsManager(t *testing.T) {
	log, _ := logger.CreateLoggerForTest(t)
	mm := NewMetricsManager()
	mm.Init(context.Background(), log)
	mm.Close()
}
