// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

type Factor interface {
	// Name returns the name of the factor.
	Name() string
	// UpdateScore updates the score of each backend.
	UpdateScore(backends []scoredBackend)
	// ScoreBitNum returns the bit number of the score.
	ScoreBitNum() int
	// BalanceCount returns the count of connections to balance per second.
	// 0 indicates the factor is already balanced.
	BalanceCount(from, to scoredBackend) (float64, []zap.Field)
	SetConfig(cfg *config.Config)
	// CanBeRouted returns whether a connection can be routed or migrated to the backend with the score.
	CanBeRouted(score uint64) bool
	Close()
}
