// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

type Factor interface {
	// Name returns the name of the factor.
	Name() string
	// UpdateScore updates the score of each backend.
	UpdateScore(backends []scoredBackend)
	// ScoreBitNum returns the bit number of the score.
	ScoreBitNum() int
	// BalanceCount returns the count of connections to balance per second.
	// 0 indicates the factor is already balanced.
	BalanceCount(from, to scoredBackend) int
	SetConfig(cfg *config.Config)
}
