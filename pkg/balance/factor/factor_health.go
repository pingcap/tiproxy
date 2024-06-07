// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

const (
	// balanceSeconds4Health indicates the time (in seconds) to migrate all the connections.
	// If some backends are unhealthy, migrate fast but do not put too much pressure on TiDB.
	balanceSeconds4Health = 5
)

var _ Factor = (*FactorHealth)(nil)

type FactorHealth struct {
	bitNum int
}

func NewFactorHealth() *FactorHealth {
	return &FactorHealth{
		bitNum: 1,
	}
}

func (fh *FactorHealth) Name() string {
	return "health"
}

func (fh *FactorHealth) UpdateScore(backends []scoredBackend) {
	for i := 0; i < len(backends); i++ {
		score := 0
		if !backends[i].Healthy() {
			score = 1
		}
		backends[i].addScore(score, fh.bitNum)
	}
}

func (fh *FactorHealth) ScoreBitNum() int {
	return fh.bitNum
}

func (fh *FactorHealth) BalanceCount(from, to scoredBackend) int {
	// Assuming that the source and target backends have similar connections at first.
	// We wish the connections to be migrated in 5 seconds but only a few are migrated in each round.
	// If we use from.ConnScore() / 5, the migration will be slower and slower.
	conns := (from.ConnScore() + to.ConnScore()) / (balanceSeconds4Health * 2)
	if conns > 0 {
		return conns
	}
	return 1
}

func (fcc *FactorHealth) SetConfig(cfg *config.Config) {
}
