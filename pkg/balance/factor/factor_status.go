// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

const (
	// balanceSeconds4Status indicates the time (in seconds) to migrate all the connections.
	// If some backends are down, migrate fast but do not put too much pressure on TiDB.
	balanceSeconds4Status = 5
)

var _ Factor = (*FactorStatus)(nil)

type FactorStatus struct {
	bitNum int
}

func NewFactorStatus() *FactorStatus {
	return &FactorStatus{
		bitNum: 1,
	}
}

func (fs *FactorStatus) Name() string {
	return "status"
}

func (fs *FactorStatus) UpdateScore(backends []scoredBackend) {
	for i := 0; i < len(backends); i++ {
		score := 0
		if !backends[i].Healthy() {
			score = 1
		}
		backends[i].addScore(score, fs.bitNum)
	}
}

func (fs *FactorStatus) ScoreBitNum() int {
	return fs.bitNum
}

func (fs *FactorStatus) BalanceCount(from, to scoredBackend) int {
	// Assuming that the source and target backends have similar connections at first.
	// We wish the connections to be migrated in 5 seconds but only a few are migrated in each round.
	// If we use from.ConnScore() / 5, the migration will be slower and slower.
	conns := (from.ConnScore() + to.ConnScore()) / (balanceSeconds4Status * 2)
	if conns > 0 {
		return conns
	}
	return 1
}

func (fs *FactorStatus) SetConfig(cfg *config.Config) {
}

func (fs *FactorStatus) Close() {
}
