// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/lib/config"

const (
	// balanceSeconds4Status indicates the time (in seconds) to migrate all the connections.
	// A typical TiDB graceful shutdown time is 15s. The health check delay is 3s and we have at most 12s.
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
	// We wish the connections to be migrated in 5 seconds but only a few are migrated in each round.
	// Recording the balance count at the first migration will make the factor stateful and complicated.
	// We wish to calcuate the balance count only based on the parameters passed in.
	//
	// Assuming A has 28 connections and B has 0 connection. We're migrating all connections from A to B in 5s.
	// If we use from / 5 + 1, each migration will be 6, 5, 4, 3, 3, 2, 2, 1, 1, 1. It takes 10 seconds.
	// If we use (from+to) / 10 + 1, each migration will be 3, 3, 3, 3, 3, 3, 3, 3, 3, 1. It takes 10 seconds.
	// If we use the max value of the 2 formula, each migration will be 6, 5, 4, 3, 3, 3, 3, 1. It takes 8 seconds.
	// If we use max(from, to) / 5 + 1, each migration will be 6, 5, 4, 4, 4, 5. It takes 6 seconds.
	// If we use (from+to) / 5 + 1, the migration is too fast if A and B has similar connection counts at first.
	connCount := from.ConnScore()
	if to.ConnScore() > connCount {
		connCount = to.ConnScore()
	}
	return connCount/balanceSeconds4Status + 1
}

func (fs *FactorStatus) SetConfig(cfg *config.Config) {
}

func (fs *FactorStatus) Close() {
}
