// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

const (
	// balanceCount4Health indicates how many connections to balance in each round.
	// If some backends are unhealthy, migrate fast but do not put too much pressure on TiDB.
	balanceCount4Health = 10
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

func (fh *FactorHealth) UpdateScore(backends []*backendWrapper) {
	for _, backend := range backends {
		score := 0
		if !backend.Healthy() {
			score = 1
		}
		backend.addScore(score, fh.bitNum)
	}
}

func (fh *FactorHealth) ScoreBitNum() int {
	return fh.bitNum
}

func (fh *FactorHealth) BalanceCount(from, to *backendWrapper) int {
	return balanceCount4Health
}
