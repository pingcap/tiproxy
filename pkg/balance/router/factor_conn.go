// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

const (
	// The threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	connBalancedRatio = 1.2
	// balanceCount4Conn indicates how many connections to balance in each round.
	// This is not urgent, so migrate slowly.
	balanceCount4Conn = 1
)

var _ Factor = (*FactorConnCount)(nil)

type FactorConnCount struct {
}

func NewFactorConnCount() *FactorConnCount {
	return &FactorConnCount{}
}

func (fcc *FactorConnCount) Name() string {
	return "conn"
}

func (fcc *FactorConnCount) UpdateScore(backends []*backendWrapper) {
	for _, backend := range backends {
		score := backend.connScore
		if score >= 1<<16 {
			score = 1<<16 - 1
		}
		backend.addScore(score, 16)
	}
}

func (fcc *FactorConnCount) ScoreBitNum() int {
	return 16
}

func (fcc *FactorConnCount) BalanceCount(from, to *backendWrapper) int {
	// If CPU factor is disabled, we need connCount factor.
	// If CPU factor is enabled, we don't need connCount factor.
	// If label factor is also enabled, we'd better disable connCount factor.
	if float64(from.connScore) > float64(to.connScore+1)*connBalancedRatio {
		return balanceCount4Conn
	}
	return 0
}
