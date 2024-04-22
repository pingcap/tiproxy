// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

const (
	// connBalancedRatio is the threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	connBalancedRatio = 1.2
	// balanceCount4Conn indicates how many connections to balance in each round.
	// This is not urgent, so migrate slowly.
	balanceCount4Conn = 1
)

var _ Factor = (*FactorConnCount)(nil)

type FactorConnCount struct {
	bitNum int
}

func NewFactorConnCount() *FactorConnCount {
	return &FactorConnCount{
		bitNum: 16,
	}
}

func (fcc *FactorConnCount) Name() string {
	return "conn"
}

func (fcc *FactorConnCount) UpdateScore(backends []scoredBackend) {
	for i := 0; i < len(backends); i++ {
		backends[i].addScore(backends[i].ConnScore(), fcc.bitNum)
	}
}

func (fcc *FactorConnCount) ScoreBitNum() int {
	return fcc.bitNum
}

func (fcc *FactorConnCount) BalanceCount(from, to scoredBackend) int {
	if float64(from.ConnScore()) > float64(to.ConnScore()+1)*connBalancedRatio {
		return balanceCount4Conn
	}
	return 0
}
