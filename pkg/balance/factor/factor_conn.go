// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

const (
	// countRatioThreshold is the threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	countRatioThreshold = 1.2
	// Narrow it to 20% in 120s, but the speed is slower and slower because the difference is getting smaller.
	// The original difference is 30%: after 120s, the difference is 23.5%.
	// The original difference is 100%: after 120s, the difference is 43.9%.
	balanceSeconds4Conn = 120
)

var _ Factor = (*FactorConnCount)(nil)

// FactorConnCount balances by connection count and must be used in following cases:
// - The policy is set to `connection`
// - The backend CPU usages are unavailable
// - The workload just starts and the backend CPU usages are low
type FactorConnCount struct {
	bitNum              int
	migrationsPerSecond float64
	countRatioThreshold float64
}

func NewFactorConnCount() *FactorConnCount {
	return &FactorConnCount{
		bitNum:              16,
		countRatioThreshold: countRatioThreshold,
	}
}

func (fcc *FactorConnCount) Name() string {
	return "conn"
}

func (fcc *FactorConnCount) UpdateScore(backends []scoredBackend) {
	for i := range backends {
		score := backends[i].ConnScore()
		if score < 0 {
			score = 0
		} else if score > 1<<fcc.bitNum-1 {
			score = 1<<fcc.bitNum - 1
		}
		backends[i].addScore(score, fcc.bitNum)
	}
}

func (fcc *FactorConnCount) ScoreBitNum() int {
	return fcc.bitNum
}

func (fcc *FactorConnCount) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	if float64(from.ConnScore()) <= float64(to.ConnScore()+1)*fcc.countRatioThreshold {
		return AdviceNeutral, 0, nil
	}
	if fcc.migrationsPerSecond > 0 {
		return AdvicePositive, fcc.migrationsPerSecond, nil
	}
	targetTo := float64(from.ConnScore()+to.ConnScore()+1) / (1 + fcc.countRatioThreshold)
	count := (targetTo - float64(to.ConnScore()+1)) / balanceSeconds4Conn
	if count < 0 {
		count = 0
	}
	return AdvicePositive, count, nil
}

func (fcc *FactorConnCount) SetConfig(cfg *config.Config) {
	fcc.migrationsPerSecond = cfg.Balance.ConnCount.MigrationsPerSecond
	fcc.countRatioThreshold = cfg.Balance.ConnCount.CountRatioThreshold
	if fcc.countRatioThreshold <= 1 {
		fcc.countRatioThreshold = countRatioThreshold
	}
}

func (fcc *FactorConnCount) CanBeRouted(_ uint64) bool {
	return true
}

func (fcc *FactorConnCount) Close() {
}
