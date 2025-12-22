// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFactorConnCount(t *testing.T) {
	factor := NewFactorConnCount()
	tests := []struct {
		connScore     int
		expectedScore uint64
	}{
		{
			connScore:     0,
			expectedScore: 0,
		},
		{
			connScore:     1,
			expectedScore: 1,
		},
		{
			connScore:     9999999,
			expectedScore: 1<<factor.bitNum - 1,
		},
	}
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backends = append(backends, newScoredBackend(newMockBackend(true, test.connScore), zap.NewNop()))
	}
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}

func TestFactorConnSpeed(t *testing.T) {
	tests := []struct {
		score1      int
		score2      int
		targetRange [2]int
	}{
		{
			score1:      120,
			score2:      100,
			targetRange: [2]int{100, 100},
		},
		{
			score1:      150,
			score2:      100,
			targetRange: [2]int{110, 114},
		},
		{
			score1:      10000,
			score2:      0,
			targetRange: [2]int{3500, 4550},
		},
	}

	factor := NewFactorConnCount()
	backend1 := newMockBackend(true, 0)
	backend2 := newMockBackend(true, 0)
	scoredBackend1 := newScoredBackend(backend1, zap.NewNop())
	scoredBackend2 := newScoredBackend(backend2, zap.NewNop())
	for i, test := range tests {
		backend1.connScore = test.score1
		backend2.connScore = test.score2
		lastRedirectTime := 0
		// Simulate rebalance for 5 minutes.
		for j := 0; j < 30000; j++ {
			advice, balanceCount, _ := factor.BalanceCount(scoredBackend1, scoredBackend2)
			if advice != AdvicePositive || balanceCount < 0.0001 {
				break
			}
			migrationInterval := 100 / balanceCount
			count := 0
			if migrationInterval < 2 {
				count = int(1 / migrationInterval)
			} else if float64(j-lastRedirectTime) >= migrationInterval {
				count = 1
			}
			if count > 0 {
				lastRedirectTime = j
				backend1.connScore -= count
				backend2.connScore += count
			}
		}
		require.GreaterOrEqual(t, backend2.connScore, test.targetRange[0], "case id: %d", i)
		require.LessOrEqual(t, backend2.connScore, test.targetRange[1], "case id: %d", i)
	}
}

func TestFactorConnConfig(t *testing.T) {
	tests := []struct {
		score1 int
		score2 int
		speed  float64
	}{
		{
			score1: 150,
			score2: 100,
			speed:  0,
		},
		{
			score1: 300,
			score2: 100,
			speed:  10,
		},
	}

	factor := NewFactorConnCount()
	cfg := config.Config{}
	cfg.Balance.ConnCount.MigrationsPerSecond = 10
	cfg.Balance.ConnCount.CountRatioThreshold = 2
	factor.SetConfig(&cfg)
	require.EqualValues(t, 10, factor.migrationsPerSecond)
	require.EqualValues(t, 2, factor.countRatioThreshold)
	backend1 := newMockBackend(true, 0)
	backend2 := newMockBackend(true, 0)
	scoredBackend1 := newScoredBackend(backend1, zap.NewNop())
	scoredBackend2 := newScoredBackend(backend2, zap.NewNop())
	for i, test := range tests {
		backend1.connScore = test.score1
		backend2.connScore = test.score2
		_, balanceCount, _ := factor.BalanceCount(scoredBackend1, scoredBackend2)
		require.EqualValues(t, test.speed, balanceCount, "case id: %d", i)
	}
}
