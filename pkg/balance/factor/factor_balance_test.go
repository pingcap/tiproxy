// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRouteWithOneFactor(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor := &mockFactor{bitNum: 8, balanceCount: 1, threshold: 1, canBeRouted: true}
	fm.factors = []Factor{factor}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores   []int
		idxRange []int
	}{
		{
			scores:   []int{10},
			idxRange: []int{0},
		},
		{
			scores:   []int{10, 20},
			idxRange: []int{0},
		},
		{
			scores:   []int{10, 20, 30},
			idxRange: []int{0},
		},
		{
			scores:   []int{30, 20, 10},
			idxRange: []int{2},
		},
		{
			scores:   []int{30, 11, 10},
			idxRange: []int{1, 2},
		},
		{
			scores:   []int{11, 11, 10},
			idxRange: []int{0, 1, 2},
		},
	}
	for tIdx, test := range tests {
		factor.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores[i], factor.bitNum)
			}
		}
		backends := createBackends(len(test.scores))
		targets := make(map[int]struct{}, len(test.scores))
		for i := 0; len(targets) < len(test.idxRange) || i < 100; i++ {
			require.Less(t, i, 100000, "test index %d", tIdx)
			backend := fm.BackendToRoute(backends)
			idx := slices.Index(backends, backend)
			require.Contains(t, test.idxRange, idx, "test index %d", tIdx)
			targets[idx] = struct{}{}
		}
	}
}

func TestRouteWith2Factors(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor1 := &mockFactor{bitNum: 4, balanceCount: 1, threshold: 1, canBeRouted: true}
	factor2 := &mockFactor{bitNum: 12, balanceCount: 1, threshold: 1, canBeRouted: true}
	fm.factors = []Factor{factor1, factor2}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores1  []int
		scores2  []int
		idxRange []int
	}{
		{
			scores1:  []int{10, 0, 0},
			scores2:  []int{0, 100, 200},
			idxRange: []int{1},
		},
		{
			scores1:  []int{10, 10, 0},
			scores2:  []int{0, 100, 200},
			idxRange: []int{2},
		},
		{
			scores1:  []int{10, 10, 10},
			scores2:  []int{0, 100, 200},
			idxRange: []int{0},
		},
		{
			scores1:  []int{10, 11, 11},
			scores2:  []int{100, 101, 100},
			idxRange: []int{0, 1, 2},
		},
		{
			scores1:  []int{10, 11, 11},
			scores2:  []int{100, 101, 110},
			idxRange: []int{0, 1},
		},
		{
			scores1:  []int{10, 11, 11},
			scores2:  []int{110, 101, 100},
			idxRange: []int{0, 1, 2},
		},
		{
			scores1:  []int{10, 20, 11},
			scores2:  []int{110, 0, 100},
			idxRange: []int{0, 2},
		},
	}
	for tIdx, test := range tests {
		factor1.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores1[i], factor1.bitNum)
			}
		}
		factor2.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores2[i], factor2.bitNum)
			}
		}
		backends := createBackends(len(test.scores1))
		targets := make(map[int]struct{}, len(test.scores1))
		for i := 0; len(targets) < len(test.idxRange) || i < 100; i++ {
			require.Less(t, i, 100000, "test index %d", tIdx)
			backend := fm.BackendToRoute(backends)
			idx := slices.Index(backends, backend)
			require.Contains(t, test.idxRange, idx, "test index %d", tIdx)
			targets[idx] = struct{}{}
		}
	}
}

func TestBalanceWithOneFactor(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor := &mockFactor{bitNum: 2, balanceCount: 1, canBeRouted: true}
	fm.factors = []Factor{factor}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores  []int
		fromIdx int
		toIdx   int
		count   int
	}{
		{
			scores:  []int{1, 2, 3},
			fromIdx: 2,
			toIdx:   0,
			count:   1,
		},
		{
			scores:  []int{3, 2, 1},
			fromIdx: 0,
			toIdx:   2,
			count:   1,
		},
		{
			scores: []int{1, 1, 1},
			count:  0,
		},
	}
	for tIdx, test := range tests {
		factor.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores[i], factor.bitNum)
			}
		}
		backends := createBackends(len(test.scores))
		from, to, count, reason, _ := fm.BackendsToBalance(backends)
		require.EqualValues(t, test.count, count, "test index %d", tIdx)
		if test.count > 0 {
			require.Equal(t, backends[test.fromIdx], from, "test index %d", tIdx)
			require.Equal(t, backends[test.toIdx], to, "test index %d", tIdx)
			require.Equal(t, "mock", reason, "test index %d", tIdx)
		} else {
			require.Nil(t, from, "test index %d", tIdx)
			require.Nil(t, to, "test index %d", tIdx)
		}
	}
}

func TestBalanceWith2Factors(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor1 := &mockFactor{bitNum: 2, balanceCount: 2, threshold: 1, canBeRouted: true}
	factor2 := &mockFactor{bitNum: 12, balanceCount: 1, canBeRouted: true}
	fm.factors = []Factor{factor1, factor2}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores1 []int
		scores2 []int
		fromIdx int
		toIdx   int
		count   int
	}{
		{
			scores1: []int{2, 0, 0},
			scores2: []int{0, 100, 200},
			fromIdx: 0,
			toIdx:   1,
			count:   2,
		},
		{
			scores1: []int{2, 2, 0},
			scores2: []int{0, 100, 200},
			fromIdx: 1,
			toIdx:   2,
			count:   2,
		},
		{
			scores1: []int{1, 1, 1},
			scores2: []int{0, 100, 200},
			fromIdx: 2,
			toIdx:   0,
			count:   1,
		},
		{
			scores1: []int{0, 0, 0},
			scores2: []int{200, 100, 0},
			fromIdx: 0,
			toIdx:   2,
			count:   1,
		},
		{
			scores1: []int{0, 2, 0},
			scores2: []int{100, 0, 0},
			fromIdx: 1,
			toIdx:   2,
			count:   2,
		},
		{
			scores1: []int{0, 0, 0},
			scores2: []int{100, 100, 100},
			count:   0,
		},
		{
			scores1: []int{1, 0, 0},
			scores2: []int{0, 100, 0},
			fromIdx: 1,
			toIdx:   2,
			count:   1,
		},
	}
	for tIdx, test := range tests {
		factor1.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores1[i], factor1.bitNum)
			}
		}
		factor2.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores2[i], factor2.bitNum)
			}
		}
		backends := createBackends(len(test.scores1))
		from, to, count, _, _ := fm.BackendsToBalance(backends)
		require.EqualValues(t, test.count, count, "test index %d", tIdx)
		if test.count > 0 {
			require.Equal(t, backends[test.fromIdx], from, "test index %d", tIdx)
			require.Equal(t, backends[test.toIdx], to, "test index %d", tIdx)
		} else {
			require.Nil(t, from, "test index %d", tIdx)
			require.Nil(t, to, "test index %d", tIdx)
		}
	}
}

func TestBalanceWith3Factors(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factors := []*mockFactor{{bitNum: 1, canBeRouted: true}, {bitNum: 2, canBeRouted: true}, {bitNum: 2, canBeRouted: true}}
	fm.factors = []Factor{factors[0], factors[1], factors[2]}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores        [2][3]int
		balanceCounts [3]int
		fromIdx       int
		toIdx         int
		count         int
	}{
		{
			scores:        [2][3]int{{1, 1, 1}, {0, 1, 0}},
			balanceCounts: [3]int{0, 10, 1},
			fromIdx:       0,
			toIdx:         1,
			count:         1,
		},
		{
			scores:        [2][3]int{{1, 0, 1}, {0, 1, 0}},
			balanceCounts: [3]int{0, 10, 1},
			count:         0,
		},
	}
	for tIdx, test := range tests {
		for factorIdx, factor := range factors {
			func(factorIdx int, factor *mockFactor) {
				factor.balanceCount = float64(test.balanceCounts[factorIdx])
				factor.updateScore = func(backends []scoredBackend) {
					for i := 0; i < len(backends); i++ {
						backends[i].addScore(test.scores[i][factorIdx], factor.bitNum)
					}
				}
			}(factorIdx, factor)
		}
		backends := createBackends(len(test.scores))
		from, to, count, _, _ := fm.BackendsToBalance(backends)
		require.EqualValues(t, test.count, count, "test index %d", tIdx)
		if test.count > 0 {
			require.Equal(t, backends[test.fromIdx], from, "test index %d", tIdx)
			require.Equal(t, backends[test.toIdx], to, "test index %d", tIdx)
		} else {
			require.Nil(t, from, "test index %d", tIdx)
			require.Nil(t, to, "test index %d", tIdx)
		}
	}
}

// Even if the factor doesn't add a score, the score is still updated so that we can find the unbalanced factor
// by locating the unbalanced bits.
func TestScoreAlwaysUpdated(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factors := []*mockFactor{{bitNum: 1, canBeRouted: true}, {bitNum: 2, canBeRouted: true}}
	fm.factors = []Factor{factors[0], factors[1]}
	factors[0].updateScore = func(backends []scoredBackend) {
		for i := 0; i < len(backends); i++ {
			backends[i].addScore(1, 1)
		}
	}
	factors[1].updateScore = func(backends []scoredBackend) {}
	backends := createBackends(1)
	scoredBackends := fm.updateScore(backends)
	require.EqualValues(t, 1<<2, scoredBackends[0].score())
}

func createBackends(num int) []policy.BackendCtx {
	backends := make([]policy.BackendCtx, 0, num)
	for i := 0; i < num; i++ {
		backend := newMockBackend(true, 100)
		backend.addr = strconv.Itoa(i)
		backends = append(backends, backend)
	}
	return backends
}

func TestSetFactors(t *testing.T) {
	tests := []struct {
		setFunc       func(balance *config.Balance)
		expectedNames []string
	}{
		{
			setFunc:       func(balance *config.Balance) {},
			expectedNames: []string{"status", "health", "memory", "cpu", "location", "conn"},
		},
		{
			setFunc: func(balance *config.Balance) {
				balance.Policy = config.BalancePolicyLocation
			},
			expectedNames: []string{"status", "location", "health", "memory", "cpu", "conn"},
		},
		{
			setFunc: func(balance *config.Balance) {
				balance.LabelName = "group"
			},
			expectedNames: []string{"label", "status", "health", "memory", "cpu", "location", "conn"},
		},
		{
			setFunc: func(balance *config.Balance) {
				balance.Policy = config.BalancePolicyLocation
				balance.LabelName = "group"
			},
			expectedNames: []string{"label", "status", "location", "health", "memory", "cpu", "conn"},
		},
		{
			setFunc: func(balance *config.Balance) {
				balance.Policy = config.BalancePolicyConnection
			},
			expectedNames: []string{"status", "conn"},
		},
		{
			setFunc: func(balance *config.Balance) {
				balance.Policy = config.BalancePolicyConnection
				balance.LabelName = "group"
			},
			expectedNames: []string{"label", "status", "conn"},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	for i, test := range tests {
		cfg := &config.Config{
			Balance: config.Balance{
				Policy: config.BalancePolicyResource,
			},
		}
		fm.Init(cfg)
		test.setFunc(&cfg.Balance)
		fm.SetConfig(cfg)
		require.Len(t, fm.factors, len(test.expectedNames), "test index %d", i)
		for j := 0; j < len(fm.factors); j++ {
			require.Equal(t, test.expectedNames[j], fm.factors[j].Name(), "test index %d", i)
		}
	}
	fm.Close()
}

func TestCanBeRouted(t *testing.T) {
	fm := NewFactorBasedBalance(zap.NewNop(), newMockMetricsReader())
	factor1 := &mockFactor{bitNum: 2, balanceCount: 1, canBeRouted: false}
	factor2 := &mockFactor{bitNum: 2, balanceCount: 1, canBeRouted: true}
	fm.factors = []Factor{factor1, factor2}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores1 []int
		scores2 []int
		routed  bool
	}{
		{
			scores1: []int{1, 0},
			scores2: []int{0, 0},
			routed:  true,
		},
		{
			scores1: []int{1, 1},
			scores2: []int{1, 0},
			routed:  false,
		},
		{
			scores1: []int{0, 0},
			scores2: []int{2, 1},
			routed:  true,
		},
		{
			scores1: []int{2, 1},
			scores2: []int{0, 0},
			routed:  false,
		},
	}
	for tIdx, test := range tests {
		factor1.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores1[i], factor1.bitNum)
			}
		}
		factor2.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores2[i], factor2.bitNum)
			}
		}
		backends := createBackends(len(test.scores1))
		_, _, count, _, _ := fm.BackendsToBalance(backends)
		require.Equal(t, test.routed, count > 0, "test index %d", tIdx)
		backends = createBackends(len(test.scores1))
		b := fm.BackendToRoute(backends)
		require.Equal(t, test.routed, b != nil, "test index %d", tIdx)
	}
}

func TestCanBalance(t *testing.T) {
	fm := NewFactorBasedBalance(zap.NewNop(), newMockMetricsReader())
	factor1 := &mockFactor{bitNum: 2, balanceCount: 1, advice: AdviceNegtive}
	factor2 := &mockFactor{bitNum: 2, balanceCount: 1}
	fm.factors = []Factor{factor1, factor2}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores1  []int
		scores2  []int
		balanced bool
	}{
		{
			scores1:  []int{1, 0},
			scores2:  []int{0, 0},
			balanced: false,
		},
		{
			scores1:  []int{1, 1},
			scores2:  []int{1, 0},
			balanced: false,
		},
		{
			scores1:  []int{1, 1},
			scores2:  []int{1, 1},
			balanced: false,
		},
	}
	for tIdx, test := range tests {
		factor1.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores1[i], factor1.bitNum)
			}
		}
		factor2.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores2[i], factor2.bitNum)
			}
		}
		backends := createBackends(len(test.scores1))
		_, _, count, _, _ := fm.BackendsToBalance(backends)
		require.Equal(t, test.balanced, count > 0, "test index %d", tIdx)
	}
}

func TestSetFactorConcurrently(t *testing.T) {
	fbb := NewFactorBasedBalance(zap.NewNop(), newMockMetricsReader())
	var wg waitgroup.WaitGroup
	cfg := &config.Config{}
	fbb.Init(cfg)
	wg.Add(10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	wg.Run(func() {
		defer wg.Done()
		policies := []string{config.BalancePolicyConnection, config.BalancePolicyResource, config.BalancePolicyLocation}
		for i := 0; ctx.Err() != nil; i++ {
			cfg.Balance = config.Balance{
				Policy: policies[i%len(policies)],
			}
			fbb.SetConfig(cfg)
		}
	})
	for i := 0; i < 9; i++ {
		wg.Run(func() {
			defer wg.Done()
			for ctx.Err() != nil {
				backends := createBackends(5)
				fbb.BackendToRoute(backends)
			}
		})
	}
	wg.Wait()
	cancel()
	fbb.Close()
}
