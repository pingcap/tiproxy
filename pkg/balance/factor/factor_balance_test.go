// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/stretchr/testify/require"
)

func TestRouteWithOneFactor(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor := &mockFactor{bitNum: 2}
	fm.factors = []Factor{factor}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores      []int
		expectedIdx int
	}{
		{
			scores:      []int{1, 2, 3},
			expectedIdx: 0,
		},
		{
			scores:      []int{3, 2, 1},
			expectedIdx: 2,
		},
	}
	for tIdx, test := range tests {
		factor.updateScore = func(backends []scoredBackend) {
			for i := 0; i < len(backends); i++ {
				backends[i].addScore(test.scores[i], factor.bitNum)
			}
		}
		backends := createBackends(len(test.scores))
		backend := fm.BackendToRoute(backends)
		require.Equal(t, backends[test.expectedIdx], backend, "test index %d", tIdx)
	}
}

func TestRouteWith2Factors(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor1, factor2 := &mockFactor{bitNum: 1}, &mockFactor{bitNum: 12}
	fm.factors = []Factor{factor1, factor2}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores1     []int
		scores2     []int
		expectedIdx int
	}{
		{
			scores1:     []int{1, 0, 0},
			scores2:     []int{0, 100, 200},
			expectedIdx: 1,
		},
		{
			scores1:     []int{1, 1, 0},
			scores2:     []int{0, 100, 200},
			expectedIdx: 2,
		},
		{
			scores1:     []int{1, 1, 1},
			scores2:     []int{0, 100, 200},
			expectedIdx: 0,
		},
		{
			scores1:     []int{0, 0, 0},
			scores2:     []int{200, 100, 0},
			expectedIdx: 2,
		},
		{
			scores1:     []int{0, 1, 0},
			scores2:     []int{100, 0, 0},
			expectedIdx: 2,
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
		backend := fm.BackendToRoute(backends)
		require.Equal(t, backends[test.expectedIdx], backend, "test index %d", tIdx)
	}
}

func TestBalanceWithOneFactor(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor := &mockFactor{bitNum: 2, balanceCount: 1}
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
		from, to, count, _ := fm.BackendsToBalance(backends)
		require.Equal(t, test.count, count, "test index %d", tIdx)
		if test.count > 0 {
			require.Equal(t, backends[test.fromIdx], from, "test index %d", tIdx)
			require.Equal(t, backends[test.toIdx], to, "test index %d", tIdx)
		} else {
			require.Nil(t, from, "test index %d", tIdx)
			require.Nil(t, to, "test index %d", tIdx)
		}
	}
}

func TestBalanceWith2Factors(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	fm := NewFactorBasedBalance(lg, newMockMetricsReader())
	factor1, factor2 := &mockFactor{bitNum: 1, balanceCount: 2}, &mockFactor{bitNum: 12, balanceCount: 1}
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
			scores1: []int{1, 0, 0},
			scores2: []int{0, 100, 200},
			fromIdx: 0,
			toIdx:   1,
			count:   2,
		},
		{
			scores1: []int{1, 1, 0},
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
			scores1: []int{0, 1, 0},
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
		from, to, count, _ := fm.BackendsToBalance(backends)
		require.Equal(t, test.count, count, "test index %d", tIdx)
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
	factors := []*mockFactor{{bitNum: 1}, {bitNum: 2}, {bitNum: 2}}
	fm.factors = []Factor{factors[0], factors[1], factors[2]}
	require.NoError(t, fm.updateBitNum())

	tests := []struct {
		scores        [2][3]int
		balanceCounts [3]int
		fromIdx       int
		toIdx         int
		count         int
	}{
		//{
		//	scores:        [2][3]int{{1, 1, 1}, {0, 1, 0}},
		//	balanceCounts: [3]int{0, 10, 1},
		//	fromIdx:       0,
		//	toIdx:         1,
		//	count:         1,
		//},
		{
			scores:        [2][3]int{{1, 0, 1}, {0, 1, 0}},
			balanceCounts: [3]int{0, 10, 1},
			count:         0,
		},
	}
	for tIdx, test := range tests {
		for factorIdx, factor := range factors {
			func(factorIdx int, factor *mockFactor) {
				factor.balanceCount = test.balanceCounts[factorIdx]
				factor.updateScore = func(backends []scoredBackend) {
					for i := 0; i < len(backends); i++ {
						backends[i].addScore(test.scores[i][factorIdx], factor.bitNum)
					}
				}
			}(factorIdx, factor)
		}
		backends := createBackends(len(test.scores))
		from, to, count, _ := fm.BackendsToBalance(backends)
		require.Equal(t, test.count, count, "test index %d", tIdx)
		if test.count > 0 {
			require.Equal(t, backends[test.fromIdx], from, "test index %d", tIdx)
			require.Equal(t, backends[test.toIdx], to, "test index %d", tIdx)
		} else {
			require.Nil(t, from, "test index %d", tIdx)
			require.Nil(t, to, "test index %d", tIdx)
		}
	}
}

func createBackends(num int) []policy.BackendCtx {
	backends := make([]policy.BackendCtx, 0, num)
	for i := 0; i < num; i++ {
		backends = append(backends, newMockBackend(true, 100))
	}
	return backends
}
