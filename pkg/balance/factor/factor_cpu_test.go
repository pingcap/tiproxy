// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestCPUBalanceOnce(t *testing.T) {
	tests := []struct {
		cpus         []float64
		connCounts   []int
		scoreOrder   []int
		balanceCount int
	}{
		{
			cpus:         []float64{0, 0.2},
			connCounts:   []int{0, 0},
			scoreOrder:   []int{0, 1},
			balanceCount: 0,
		},
		{
			cpus:         []float64{0.25, 0.5, 0, 1},
			connCounts:   []int{10, 10, 10, 10},
			scoreOrder:   []int{2, 0, 1, 3},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.25, math.NaN(), 0.5},
			connCounts:   []int{10, 10, 10},
			scoreOrder:   []int{0, 2, 1},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.92, 0.76},
			connCounts:   []int{10, 10},
			scoreOrder:   []int{1, 0},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.42, 0.59},
			connCounts:   []int{10, 10},
			scoreOrder:   []int{0, 1},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.1, 0.25},
			connCounts:   []int{0, 0},
			scoreOrder:   []int{0, 1},
			balanceCount: 0,
		},
		{
			cpus:         []float64{0.1, 0.35},
			connCounts:   []int{10, 10},
			scoreOrder:   []int{0, 1},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.95, math.NaN()},
			connCounts:   []int{0, 0},
			scoreOrder:   []int{0, 1},
			balanceCount: 0,
		},
		{
			cpus:         []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.1},
			connCounts:   []int{0, 0, 0, 0, 0, 0, 0, 0, 0},
			scoreOrder:   []int{8, 7, 6, 5, 4, 3, 2, 1, 0},
			balanceCount: 1,
		},
		{
			cpus:         []float64{0.4, 0.8},
			connCounts:   []int{2000, 4000},
			scoreOrder:   []int{0, 1},
			balanceCount: 8,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.Sample, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, test.connCounts[j], test.connCounts[j]))
			values = append(values, createSample(test.cpus[j], j))
		}
		mmr := &mockMetricsReader{
			qrs: map[uint64]metricsreader.QueryResult{
				1: {
					UpdateTime: monotime.Now(),
					Value:      model.Vector(values),
				},
			},
		}
		fc := NewFactorCPU(mmr)
		updateScore(fc, backends)
		sortedIdx := make([]int, 0, len(test.cpus))
		for _, backend := range backends {
			idx, err := strconv.Atoi(backend.GetBackendInfo().IP)
			require.NoError(t, err)
			sortedIdx = append(sortedIdx, idx)
		}
		require.Equal(t, test.scoreOrder, sortedIdx, "test index %d", i)
		from, to := backends[len(backends)-1], backends[0]
		balanceCount := fc.BalanceCount(from, to)
		require.Equal(t, test.balanceCount, balanceCount, "test index %d", i)
	}
}

func TestCPUBalanceContinuously(t *testing.T) {
	tests := []struct {
		cpus          []float64
		connCounts    []int
		connScores    []int
		newConnScores []int
	}{
		{
			cpus:          []float64{math.NaN(), math.NaN()},
			connCounts:    []int{0, 0},
			connScores:    []int{0, 0},
			newConnScores: []int{0, 0},
		},
		{
			cpus:          []float64{0.01, 0.02},
			connCounts:    []int{100, 200},
			connScores:    []int{100, 200},
			newConnScores: []int{100, 200},
		},
		{
			cpus:          []float64{0.5, 0.2},
			connCounts:    []int{200, 100},
			connScores:    []int{200, 100},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          []float64{0.5, 0.2},
			connCounts:    []int{170, 130},
			connScores:    []int{170, 130},
			newConnScores: []int{142, 158},
		},
		{
			cpus:          []float64{0.5, 0.2},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          []float64{0.5, 0.2},
			connCounts:    []int{200, 100},
			connScores:    []int{0, 300},
			newConnScores: []int{99, 201},
		},
		{
			cpus:          []float64{0.5, math.NaN()},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{241, 59},
		},
		{
			cpus:          []float64{math.NaN(), 0.2},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{115, 185},
		},
		{
			cpus:          []float64{0.5, 0.2},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.Sample, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, test.connCounts[j], test.connScores[j]))
			values = append(values, createSample(test.cpus[j], j))
		}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: monotime.Now(),
			Value:      model.Vector(values),
		}
		// Rebalance until it stops. The final scores should be newConnScores.
		for k := 0; ; k++ {
			if k > 300 {
				t.Fatal("balance doesn't stop")
			}
			updateScore(fc, backends)
			balanceCount := fc.BalanceCount(backends[len(backends)-1], backends[0])
			if balanceCount == 0 {
				break
			}
			backends[len(backends)-1].BackendCtx.(*mockBackend).connScore -= balanceCount
			backends[0].BackendCtx.(*mockBackend).connScore += balanceCount
		}
		connScores := make([]int, len(test.connScores))
		for _, backend := range backends {
			idx, err := strconv.Atoi(backend.GetBackendInfo().IP)
			require.NoError(t, err)
			connScores[idx] = backend.ConnScore()
		}
		require.Equal(t, test.newConnScores, connScores, "test index %d", i)
	}
}

func TestNoCPUMetric(t *testing.T) {
	tests := []struct {
		cpus       []float64
		updateTime monotime.Time
	}{
		{
			cpus: nil,
		},
		{
			cpus:       []float64{1.0, 0.0},
			updateTime: monotime.Now().Sub(cpuMetricExpDuration * 2),
		},
		{
			cpus:       []float64{math.NaN(), math.NaN()},
			updateTime: monotime.Now(),
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	backends := make([]scoredBackend, 0, 2)
	for i := 0; i < 2; i++ {
		backends = append(backends, createBackend(i, i*100, i*100))
	}
	for i, test := range tests {
		values := make([]*model.Sample, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			values = append(values, createSample(test.cpus[j], j))
		}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: test.updateTime,
			Value:      model.Vector(values),
		}
		updateScore(fc, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}
