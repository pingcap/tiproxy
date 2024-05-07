// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestCPUBalanceOnce(t *testing.T) {
	tests := []struct {
		cpus         [][]float64
		scoreOrder   []int
		balanceCount int
	}{
		{
			cpus:         [][]float64{{0}, {0.2}},
			scoreOrder:   []int{0, 1},
			balanceCount: 0,
		},
		{
			cpus:         [][]float64{{1, 0.5, 0.25, 0}, {0, 0.25, 0.5, 1}, {0, 0, 0, 0}, {1, 1, 1, 1}},
			scoreOrder:   []int{2, 0, 1, 3},
			balanceCount: balanceCount4Cpu,
		},
		{
			cpus:         [][]float64{{0.25}, {}, {0.5}},
			scoreOrder:   []int{0, 2, 1},
			balanceCount: balanceCount4Cpu,
		},
		{
			cpus:         [][]float64{{0.95, 0.92, 0.93, 0.94, 0.92, 0.94}, {0.81, 0.79, 0.82, 0.83, 0.76, 0.78}},
			scoreOrder:   []int{1, 0},
			balanceCount: balanceCount4Cpu,
		},
		{
			cpus:         [][]float64{{0.35, 0.42, 0.37, 0.45, 0.42, 0.44}, {0.56, 0.62, 0.58, 0.57, 0.59, 0.63}},
			scoreOrder:   []int{0, 1},
			balanceCount: balanceCount4Cpu,
		},
		{
			cpus:         [][]float64{{0, 0.1, 0, 0.1}, {0.15, 0.1, 0.15, 0.1}, {0.1, 0, 0.1, 0}},
			scoreOrder:   []int{2, 0, 1},
			balanceCount: 0,
		},
		{
			cpus:         [][]float64{{0.5}, {}, {0.1, 0.3, 0.5}},
			scoreOrder:   []int{2, 0, 1},
			balanceCount: balanceCount4Cpu,
		},
		{
			cpus:         [][]float64{{1.0}, {0.97}},
			scoreOrder:   []int{1, 0},
			balanceCount: 0,
		},
		{
			cpus:         [][]float64{{1.0}, {0.9}, {0.8}, {0.7}, {0.6}, {0.5}, {0.4}, {0.3}, {0.1}},
			scoreOrder:   []int{8, 7, 6, 5, 4, 3, 2, 1, 0},
			balanceCount: balanceCount4Cpu,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, 0, 0))
			values = append(values, createSampleStream(test.cpus[j], j))
		}
		mmr := &mockMetricsReader{
			qr: metricsreader.QueryResult{
				UpdateTime: monotime.Now(),
				Value:      model.Matrix(values),
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
		cpus          [][]float64
		connCounts    []int
		connScores    []int
		newConnScores []int
	}{
		{
			cpus:          [][]float64{{}, {}},
			connCounts:    []int{0, 0},
			connScores:    []int{0, 0},
			newConnScores: []int{0, 0},
		},
		{
			cpus:          [][]float64{{0.01}, {0.02}},
			connCounts:    []int{100, 200},
			connScores:    []int{100, 200},
			newConnScores: []int{100, 200},
		},
		{
			cpus:          [][]float64{{0.1, 0.1, 0.1, 0.1, 0.1}, {0.05, 0.07, 0.05, 0.04, 0.03}},
			connCounts:    []int{200, 100},
			connScores:    []int{200, 100},
			newConnScores: []int{200, 100},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{200, 100},
			newConnScores: []int{170, 130},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{170, 130},
			connScores:    []int{170, 130},
			newConnScores: []int{140, 160},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{170, 130},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{0, 300},
			newConnScores: []int{86, 214},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{170, 130},
		},
		{
			cpus:          [][]float64{{}, {}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{170, 130},
		},
		{
			cpus:          [][]float64{{}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{170, 130},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{170, 130},
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, test.connCounts[j], test.connScores[j]))
			values = append(values, createSampleStream(test.cpus[j], j))
		}
		mmr.qr = metricsreader.QueryResult{
			UpdateTime: monotime.Now(),
			Value:      model.Matrix(values),
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
		cpus       [][]float64
		updateTime monotime.Time
	}{
		{
			cpus: nil,
		},
		{
			cpus:       [][]float64{{1.0}, {0.0}},
			updateTime: monotime.Now().Sub(metricExpDuration * 2),
		},
		{
			cpus:       [][]float64{{math.NaN()}, {math.NaN()}},
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
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			values = append(values, createSampleStream(test.cpus[j], j))
		}
		mmr.qr = metricsreader.QueryResult{
			UpdateTime: test.updateTime,
			Value:      model.Matrix(values),
		}
		updateScore(fc, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}

func createBackend(backendIdx, connCount, connScore int) scoredBackend {
	host := strconv.Itoa(backendIdx)
	return scoredBackend{
		BackendCtx: &mockBackend{
			BackendInfo: observer.BackendInfo{
				IP:         host,
				StatusPort: 10080,
			},
			addr:      host + ":4000",
			connCount: connCount,
			connScore: connScore,
			healthy:   true,
		},
	}
}

func createSampleStream(cpus []float64, backendIdx int) *model.SampleStream {
	host := strconv.Itoa(backendIdx)
	labelSet := model.Metric{metricsreader.LabelNameInstance: model.LabelValue(host + ":10080")}
	pairs := make([]model.SamplePair, 0, len(cpus))
	for _, cpu := range cpus {
		pairs = append(pairs, model.SamplePair{Value: model.SampleValue(cpu)})
	}
	return &model.SampleStream{Metric: labelSet, Values: pairs}
}

func updateScore(fc *FactorCPU, backends []scoredBackend) {
	for i := 0; i < len(backends); i++ {
		backends[i].scoreBits = 0
	}
	fc.UpdateScore(backends)
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].score() < backends[j].score()
	})
}
