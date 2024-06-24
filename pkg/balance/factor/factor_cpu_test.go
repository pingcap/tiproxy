// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestCPUBalanceOnce(t *testing.T) {
	tests := []struct {
		cpus       [][]float64
		scoreOrder []int
		balanced   bool
	}{
		{
			cpus:       [][]float64{{0}, {0.2}},
			scoreOrder: []int{0, 1},
			balanced:   true,
		},
		{
			cpus:       [][]float64{{1, 0.5, 0.25}, {0, 0.25, 0.5}, {0, 0, 0}, {1, 1, 1}},
			scoreOrder: []int{2, 0, 1, 3},
			balanced:   false,
		},
		{
			cpus:       [][]float64{{0.25}, {}, {0.5}},
			scoreOrder: []int{0, 2, 1},
			balanced:   false,
		},
		{
			cpus:       [][]float64{{0.95, 0.92, 0.93, 0.94, 0.92, 0.94}, {0.81, 0.79, 0.82, 0.83, 0.76, 0.78}},
			scoreOrder: []int{1, 0},
			balanced:   false,
		},
		{
			cpus:       [][]float64{{0.35, 0.42, 0.37, 0.45, 0.42, 0.44}, {0.56, 0.62, 0.58, 0.57, 0.59, 0.63}},
			scoreOrder: []int{0, 1},
			balanced:   false,
		},
		{
			cpus:       [][]float64{{0, 0.1, 0, 0.1}, {0.15, 0.1, 0.15, 0.15}, {0.1, 0, 0.1, 0}},
			scoreOrder: []int{2, 0, 1},
			balanced:   true,
		},
		{
			cpus:       [][]float64{{0.5}, {}, {0.1, 0.3, 0.4}},
			scoreOrder: []int{2, 0, 1},
			balanced:   false,
		},
		{
			cpus:       [][]float64{{1.0}, {0.97}},
			scoreOrder: []int{1, 0},
			balanced:   true,
		},
		{
			cpus:       [][]float64{{0.8, 0.2, 0.8, 0.2}, {0.3, 0.5, 0.3, 0.5}},
			scoreOrder: []int{0, 1},
			balanced:   true,
		},
		{
			cpus:       [][]float64{{1.0}, {0.9}, {0.8}, {0.7}, {0.6}, {0.5}, {0.4}, {0.3}, {0.1}},
			scoreOrder: []int{8, 7, 6, 5, 4, 3, 2, 1, 0},
			balanced:   false,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, 0, 0))
			values = append(values, createSampleStream(test.cpus[j], j, model.Now()))
		}
		mmr := &mockMetricsReader{
			qrs: map[uint64]metricsreader.QueryResult{
				1: {
					UpdateTime: monotime.Now(),
					Value:      model.Matrix(values),
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
		require.Equal(t, test.balanced, balanceCount < 0.0001, "test index %d", i)
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
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{170, 130},
			connScores:    []int{170, 130},
			newConnScores: []int{142, 158},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{0, 300},
			newConnScores: []int{0, 300},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{}, {}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{0.5, 0.4, 0.6, 0.5, 0.5}, {0.2, 0.1, 0.2, 0.1, 0.2}},
			connCounts:    []int{200, 100},
			connScores:    []int{180, 120},
			newConnScores: []int{172, 128},
		},
		{
			cpus:          [][]float64{{0.8, 0.2, 0.8, 0.2}, {0.3, 0.5, 0.3, 0.5}},
			connCounts:    []int{100, 200},
			connScores:    []int{100, 200},
			newConnScores: []int{100, 200},
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	curTime := model.Now().Add(-10 * time.Second)
	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, test.connCounts[j], test.connScores[j]))
			values = append(values, createSampleStream(test.cpus[j], j, curTime))
		}
		curTime = curTime.Add(time.Millisecond)
		mmr.qrs[1] = metricsreader.QueryResult{
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
			count := int(balanceCount + 0.9999)
			backends[len(backends)-1].BackendCtx.(*mockBackend).connScore -= count
			backends[0].BackendCtx.(*mockBackend).connScore += count
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
			updateTime: monotime.Now().Add(-cpuMetricExpDuration * 2),
		},
		{
			cpus:       [][]float64{{math.NaN()}, {math.NaN()}},
			updateTime: monotime.Now(),
		},
	}
	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	backends := []scoredBackend{createBackend(0, 0, 0), createBackend(1, 0, 0)}
	for i, test := range tests {
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			ss := createSampleStream(test.cpus[j], j, model.Time(test.updateTime/monotime.Time(time.Millisecond)))
			values = append(values, ss)
		}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: test.updateTime,
			Value:      model.Matrix(values),
		}
		updateScore(fc, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}

func TestCPUResultNotUpdated(t *testing.T) {
	now := model.Now()
	tests := []struct {
		cpu           float64
		updateTime    model.Time
		expectedScore int
	}{
		{
			cpu:           0.1,
			updateTime:    now,
			expectedScore: 2,
		},
		{
			cpu:           0.1,
			updateTime:    now,
			expectedScore: 2,
		},
		{
			cpu:           0.3,
			updateTime:    now.Add(time.Second),
			expectedScore: 6,
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr)
	backends := []scoredBackend{createBackend(0, 0, 0), createBackend(1, 0, 0)}
	for i, test := range tests {
		array := []float64{test.cpu}
		values := []*model.SampleStream{createSampleStream(array, 0, test.updateTime), createSampleStream(array, 1, test.updateTime)}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: monotime.Now(),
			Value:      model.Matrix(values),
		}
		updateScore(fc, backends)
		require.EqualValues(t, test.expectedScore, backends[0].score(), "test index %d", i)
	}
}
