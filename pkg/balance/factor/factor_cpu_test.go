// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
			qrs: map[string]metricsreader.QueryResult{
				"cpu": {
					UpdateTime: time.Now(),
					Value:      model.Matrix(values),
				},
			},
		}
		fc := NewFactorCPU(mmr, zap.NewNop())
		updateScore(fc, backends)
		sortedIdx := make([]int, 0, len(test.cpus))
		for _, backend := range backends {
			idx, err := strconv.Atoi(backend.GetBackendInfo().IP)
			require.NoError(t, err)
			sortedIdx = append(sortedIdx, idx)
		}
		require.Equal(t, test.scoreOrder, sortedIdx, "test index %d", i)
		from, to := backends[len(backends)-1], backends[0]
		balanceCount, _ := fc.BalanceCount(from, to)
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
	fc := NewFactorCPU(mmr, zap.NewNop())
	curTime := model.Now().Add(-10 * time.Second)
	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.cpus))
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			backends = append(backends, createBackend(j, test.connCounts[j], test.connScores[j]))
			values = append(values, createSampleStream(test.cpus[j], j, curTime))
		}
		curTime = curTime.Add(time.Millisecond)
		mmr.qrs["cpu"] = metricsreader.QueryResult{
			UpdateTime: time.Now(),
			Value:      model.Matrix(values),
		}
		// Rebalance until it stops. The final scores should be newConnScores.
		for k := 0; ; k++ {
			if k > 300 {
				t.Fatal("balance doesn't stop")
			}
			updateScore(fc, backends)
			balanceCount, _ := fc.BalanceCount(backends[len(backends)-1], backends[0])
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
		updateTime time.Time
	}{
		{
			cpus: nil,
		},
		{
			cpus:       [][]float64{{1.0}, {0.0}},
			updateTime: time.Now().Add(-cpuMetricExpDuration * 2),
		},
		{
			cpus:       [][]float64{{math.NaN()}, {math.NaN()}},
			updateTime: time.Now(),
		},
	}
	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr, zap.NewNop())
	backends := []scoredBackend{createBackend(0, 0, 0), createBackend(1, 0, 0)}
	for i, test := range tests {
		values := make([]*model.SampleStream, 0, len(test.cpus))
		for j := 0; j < len(test.cpus); j++ {
			ss := createSampleStream(test.cpus[j], j, model.TimeFromUnixNano(test.updateTime.UnixNano()))
			values = append(values, ss)
		}
		mmr.qrs["cpu"] = metricsreader.QueryResult{
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
	fc := NewFactorCPU(mmr, zap.NewNop())
	backends := []scoredBackend{createBackend(0, 0, 0), createBackend(1, 0, 0)}
	for i, test := range tests {
		array := []float64{test.cpu}
		values := []*model.SampleStream{createSampleStream(array, 0, test.updateTime), createSampleStream(array, 1, test.updateTime)}
		mmr.qrs["cpu"] = metricsreader.QueryResult{
			UpdateTime: time.Now(),
			Value:      model.Matrix(values),
		}
		updateScore(fc, backends)
		require.EqualValues(t, test.expectedScore, backends[0].score(), "test index %d", i)
	}
}

func TestCPUQueryRule(t *testing.T) {
	tests := []struct {
		text       string
		timestamp  model.Time
		curValue   model.SampleValue
		finalValue model.SampleValue
	}{
		{
			text: `process_cpu_seconds_total 10
tidb_server_maxprocs 2
`,
			timestamp:  model.Time(0),
			curValue:   5,
			finalValue: model.SampleValue(math.NaN()),
		},
		{
			text: `process_cpu_seconds_total 10
tidb_server_maxprocs 2
`,
			timestamp:  model.Time(1000),
			curValue:   5,
			finalValue: 0,
		},
		{
			text: `process_cpu_seconds_total 12
tidb_server_maxprocs 2
`,
			timestamp:  model.Time(2000),
			curValue:   6,
			finalValue: 1,
		},
		{
			text: `process_cpu_seconds_total 2
tidb_server_maxprocs 2
`,
			timestamp:  model.Time(3000),
			curValue:   1,
			finalValue: model.SampleValue(math.NaN()),
		},
		{
			text: `process_cpu_seconds_total 3
tidb_server_maxprocs 2
`,
			timestamp:  model.Time(3500),
			curValue:   1.5,
			finalValue: model.SampleValue(math.NaN()),
		},
	}

	historyPair := make([]model.SamplePair, 0)
	for i, test := range tests {
		var parser expfmt.TextParser
		mfs, err := parser.TextToMetricFamilies(strings.NewReader(test.text))
		require.NoError(t, err, "case %d", i)
		value := cpuQueryRule.Metric2Value(mfs)
		require.Equal(t, test.curValue, value, "case %d", i)
		historyPair = append(historyPair, model.SamplePair{Timestamp: test.timestamp, Value: value})
		value = cpuQueryRule.Range2Value(historyPair)
		if math.IsNaN(float64(test.finalValue)) {
			require.True(t, math.IsNaN(float64(value)), "case %d", i)
		} else {
			require.Equal(t, test.finalValue, value, "case %d", i)
		}
	}
}

func TestCPUScore(t *testing.T) {
	tests := []struct {
		connCounts   []int
		connScores   []int
		scores       []uint64
		usagePerConn float64
	}{
		{
			connCounts:   []int{18, 2},
			connScores:   []int{18, 2},
			scores:       []uint64{8, 8},
			usagePerConn: 0.04,
		},
		// cpu1=0.4-8*0.04, cpu2=0.4+8*0.04
		{
			connCounts:   []int{10, 10},
			connScores:   []int{10, 10},
			scores:       []uint64{1, 14},
			usagePerConn: 0.04,
		},
		// cpu1=0.4-16*0.04, cpu2=0.4+16*0.04
		{
			connCounts:   []int{2, 18},
			connScores:   []int{2, 18},
			scores:       []uint64{0, 20},
			usagePerConn: 0.04,
		},
	}

	mmr := newMockMetricsReader()
	fc := NewFactorCPU(mmr, zap.NewNop())
	cpus := []float64{0.4}
	curTime := model.Now().Add(-10 * time.Second)
	values := []*model.SampleStream{createSampleStream(cpus, 0, curTime), createSampleStream(cpus, 1, curTime)}
	mmr.qrs["cpu"] = metricsreader.QueryResult{
		UpdateTime: curTime.Time(),
		Value:      model.Matrix(values),
	}
	for i, test := range tests {
		backends := []scoredBackend{createBackend(0, test.connCounts[0], test.connScores[0]), createBackend(1, test.connCounts[1], test.connScores[1])}
		fc.UpdateScore(backends)
		require.Equal(t, test.usagePerConn, fc.usagePerConn, "test index %d", i)
		scores := make([]uint64, 0, len(backends))
		for _, backend := range backends {
			scores = append(scores, backend.scoreBits)
		}
		require.Equal(t, test.scores, scores, "test index %d", i)
	}
}
