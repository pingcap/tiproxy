// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestMemoryScore(t *testing.T) {
	tests := []struct {
		memory []float64
		score  uint64
	}{
		{
			memory: []float64{},
			score:  0,
		},
		{
			memory: []float64{math.NaN()},
			score:  0,
		},
		{
			memory: []float64{0.5, math.NaN()},
			score:  0,
		},
		{
			memory: []float64{math.NaN(), 0.5},
			score:  0,
		},
		{
			memory: []float64{0},
			score:  0,
		},
		{
			memory: []float64{0.8},
			score:  2,
		},
		{
			memory: []float64{0, 0.1},
			score:  1,
		},
		{
			memory: []float64{1.0, 0.95},
			score:  2,
		},
		{
			memory: []float64{0.18, 0.2},
			score:  0,
		},
		{
			memory: []float64{0.2, 0.2},
			score:  0,
		},
		{
			memory: []float64{0.2, 0.18},
			score:  0,
		},
		{
			memory: []float64{0.4, 0.7},
			score:  2,
		},
		{
			memory: []float64{0.6, 0.65},
			score:  1,
		},
		{
			memory: []float64{0.75, 0.85},
			score:  2,
		},
		{
			memory: []float64{0.55, math.NaN(), math.NaN(), 0.6},
			score:  1,
		},
		{
			memory: []float64{0.55, 0.6, 0.55, 0.6},
			score:  1,
		},
	}

	backends := make([]scoredBackend, 0, len(tests))
	values := make([]*model.SampleStream, 0, len(tests))
	for i, test := range tests {
		backends = append(backends, createBackend(i, 0, 0))
		values = append(values, createSampleStream(test.memory, i, model.Now()))
	}
	mmr := &mockMetricsReader{
		qrs: map[uint64]metricsreader.QueryResult{
			1: {
				UpdateTime: monotime.Now(),
				Value:      model.Matrix(values),
			},
		},
	}
	fm := NewFactorMemory(mmr)
	fm.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.score, backends[i].score(), "test index %d", i)
	}
}

func TestMemoryBalance(t *testing.T) {
	tests := []struct {
		memory   [][]float64
		scores   []uint64
		balanced bool
	}{
		{
			memory:   [][]float64{{0.2, 0.3}, {0.3, 0.4}},
			scores:   []uint64{1, 1},
			balanced: true,
		},
		{
			memory:   [][]float64{{0.21, 0.22}, {0.5, 0.55}},
			scores:   []uint64{0, 1},
			balanced: true,
		},
		{
			memory:   [][]float64{{0.5, 0.7}, {0.71, 0.74}},
			scores:   []uint64{2, 1},
			balanced: true,
		},
		{
			memory:   [][]float64{{0.85, 0.75}, {0.85, 0.85}},
			scores:   []uint64{1, 2},
			balanced: true,
		},
		{
			memory:   [][]float64{{0.6, 0.75}, {0.81, 0.82}},
			scores:   []uint64{2, 2},
			balanced: true,
		},
		{
			memory:   [][]float64{{0.2, 0.15}, {0.81, 0.82}},
			scores:   []uint64{0, 2},
			balanced: false,
		},
		{
			memory:   [][]float64{{0.2, 0.15}, {0.81, 0.82}, {0.65, 0.65}},
			scores:   []uint64{0, 2, 1},
			balanced: false,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.memory))
		values := make([]*model.SampleStream, 0, len(test.memory))
		for j := 0; j < len(test.memory); j++ {
			backends = append(backends, createBackend(j, 100, 100))
			values = append(values, createSampleStream(test.memory[j], j, model.Now()))
		}
		mmr := &mockMetricsReader{
			qrs: map[uint64]metricsreader.QueryResult{
				1: {
					UpdateTime: monotime.Now(),
					Value:      model.Matrix(values),
				},
			},
		}
		fm := NewFactorMemory(mmr)
		fm.UpdateScore(backends)
		scores := make([]uint64, 0, len(backends))
		for _, backend := range backends {
			scores = append(scores, backend.score())
		}
		require.Equal(t, test.scores, scores, "test index %d", i)
		sort.Slice(backends, func(i, j int) bool {
			return backends[i].score() < backends[j].score()
		})
		from, to := backends[len(backends)-1], backends[0]
		balanceCount := fm.BalanceCount(from, to)
		require.EqualValues(t, test.balanced, balanceCount < 0.0001, "test index %d", i)
	}
}

func TestNoMemMetrics(t *testing.T) {
	tests := []struct {
		mem        [][]float64
		updateTime monotime.Time
	}{
		{
			mem: nil,
		},
		{
			mem:        [][]float64{{1.0}, {0.0}},
			updateTime: monotime.Now().Sub(memMetricExpDuration * 2),
		},
		{
			mem:        [][]float64{{math.NaN()}, {math.NaN()}},
			updateTime: monotime.Now(),
		},
	}

	mmr := newMockMetricsReader()
	fm := NewFactorMemory(mmr)
	backends := make([]scoredBackend, 0, 2)
	for i := 0; i < 2; i++ {
		backends = append(backends, createBackend(i, i*100, i*100))
	}
	for i, test := range tests {
		values := make([]*model.SampleStream, 0, len(test.mem))
		for j := 0; j < len(test.mem); j++ {
			ss := createSampleStream(test.mem[j], j, model.Now())
			ss.Values[0].Timestamp = model.Time(test.updateTime / monotime.Time(time.Millisecond))
			values = append(values, ss)
		}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: test.updateTime,
			Value:      model.Matrix(values),
		}
		updateScore(fm, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}

func TestMemoryBalanceCount(t *testing.T) {
	tests := []struct {
		conn      int
		riskLevel int
		count     float64
	}{
		{
			conn:      120,
			riskLevel: 1,
			count:     120 / balanceSeconds4HighMemory,
		},
		{
			conn:      60,
			riskLevel: 1,
			count:     120 / balanceSeconds4HighMemory,
		},
		{
			conn:      60,
			riskLevel: 2,
			count:     60 / balanceSeconds4OOMRisk,
		},
		{
			conn:      30,
			riskLevel: 2,
			count:     60 / balanceSeconds4OOMRisk,
		},
		{
			conn:      60,
			riskLevel: 0,
			count:     0,
		},
		{
			conn:      60,
			riskLevel: 1,
			count:     60 / balanceSeconds4HighMemory,
		},
	}

	mmr := &mockMetricsReader{
		qrs: map[uint64]metricsreader.QueryResult{},
	}
	ts := model.Now().Add(-100 * time.Millisecond)
	updateMmr := func(riskLevel int) {
		var usage []float64
		switch riskLevel {
		case 0:
			usage = []float64{0.1, 0.1}
		case 1:
			usage = []float64{0.8, 0.8}
		case 2:
			usage = []float64{0.1, 0.4}
		}
		ts = ts.Add(time.Millisecond)
		values := []*model.SampleStream{
			createSampleStream(usage, 0, ts),
			createSampleStream([]float64{0, 0}, 1, ts),
		}
		mmr.qrs[1] = metricsreader.QueryResult{
			UpdateTime: monotime.Now(),
			Value:      model.Matrix(values),
		}
	}
	fs := NewFactorMemory(mmr)
	backends := make([]scoredBackend, 0, 2)
	backends = append(backends, createBackend(0, 0, 0))
	backends = append(backends, createBackend(1, 0, 0))
	unhealthyBackend := backends[0].BackendCtx.(*mockBackend)
	for i, test := range tests {
		updateMmr(test.riskLevel)
		unhealthyBackend.connScore = test.conn
		fs.UpdateScore(backends)
		if test.riskLevel == 0 {
			continue
		}
		count := fs.BalanceCount(backends[0], backends[1])
		require.Equal(t, test.count, count, "test idx: %d", i)
	}
}
