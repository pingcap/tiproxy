// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"sort"
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestErrorScore(t *testing.T) {
	tests := []struct {
		errCounts []float64
		score     uint64
	}{
		{
			errCounts: []float64{math.NaN(), math.NaN()},
			score:     2,
		},
		{
			errCounts: []float64{math.NaN(), 0},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].failThreshold + 1), 0},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].failThreshold - 1)},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold - 1), float64(errDefinitions[1].recoverThreshold - 1)},
			score:     0,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
			score:     1,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold + 1)},
			score:     1,
		},
	}

	backends := make([]scoredBackend, 0, len(tests))
	values1 := make([]*model.Sample, 0, len(tests))
	values2 := make([]*model.Sample, 0, len(tests))
	for i, test := range tests {
		backends = append(backends, createBackend(i, 0, 0))
		values1 = append(values1, createSample(float64(test.errCounts[0]), i))
		values2 = append(values2, createSample(float64(test.errCounts[1]), i))
	}
	mmr := &mockMetricsReader{
		qrs: map[uint64]metricsreader.QueryResult{
			1: {
				UpdateTime: monotime.Now(),
				Value:      model.Vector(values1),
			},
			2: {
				UpdateTime: monotime.Now(),
				Value:      model.Vector(values2),
			},
		},
	}
	fm := NewFactorError(mmr)
	fm.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.score, backends[i].score(), "test index %d", i)
	}
}

func TestErrorBalance(t *testing.T) {
	tests := []struct {
		errCounts    [][]float64
		scores       []uint64
		balanceCount int
	}{
		{
			errCounts: [][]float64{{math.NaN(), math.NaN()}, {math.NaN(), math.NaN()}},
			scores:    []uint64{2, 2},
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{math.NaN(), math.NaN()}},
			scores: []uint64{1, 2},
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores: []uint64{1, 1},
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold - 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores: []uint64{0, 1},
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].recoverThreshold + 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores: []uint64{2, 1},
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].recoverThreshold + 1)},
				{float64(errDefinitions[0].recoverThreshold - 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores:       []uint64{2, 0},
			balanceCount: 1,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.errCounts))
		values1 := make([]*model.Sample, 0, len(test.errCounts))
		values2 := make([]*model.Sample, 0, len(test.errCounts))
		for j := 0; j < len(test.errCounts); j++ {
			backends = append(backends, createBackend(j, 0, 0))
			values1 = append(values1, createSample(test.errCounts[j][0], j))
			values2 = append(values2, createSample(test.errCounts[j][1], j))
		}
		mmr := &mockMetricsReader{
			qrs: map[uint64]metricsreader.QueryResult{
				1: {
					UpdateTime: monotime.Now(),
					Value:      model.Vector(values1),
				},
				2: {
					UpdateTime: monotime.Now(),
					Value:      model.Vector(values2),
				},
			},
		}
		fm := NewFactorError(mmr)
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
		require.Equal(t, test.balanceCount, balanceCount, "test index %d", i)
	}
}

func TestNoErrorMetrics(t *testing.T) {
	tests := []struct {
		errCounts  [][]float64
		updateTime monotime.Time
	}{
		{
			errCounts: [][]float64{nil, nil},
		},
		{
			errCounts:  [][]float64{{1, 1}, {0, 0}},
			updateTime: monotime.Now().Sub(errMetricExpDuration * 2),
		},
		{
			errCounts:  [][]float64{{math.NaN(), math.NaN()}, {math.NaN(), math.NaN()}},
			updateTime: monotime.Now(),
		},
	}

	mmr := newMockMetricsReader()
	fm := NewFactorError(mmr)
	backends := make([]scoredBackend, 0, 2)
	for i := 0; i < 2; i++ {
		backends = append(backends, createBackend(i, i*100, i*100))
	}
	for i, test := range tests {
		values1 := make([]*model.Sample, 0, len(test.errCounts))
		values2 := make([]*model.Sample, 0, len(test.errCounts))
		for j := 0; j < len(test.errCounts); j++ {
			if test.errCounts[j] != nil {
				values1 = append(values1, createSample(test.errCounts[j][0], j))
				values2 = append(values2, createSample(test.errCounts[j][1], j))
			}
		}
		var v1, v2 model.Vector
		if len(values1) > 0 {
			v1 = model.Vector(values1)
		}
		if len(values2) > 0 {
			v2 = model.Vector(values2)
		}
		mmr.qrs = map[uint64]metricsreader.QueryResult{
			1: {
				UpdateTime: test.updateTime,
				Value:      v1,
			},
			2: {
				UpdateTime: test.updateTime,
				Value:      v2,
			},
		}
		updateScore(fm, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}

func TestErrorBalanceCount(t *testing.T) {
	tests := []struct {
		conns    []int
		minCount int
		maxCount int
	}{
		{
			conns:    []int{1, 0},
			minCount: 1,
			maxCount: 1,
		},
		{
			conns:    []int{10, 0},
			minCount: 1,
			maxCount: 4,
		},
		{
			conns:    []int{10, 10},
			minCount: 1,
			maxCount: 4,
		},
		{
			conns:    []int{100, 10},
			minCount: 5,
			maxCount: 20,
		},
		{
			conns:    []int{1000, 100},
			minCount: 50,
			maxCount: 100,
		},
		{
			conns:    []int{100, 1000},
			minCount: 50,
			maxCount: 100,
		},
		{
			conns:    []int{10000, 10000},
			minCount: 500,
			maxCount: 1000,
		},
	}

	values := []*model.Sample{
		createSample(99999999, 0),
		createSample(0, 1),
	}
	mmr := &mockMetricsReader{
		qrs: map[uint64]metricsreader.QueryResult{
			1: {
				UpdateTime: monotime.Now(),
				Value:      model.Vector(values),
			},
			2: {
				UpdateTime: monotime.Now(),
				Value:      model.Vector(values),
			},
		},
	}
	fe := NewFactorError(mmr)
	for i, test := range tests {
		backends := []scoredBackend{
			createBackend(0, test.conns[0], test.conns[0]),
			createBackend(1, test.conns[1], test.conns[1]),
		}
		fe.UpdateScore(backends)
		count := fe.BalanceCount(backends[0], backends[1])
		require.GreaterOrEqual(t, count, test.minCount, "test idx: %d", i)
		require.LessOrEqual(t, count, test.maxCount, "test idx: %d", i)
	}
}
