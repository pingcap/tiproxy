// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestHealthScore(t *testing.T) {
	tests := []struct {
		errCounts []float64
		score     uint64
	}{
		{
			errCounts: []float64{math.NaN(), math.NaN()},
			score:     0,
		},
		{
			errCounts: []float64{math.NaN(), 0},
			score:     0,
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
		qrs: map[string]metricsreader.QueryResult{
			"health_pd": {
				UpdateTime: time.Now(),
				Value:      model.Vector(values1),
			},
			"health_tikv": {
				UpdateTime: time.Now(),
				Value:      model.Vector(values2),
			},
		},
	}
	fh := NewFactorHealth(mmr, zap.NewNop())
	fh.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.score, backends[i].score(), "test index %d", i)
	}
}

func TestHealthBalance(t *testing.T) {
	tests := []struct {
		errCounts [][]float64
		scores    []uint64
		balanced  bool
	}{
		{
			errCounts: [][]float64{{math.NaN(), math.NaN()}, {math.NaN(), math.NaN()}},
			scores:    []uint64{0, 0},
			balanced:  true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{math.NaN(), math.NaN()}},
			scores:   []uint64{1, 0},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores:   []uint64{1, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold - 1), float64(errDefinitions[1].recoverThreshold - 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores:   []uint64{0, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].recoverThreshold + 1)},
				{float64(errDefinitions[0].recoverThreshold + 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores:   []uint64{2, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold + 1), float64(errDefinitions[1].recoverThreshold + 1)},
				{float64(errDefinitions[0].recoverThreshold - 1), float64(errDefinitions[1].recoverThreshold - 1)}},
			scores:   []uint64{2, 0},
			balanced: false,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, len(test.errCounts))
		values1 := make([]*model.Sample, 0, len(test.errCounts))
		values2 := make([]*model.Sample, 0, len(test.errCounts))
		for j := 0; j < len(test.errCounts); j++ {
			backends = append(backends, createBackend(j, 100, 100))
			values1 = append(values1, createSample(test.errCounts[j][0], j))
			values2 = append(values2, createSample(test.errCounts[j][1], j))
		}
		mmr := &mockMetricsReader{
			qrs: map[string]metricsreader.QueryResult{
				"health_pd": {
					UpdateTime: time.Now(),
					Value:      model.Vector(values1),
				},
				"health_tikv": {
					UpdateTime: time.Now(),
					Value:      model.Vector(values2),
				},
			},
		}
		fh := NewFactorHealth(mmr, zap.NewNop())
		fh.UpdateScore(backends)
		scores := make([]uint64, 0, len(backends))
		for _, backend := range backends {
			scores = append(scores, backend.score())
		}
		require.Equal(t, test.scores, scores, "test index %d", i)
		sort.Slice(backends, func(i, j int) bool {
			return backends[i].score() < backends[j].score()
		})
		from, to := backends[len(backends)-1], backends[0]
		balanceCount := fh.BalanceCount(from, to)
		require.Equal(t, test.balanced, balanceCount < 0.0001, "test index %d", i)
	}
}

func TestNoHealthMetrics(t *testing.T) {
	tests := []struct {
		errCounts  [][]float64
		updateTime time.Time
	}{
		{
			errCounts: [][]float64{nil, nil},
		},
		{
			errCounts:  [][]float64{{1, 1}, {0, 0}},
			updateTime: time.Now().Add(-errMetricExpDuration * 2),
		},
		{
			errCounts:  [][]float64{{math.NaN(), math.NaN()}, {math.NaN(), math.NaN()}},
			updateTime: time.Now(),
		},
	}

	mmr := newMockMetricsReader()
	fh := NewFactorHealth(mmr, zap.NewNop())
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
		mmr.qrs = map[string]metricsreader.QueryResult{
			"health_pd": {
				UpdateTime: test.updateTime,
				Value:      v1,
			},
			"health_tikv": {
				UpdateTime: test.updateTime,
				Value:      v2,
			},
		}
		updateScore(fh, backends)
		require.Equal(t, backends[0].score(), backends[1].score(), "test index %d", i)
	}
}

func TestHealthBalanceCount(t *testing.T) {
	tests := []struct {
		conn    int
		healthy bool
		count   float64
	}{
		{
			conn:    100,
			healthy: false,
			count:   100 / balanceSeconds4Health,
		},
		{
			conn:    50,
			healthy: false,
			count:   100 / balanceSeconds4Health,
		},
		{
			conn:    100,
			healthy: true,
			count:   0,
		},
		{
			conn:    50,
			healthy: false,
			count:   50 / balanceSeconds4Health,
		},
	}

	backends := make([]scoredBackend, 0, 2)
	backends = append(backends, createBackend(0, 0, 0))
	backends = append(backends, createBackend(1, 0, 0))
	unhealthyBackend := backends[0].BackendCtx.(*mockBackend)
	mmr := &mockMetricsReader{
		qrs: map[string]metricsreader.QueryResult{},
	}
	fh := NewFactorHealth(mmr, zap.NewNop())
	updateMmr := func(healthy bool) {
		number := 0.0
		if !healthy {
			number = 99999999.0
		}
		values := []*model.Sample{
			createSample(number, 0),
			createSample(0, 1),
		}
		mmr.qrs["health_pd"] = metricsreader.QueryResult{
			UpdateTime: time.Now(),
			Value:      model.Vector(values),
		}
		mmr.qrs["health_tikv"] = mmr.qrs["health_pd"]
	}
	for i, test := range tests {
		updateMmr(test.healthy)
		unhealthyBackend.connScore = test.conn
		fh.UpdateScore(backends)
		if test.count == 0 {
			continue
		}
		count := fh.BalanceCount(backends[0], backends[1])
		require.Equal(t, test.count, count, "test idx: %d", i)
	}
}

func TestHealthQueryRule(t *testing.T) {
	tests := []struct {
		text       string
		curValue   []model.SampleValue
		finalValue []model.SampleValue
	}{
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 0
tidb_tikvclient_backoff_seconds_count{type="dataNotReady"} 0
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 10
tidb_tikvclient_backoff_seconds_count{type="regionMiss"} 10
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 0
`,
			curValue:   []model.SampleValue{10, 10},
			finalValue: []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN())},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 10
tidb_tikvclient_backoff_seconds_count{type="dataNotReady"} 10
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 20
tidb_tikvclient_backoff_seconds_count{type="regionMiss"} 110
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 100
`,
			curValue:   []model.SampleValue{20, 210},
			finalValue: []model.SampleValue{10, 200},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 10
tidb_tikvclient_backoff_seconds_count{type="dataNotReady"} 10
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 20
tidb_tikvclient_backoff_seconds_count{type="regionMiss"} 110
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 100
`,
			curValue:   []model.SampleValue{20, 210},
			finalValue: []model.SampleValue{10, 200},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 0
tidb_tikvclient_backoff_seconds_count{type="dataNotReady"} 0
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 0
tidb_tikvclient_backoff_seconds_count{type="regionMiss"} 5
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 0
`,
			curValue:   []model.SampleValue{0, 5},
			finalValue: []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN())},
		},
	}

	historyPair := make([][]model.SamplePair, len(errDefinitions))
	for i, test := range tests {
		var parser expfmt.TextParser
		mfs, err := parser.TextToMetricFamilies(strings.NewReader(test.text))
		require.NoError(t, err, "case %d", i)
		for j, ed := range errDefinitions {
			value := ed.queryRule.Metric2Value(mfs)
			require.Equal(t, test.curValue[j], value, "case %d %d", i, j)
			historyPair[j] = append(historyPair[j], model.SamplePair{Value: value})
			value = ed.queryRule.Range2Value(historyPair[j])
			if math.IsNaN(float64(test.finalValue[j])) {
				require.True(t, math.IsNaN(float64(value)), "case %d %d", i, j)
			} else {
				require.Equal(t, test.finalValue[j], value, "case %d %d", i, j)
			}
		}
	}
}
