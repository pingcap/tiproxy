// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"fmt"
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
			errCounts: []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			score:     0,
		},
		{
			errCounts: []float64{math.NaN(), 0, 0, math.NaN()},
			score:     0,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].failThreshold*100 + 1), 100, float64(errDefinitions[1].failThreshold - 1), 100},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold*100 - 1), 100, float64(errDefinitions[1].recoverThreshold*100 - 1), 100},
			score:     0,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold*100 + 1), 100, float64(errDefinitions[1].recoverThreshold*100 - 1), 100},
			score:     1,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].failThreshold*100 + 1), 100, float64(errDefinitions[1].recoverThreshold*100 - 1), 100},
			score:     2,
		},
		{
			errCounts: []float64{float64(errDefinitions[0].recoverThreshold*100 + 1), 100, float64(errDefinitions[1].recoverThreshold*100 + 1), 100},
			score:     1,
		},
	}

	backends := make([]scoredBackend, 0, len(tests))
	values := make([][]*model.Sample, 4)
	for i, test := range tests {
		backends = append(backends, createBackend(i, 0, 0))
		for indIdx, fv := range test.errCounts {
			values[indIdx] = append(values[indIdx], createSample(fv, i))
		}
	}
	mmr := &mockMetricsReader{
		qrs: make(map[string]metricsreader.QueryResult),
	}
	for i, indicator := range []string{"failure_pd", "total_pd", "failure_tikv", "total_tikv"} {
		mmr.qrs[indicator] = metricsreader.QueryResult{
			UpdateTime: time.Now(),
			Value:      model.Vector(values[i]),
		}
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
			errCounts: [][]float64{{math.NaN(), 0}, {0, math.NaN()}},
			scores:    []uint64{0, 0},
			balanced:  true,
		},
		{
			errCounts: [][]float64{{0, 0}, {0, 0}},
			scores:    []uint64{0, 0},
			balanced:  true,
		},
		{
			errCounts: [][]float64{{10, 0}, {0, 0}},
			scores:    []uint64{2, 0},
			balanced:  false,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold*100 + 1), 100},
				{math.NaN(), math.NaN()}},
			scores:   []uint64{1, 0},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold*100 + 1), 100},
				{float64(errDefinitions[0].recoverThreshold*100 + 1), 100}},
			scores:   []uint64{1, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].recoverThreshold*100 - 1), 100},
				{float64(errDefinitions[0].recoverThreshold*100 + 1), 100}},
			scores:   []uint64{0, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold*100 + 1), 100},
				{float64(errDefinitions[0].recoverThreshold*100 + 1), 100}},
			scores:   []uint64{2, 1},
			balanced: true,
		},
		{
			errCounts: [][]float64{
				{float64(errDefinitions[0].failThreshold*100 + 1), 100},
				{float64(errDefinitions[0].recoverThreshold*100 - 1), 100}},
			scores:   []uint64{2, 0},
			balanced: false,
		},
	}

	for i, test := range tests {
		for _, indicator := range []string{"pd", "tikv"} {
			backends := make([]scoredBackend, 0, len(test.errCounts))
			values1 := make([]*model.Sample, 0, len(test.errCounts))
			values2 := make([]*model.Sample, 0, len(test.errCounts))
			for beIndx := 0; beIndx < len(test.errCounts); beIndx++ {
				backends = append(backends, createBackend(beIndx, 100, 100))
				values1 = append(values1, createSample(test.errCounts[beIndx][0], beIndx))
				values2 = append(values2, createSample(test.errCounts[beIndx][1], beIndx))
			}
			mmr := &mockMetricsReader{
				qrs: map[string]metricsreader.QueryResult{
					fmt.Sprintf("failure_%s", indicator): {
						UpdateTime: time.Now(),
						Value:      model.Vector(values1),
					},
					fmt.Sprintf("total_%s", indicator): {
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
			advice, balanceCount, _ := fh.BalanceCount(from, to)
			require.Equal(t, test.balanced, advice == AdviceNeutral, "test index %d, indicator %s", i, indicator)
			require.Equal(t, test.balanced, balanceCount < 0.0001, "test index %d, indicator %s", i, indicator)
		}
	}
}

func TestNoHealthMetrics(t *testing.T) {
	tests := []struct {
		errCounts  [][]float64
		updateTime time.Time
	}{
		{
			errCounts:  [][]float64{nil, nil},
			updateTime: time.Now(),
		},
		{
			errCounts:  [][]float64{nil, {1, 10000, 0, 10000}},
			updateTime: time.Now(),
		},
		{
			errCounts:  [][]float64{{1, 1, 1, 1}, {0, 0, 0, 0}},
			updateTime: time.Now().Add(-errMetricExpDuration * 2),
		},
		{
			errCounts:  [][]float64{{math.NaN(), math.NaN(), math.NaN(), math.NaN()}, {math.NaN(), math.NaN(), math.NaN(), math.NaN()}},
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
		values := make([][]*model.Sample, 4)
		for beIdx := range test.errCounts {
			if test.errCounts[beIdx] != nil {
				for indIdx := 0; indIdx < len(test.errCounts[beIdx]); indIdx++ {
					values[indIdx] = append(values[indIdx], createSample(test.errCounts[beIdx][indIdx], beIdx))
				}
			}
		}
		vecs := make([]model.Vector, 4)
		for indIdx := range vecs {
			if len(values[indIdx]) > 0 {
				vecs[indIdx] = model.Vector(values[indIdx])
			}
		}
		mmr.qrs = make(map[string]metricsreader.QueryResult)
		for j, ind := range []string{"failure_pd", "total_pd", "failure_tikv", "total_tikv"} {
			mmr.qrs[ind] = metricsreader.QueryResult{
				UpdateTime: test.updateTime,
				Value:      vecs[j],
			}
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
			number = 10000.0
		}
		failureValues := []*model.Sample{
			createSample(number, 0),
			createSample(0, 1),
		}
		totalValues := []*model.Sample{
			createSample(10000, 0),
			createSample(10000, 1),
		}
		mmr.qrs = map[string]metricsreader.QueryResult{
			"failure_pd": {
				UpdateTime: time.Now(),
				Value:      model.Vector(failureValues),
			},
			"total_pd": {
				UpdateTime: time.Now(),
				Value:      model.Vector(totalValues),
			},
		}
	}
	for i, test := range tests {
		updateMmr(test.healthy)
		unhealthyBackend.connScore = test.conn
		fh.UpdateScore(backends)
		if test.count == 0 {
			continue
		}
		advice, count, _ := fh.BalanceCount(backends[0], backends[1])
		require.Equal(t, AdvicePositive, advice)
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
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 10
pd_client_cmd_handle_failed_cmds_duration_seconds_count{type="tso"} 10
pd_client_cmd_handle_cmds_duration_seconds_count{type="tso"} 100
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="Get"} 20
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="Get"} 30
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="BatchGet"} 20
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="BatchGet"} 30
`,
			curValue:   []model.SampleValue{10, 100, 0, 100},
			finalValue: []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN())},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 10
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 20
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 100
pd_client_cmd_handle_failed_cmds_duration_seconds_count{type="tso"} 20
pd_client_cmd_handle_cmds_duration_seconds_count{type="tso"} 200
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="Get"} 50
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="Get"} 50
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="BatchGet"} 50
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="BatchGet"} 50
`,
			curValue:   []model.SampleValue{20, 200, 100, 200},
			finalValue: []model.SampleValue{10, 100, 100, 100},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 10
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 20
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 150
pd_client_cmd_handle_failed_cmds_duration_seconds_count{type="tso"} 20
pd_client_cmd_handle_cmds_duration_seconds_count{type="tso"} 300
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="Get"} 100
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="Get"} 100
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="BatchGet"} 100
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="2",type="BatchGet"} 100
`,
			curValue:   []model.SampleValue{20, 300, 150, 400},
			finalValue: []model.SampleValue{10, 200, 150, 300},
		},
		{
			text: `tidb_tikvclient_backoff_seconds_count{type=""} 0
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 10
pd_client_cmd_handle_failed_cmds_duration_seconds_count{type="tso"} 5
pd_client_cmd_handle_cmds_duration_seconds_count{type="tso"} 50
tidb_tikvclient_request_counter{scope="false",stale_read="false",store="1",type="Get"} 50
`,
			curValue:   []model.SampleValue{5, 50, 10, 50},
			finalValue: []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), 10, model.SampleValue(math.NaN())},
		},
		{
			text:       ``,
			curValue:   []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN())},
			finalValue: []model.SampleValue{model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN()), model.SampleValue(math.NaN())},
		},
	}

	rules := []metricsreader.QueryRule{
		errDefinitions[0].queryFailureRule,
		errDefinitions[0].queryTotalRule,
		errDefinitions[1].queryFailureRule,
		errDefinitions[1].queryTotalRule,
	}
	historyPair := make([][]model.SamplePair, len(rules))
	for i, test := range tests {
		var parser expfmt.TextParser
		mfs, err := parser.TextToMetricFamilies(strings.NewReader(test.text))
		require.NoError(t, err, "case %d", i)
		for j, rule := range rules {
			value := rule.Metric2Value(mfs)
			if math.IsNaN(float64(test.curValue[j])) {
				require.True(t, math.IsNaN(float64(value)), "case %d %d", i, j)
			} else {
				require.Equal(t, test.curValue[j], value, "case %d %d", i, j)
			}
			historyPair[j] = append(historyPair[j], model.SamplePair{Value: value})
			value = rule.Range2Value(historyPair[j])
			if math.IsNaN(float64(test.finalValue[j])) {
				require.True(t, math.IsNaN(float64(value)), "case %d %d", i, j)
			} else {
				require.Equal(t, test.finalValue[j], value, "case %d %d", i, j)
			}
		}
	}
}

func TestMissBackendInHealth(t *testing.T) {
	mmr := newMockMetricsReader()
	fh := NewFactorHealth(mmr, zap.NewNop())
	backends := make([]scoredBackend, 0, 2)
	errors := []float64{10000, 0}
	var failureValues, totalValues []*model.Sample
	for i := 0; i < 2; i++ {
		backends = append(backends, createBackend(i, 100, 100))
		failureValues = append(failureValues, createSample(errors[i], i))
		totalValues = append(totalValues, createSample(10000, i))
	}
	mmr.qrs["failure_pd"] = metricsreader.QueryResult{
		UpdateTime: time.Now(),
		Value:      model.Vector(failureValues),
	}
	mmr.qrs["total_pd"] = metricsreader.QueryResult{
		UpdateTime: time.Now(),
		Value:      model.Vector(totalValues),
	}
	fh.UpdateScore(backends)
	advice, count, _ := fh.BalanceCount(backends[0], backends[1])
	require.Equal(t, AdvicePositive, advice)
	require.Equal(t, 100/balanceSeconds4Health, count)

	// Miss the first backend but the snapshot should be preserved.
	fh.UpdateScore(backends[1:])
	unhealthyBackend := backends[0].BackendCtx.(*mockBackend)
	unhealthyBackend.connScore = 50
	fh.UpdateScore(backends)
	advice, count, _ = fh.BalanceCount(backends[0], backends[1])
	require.Equal(t, AdvicePositive, advice)
	require.Equal(t, 100/balanceSeconds4Health, count)
}
