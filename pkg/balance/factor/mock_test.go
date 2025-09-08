// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
)

var _ policy.BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	observer.BackendInfo
	addr      string
	connScore int
	connCount int
	healthy   bool
	local     bool
}

func newMockBackend(healthy bool, connScore int) *mockBackend {
	return &mockBackend{
		healthy:   healthy,
		connScore: connScore,
		connCount: connScore,
	}
}

func (mb *mockBackend) Healthy() bool {
	return mb.healthy
}

func (mb *mockBackend) ConnScore() int {
	return mb.connScore
}

func (mb *mockBackend) Addr() string {
	return mb.addr
}

func (mb *mockBackend) ConnCount() int {
	return mb.connCount
}

func (mb *mockBackend) GetBackendInfo() observer.BackendInfo {
	return mb.BackendInfo
}

func (mb *mockBackend) Local() bool {
	return mb.local
}

func (mb *mockBackend) Keyspace() string {
	return ""
}

var _ Factor = (*mockFactor)(nil)

type mockFactor struct {
	scores       map[string]int
	bitNum       int
	threshold    int
	balanceCount float64
	updateScore  func(backends []scoredBackend)
	cfg          *config.Config
	advice       BalanceAdvice
	canBeRouted  bool
}

func (mf *mockFactor) Name() string {
	return "mock"
}

func (mf *mockFactor) UpdateScore(backends []scoredBackend) {
	mf.updateScore(backends)
	if mf.scores == nil {
		mf.scores = make(map[string]int)
	}
	for _, backend := range backends {
		mf.scores[backend.Addr()] = backend.factorScore(mf.bitNum)
	}
}

func (mf *mockFactor) ScoreBitNum() int {
	return mf.bitNum
}

func (mf *mockFactor) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	fromScore, toScore := mf.scores[from.Addr()], mf.scores[to.Addr()]
	if mf.advice == AdviceNegtive {
		return AdviceNegtive, 0, nil
	}
	if fromScore-toScore > mf.threshold {
		return AdvicePositive, mf.balanceCount, nil
	}
	return AdviceNeutral, 0, nil
}

func (mf *mockFactor) SetConfig(cfg *config.Config) {
	mf.cfg = cfg
}

func (mf *mockFactor) CanBeRouted(score uint64) bool {
	if mf.canBeRouted {
		return true
	}
	return score == 0
}

func (mf *mockFactor) Close() {
}

var _ metricsreader.MetricsReader = (*mockMetricsReader)(nil)

type mockMetricsReader struct {
	qrs map[string]metricsreader.QueryResult
}

func newMockMetricsReader() *mockMetricsReader {
	return &mockMetricsReader{
		qrs: make(map[string]metricsreader.QueryResult),
	}
}

func (mmr *mockMetricsReader) Start(ctx context.Context) error {
	return nil
}

func (mmr *mockMetricsReader) AddQueryExpr(key string, queryExpr metricsreader.QueryExpr, queryRule metricsreader.QueryRule) {
}

func (mmr *mockMetricsReader) RemoveQueryExpr(key string) {
}

func (mmr *mockMetricsReader) GetQueryResult(key string) metricsreader.QueryResult {
	return mmr.qrs[key]
}

func (mmr *mockMetricsReader) GetBackendMetrics() []byte {
	return nil
}

func (mmr *mockMetricsReader) PreClose() {
}

func (mmr *mockMetricsReader) Close() {
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

func createSampleStream(values []float64, backendIdx int, curTime model.Time) *model.SampleStream {
	host := strconv.Itoa(backendIdx)
	labelSet := model.Metric{metricsreader.LabelNameInstance: model.LabelValue(host + ":10080")}
	pairs := make([]model.SamplePair, 0, len(values))
	for i, cpu := range values {
		ts := curTime.Add(15 * time.Second * time.Duration(i-len(values)))
		pairs = append(pairs, model.SamplePair{Timestamp: ts, Value: model.SampleValue(cpu)})
	}
	return &model.SampleStream{Metric: labelSet, Values: pairs}
}

func createPairs(values []float64, ts []model.Time) []model.SamplePair {
	pairs := make([]model.SamplePair, 0, len(values))
	for i, value := range values {
		pairs = append(pairs, model.SamplePair{Timestamp: ts[i], Value: model.SampleValue(value)})
	}
	return pairs
}

func createSample(value float64, backendIdx int) *model.Sample {
	host := strconv.Itoa(backendIdx)
	labelSet := model.Metric{metricsreader.LabelNameInstance: model.LabelValue(host + ":10080")}
	ts := model.Time(time.Now().UnixMilli())
	return &model.Sample{Metric: labelSet, Timestamp: ts, Value: model.SampleValue(value)}
}

func updateScore(fc Factor, backends []scoredBackend) {
	for i := range backends {
		backends[i].scoreBits = 0
	}
	fc.UpdateScore(backends)
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].score() < backends[j].score()
	})
}
