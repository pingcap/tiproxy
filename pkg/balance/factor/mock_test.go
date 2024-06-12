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

var _ Factor = (*mockFactor)(nil)

type mockFactor struct {
	bitNum       int
	balanceCount int
	updateScore  func(backends []scoredBackend)
	cfg          *config.Config
}

func (mf *mockFactor) Name() string {
	return "mock"
}

func (mf *mockFactor) UpdateScore(backends []scoredBackend) {
	mf.updateScore(backends)
}

func (mf *mockFactor) ScoreBitNum() int {
	return mf.bitNum
}

func (mf *mockFactor) BalanceCount(from, to scoredBackend) int {
	return mf.balanceCount
}

func (mf *mockFactor) SetConfig(cfg *config.Config) {
	mf.cfg = cfg
}

func (mf *mockFactor) Close() {
}

var _ metricsreader.MetricsReader = (*mockMetricsReader)(nil)

type mockMetricsReader struct {
	queryID uint64
	qrs     map[uint64]metricsreader.QueryResult
}

func newMockMetricsReader() *mockMetricsReader {
	return &mockMetricsReader{
		qrs: make(map[uint64]metricsreader.QueryResult),
	}
}

func (mmr *mockMetricsReader) Start(ctx context.Context) {
}

func (mmr *mockMetricsReader) AddQueryExpr(queryExpr metricsreader.QueryExpr) uint64 {
	mmr.queryID++
	return mmr.queryID
}

func (mmr *mockMetricsReader) RemoveQueryExpr(id uint64) {
}

func (mmr *mockMetricsReader) GetQueryResult(id uint64) metricsreader.QueryResult {
	return mmr.qrs[id]
}

func (mmr *mockMetricsReader) Subscribe(receiverName string) <-chan struct{} {
	return nil
}

func (mmr *mockMetricsReader) Unsubscribe(receiverName string) {
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

func createSampleStream(values []float64, backendIdx int) *model.SampleStream {
	host := strconv.Itoa(backendIdx)
	labelSet := model.Metric{metricsreader.LabelNameInstance: model.LabelValue(host + ":10080")}
	pairs := make([]model.SamplePair, 0, len(values))
	for i, cpu := range values {
		ts := model.Time(time.Now().UnixMilli() - int64(15000*(len(values)-i)))
		pairs = append(pairs, model.SamplePair{Timestamp: ts, Value: model.SampleValue(cpu)})
	}
	return &model.SampleStream{Metric: labelSet, Values: pairs}
}

func createSample(value float64, backendIdx int) *model.Sample {
	host := strconv.Itoa(backendIdx)
	labelSet := model.Metric{metricsreader.LabelNameInstance: model.LabelValue(host + ":10080")}
	ts := model.Time(time.Now().UnixMilli())
	return &model.Sample{Metric: labelSet, Timestamp: ts, Value: model.SampleValue(value)}
}

func updateScore(fc Factor, backends []scoredBackend) {
	for i := 0; i < len(backends); i++ {
		backends[i].scoreBits = 0
	}
	fc.UpdateScore(backends)
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].score() < backends[j].score()
	})
}
