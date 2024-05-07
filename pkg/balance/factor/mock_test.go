// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"context"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
)

var _ policy.BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	observer.BackendInfo
	addr      string
	connScore int
	connCount int
	healthy   bool
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

var _ Factor = (*mockFactor)(nil)

type mockFactor struct {
	bitNum       int
	balanceCount int
	updateScore  func(backends []scoredBackend)
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

var _ metricsreader.MetricsReader = (*mockMetricsReader)(nil)

type mockMetricsReader struct {
	queryID uint64
	qr      metricsreader.QueryResult
}

func newMockMetricsReader() *mockMetricsReader {
	return &mockMetricsReader{}
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
	return mmr.qr
}

func (mmr *mockMetricsReader) Subscribe(receiverName string) <-chan struct{} {
	return nil
}

func (mmr *mockMetricsReader) Unsubscribe(receiverName string) {
}

func (mmr *mockMetricsReader) Close() {
}
