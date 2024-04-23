// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import "github.com/pingcap/tiproxy/pkg/balance/policy"

var _ policy.BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	healthy   bool
	connScore int
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
