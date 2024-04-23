// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"sort"

	"go.uber.org/zap"
)

const (
	// ConnBalancedRatio is the threshold of ratio of the most connection count and least count.
	// If the ratio exceeds the threshold, we migrate connections.
	ConnBalancedRatio = 1.2
	// BalanceCount4Health indicates how many connections to balance in each round.
	// If some backends are unhealthy, migrate fast but do not put too much pressure on TiDB.
	BalanceCount4Health = 10
)

var _ BalancePolicy = (*SimpleBalancePolicy)(nil)

// SimpleBalancePolicy is used for serverless tier and testing of router.
// It simply balances by health and connection count.
type SimpleBalancePolicy struct {
}

func NewSimpleBalancePolicy() *SimpleBalancePolicy {
	return &SimpleBalancePolicy{}
}

func (sbp *SimpleBalancePolicy) Init() {
}

func (sbp *SimpleBalancePolicy) BackendToRoute(backends []BackendCtx) BackendCtx {
	if len(backends) == 0 {
		return nil
	}
	sortBackends(backends)
	if backends[0].Healthy() {
		return backends[0]
	}
	return nil
}

func (sbp *SimpleBalancePolicy) BackendsToBalance(backends []BackendCtx) (from, to BackendCtx, balanceCount int, reason []zap.Field) {
	if len(backends) <= 1 {
		return
	}
	sortBackends(backends)
	from, to = backends[len(backends)-1], backends[0]
	if !to.Healthy() || from.ConnScore() <= 0 {
		return nil, nil, 0, nil
	}
	if !from.Healthy() {
		balanceCount = BalanceCount4Health
	} else {
		if float64(from.ConnScore()) <= float64(to.ConnScore()+1)*ConnBalancedRatio {
			return nil, nil, 0, nil
		}
		balanceCount = 1
	}
	reason = []zap.Field{
		zap.Bool("from_healthy", from.Healthy()),
		zap.Bool("to_healthy", to.Healthy()),
		zap.Int("from_score", from.ConnScore()),
		zap.Int("to_score", to.ConnScore()),
	}
	return
}

func sortBackends(backends []BackendCtx) {
	sort.Slice(backends, func(i, j int) bool {
		if backends[i].Healthy() && !backends[j].Healthy() {
			return true
		}
		return backends[i].Healthy() == backends[j].Healthy() && backends[i].ConnScore() < backends[j].ConnScore()
	})
}
