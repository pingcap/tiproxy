// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

const (
	maxBitNum = 64
)

type BalancePolicy interface {
	Init()
	UpdatePolicy()
	BackendToRoute(backends []Backend) Backend
	BackendsToBalance(backends []Backend) (from, to Backend, balanceCount int, reason []zap.Field)
}

var _ BalancePolicy = (*FactorBasedBalance)(nil)

// FactorBasedBalance manages factors.
// It's not concurrency-safe, DO NOT visit it concurrently.
type FactorBasedBalance struct {
	factors []Factor
	lg      *zap.Logger
	// to reduce memory allocation
	cachedList  []scoredBackend
	totalBitNum int
}

func NewFactorBasedBalance(lg *zap.Logger) *FactorBasedBalance {
	return &FactorBasedBalance{
		lg:         lg,
		cachedList: make([]scoredBackend, 0, 512),
	}
}

// Init creates factors at the first time.
// TODO: create factors according to config.
func (fm *FactorBasedBalance) Init() {
	fm.factors = []Factor{
		NewFactorHealth(),
		NewFactorConnCount(),
	}
	err := fm.updateBitNum()
	if err != nil {
		panic(err.Error())
	}
}

// UpdatePolicy updates the factor list according to the changed config.
func (fm *FactorBasedBalance) UpdatePolicy() {

}

func (fm *FactorBasedBalance) updateBitNum() error {
	totalBitNum := 0
	for _, factor := range fm.factors {
		totalBitNum += factor.ScoreBitNum()
	}
	if totalBitNum > maxBitNum {
		return errors.Errorf("the total bit number of factors is %d", totalBitNum)
	}
	fm.totalBitNum = totalBitNum
	return nil
}

// BackendToRoute returns the idlest backend.
func (fm *FactorBasedBalance) BackendToRoute(backends []Backend) Backend {
	if len(backends) == 0 {
		return nil
	}
	if len(backends) == 1 {
		return backends[0]
	}
	scoredBackends := fm.cachedList[:0]
	for _, backend := range backends {
		scoredBackends = append(scoredBackends, newScoredBackend(backend))
	}
	// Update backend scores.
	for _, factor := range fm.factors {
		factor.UpdateScore(scoredBackends)
	}
	// Find the idlest backend.
	idlestBackend := scoredBackends[0]
	minScore := idlestBackend.score()
	for i := 1; i < len(scoredBackends); i++ {
		score := scoredBackends[i].score()
		if score < minScore {
			minScore = score
			idlestBackend = scoredBackends[i]
		}
	}
	return idlestBackend.Backend
}

// BackendsToBalance returns the busiest/unhealthy backend and the idlest backend.
// balanceCount: the count of connections to migrate in this round. 0 indicates no need to balance.
// reason: the debug information to be logged.
func (fm *FactorBasedBalance) BackendsToBalance(backends []Backend) (from, to Backend, balanceCount int, reason []zap.Field) {
	if len(backends) <= 1 {
		return
	}
	// Update backend scores.
	scoredBackends := fm.cachedList[:0]
	for _, backend := range backends {
		scoredBackends = append(scoredBackends, newScoredBackend(backend))
	}
	for _, factor := range fm.factors {
		factor.UpdateScore(scoredBackends)
	}

	// Get the unbalanced backends and their scores.
	var idlestBackend, busiestBackend scoredBackend
	minScore, maxScore := uint64(1<<maxBitNum-1), uint64(0)
	for _, backend := range scoredBackends {
		score := backend.score()
		// Skip the unhealthy backends.
		if score < minScore && backend.Healthy() {
			minScore = score
			idlestBackend = backend
		}
		// Skip the backends without connections.
		if score > maxScore && backend.ConnScore() > 0 {
			maxScore = score
			busiestBackend = backend
		}
	}
	if idlestBackend.Backend == nil || busiestBackend.Backend == nil || idlestBackend.Backend == busiestBackend.Backend {
		return
	}

	// Get the unbalanced factor and the connection count to migrate.
	var factor Factor
	leftBitNum := fm.totalBitNum
	for _, factor = range fm.factors {
		bitNum := factor.ScoreBitNum()
		score1 := maxScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		score2 := minScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		if score1 > score2 {
			// The previous factors are ordered, so this factor won't violate them.
			// E.g.
			// backend1 factor scores: 1, 1
			// backend2 factor scores: 0, 0
			// Balancing the second factor won't make the first factor unbalanced.
			balanceCount = factor.BalanceCount(busiestBackend, idlestBackend)
			if balanceCount > 0 {
				break
			}
		} else if score1 < score2 {
			// Stop it once a factor is in the opposite order, otherwise a subsequent factor may violate this one.
			// E.g.
			// backend1 factor scores: 1, 0, 1
			// backend2 factor scores: 0, 1, 0
			// Balancing the third factor may make the second factor unbalanced, although it's in the same order with the first factor.
			return nil, nil, 0, nil
		}
		leftBitNum -= bitNum
	}
	fields := []zap.Field{
		zap.String("factor", factor.Name()),
		zap.Uint64("from_score", maxScore),
		zap.Uint64("to_score", minScore),
	}
	return busiestBackend.Backend, idlestBackend.Backend, balanceCount, fields
}
