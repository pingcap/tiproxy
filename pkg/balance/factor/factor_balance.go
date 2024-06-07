// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"go.uber.org/zap"
)

const (
	maxBitNum = 64
)

var _ policy.BalancePolicy = (*FactorBasedBalance)(nil)

// FactorBasedBalance is the default balance policy.
// It's not concurrency-safe for now.
type FactorBasedBalance struct {
	factors []Factor
	// to reduce memory allocation
	cachedList      []scoredBackend
	mr              metricsreader.MetricsReader
	lg              *zap.Logger
	factorHealth    *FactorHealth
	factorLabel     *FactorLabel
	factorError     *FactorError
	factorMemory    *FactorMemory
	factorCPU       *FactorCPU
	factorLocation  *FactorLocation
	factorConnCount *FactorConnCount
	totalBitNum     int
}

func NewFactorBasedBalance(lg *zap.Logger, mr metricsreader.MetricsReader) *FactorBasedBalance {
	return &FactorBasedBalance{
		lg:         lg,
		mr:         mr,
		cachedList: make([]scoredBackend, 0, 512),
	}
}

// Init creates factors at the first time.
// TODO: create factors according to config and update policy when config changes.
func (fbb *FactorBasedBalance) Init(cfg *config.Config) {
	fbb.factors = make([]Factor, 0, 7)
	fbb.setFactors(cfg)
}

func (fbb *FactorBasedBalance) setFactors(cfg *config.Config) {
	fbb.factors = fbb.factors[:0]

	if fbb.factorHealth == nil {
		fbb.factorHealth = NewFactorHealth()
	}
	fbb.factors = append(fbb.factors, fbb.factorHealth)

	if cfg.Balance.Label.Enable && cfg.Balance.Label.LabelName != "" {
		if fbb.factorLabel == nil {
			fbb.factorLabel = NewFactorLabel()
		}
		fbb.factors = append(fbb.factors, fbb.factorLabel)
	} else if fbb.factorLabel != nil {
		fbb.factorLabel.Close()
		fbb.factorLabel = nil
	}

	if cfg.Balance.Location.Enable {
		if fbb.factorLocation == nil {
			fbb.factorLocation = NewFactorLocation()
		}
	} else if fbb.factorLocation != nil {
		fbb.factorLocation.Close()
		fbb.factorLocation = nil
	}
	if cfg.Balance.Location.Enable && cfg.Balance.Location.LocationFirst {
		fbb.factors = append(fbb.factors, fbb.factorLocation)
	}

	if cfg.Balance.Error.Enable {
		if fbb.factorError == nil {
			fbb.factorError = NewFactorError(fbb.mr)
		}
		fbb.factors = append(fbb.factors, fbb.factorError)
	} else if fbb.factorError != nil {
		fbb.factorError.Close()
		fbb.factorError = nil
	}

	if cfg.Balance.Memory.Enable {
		if fbb.factorMemory == nil {
			fbb.factorMemory = NewFactorMemory(fbb.mr)
		}
		fbb.factors = append(fbb.factors, fbb.factorMemory)
	} else if fbb.factorMemory != nil {
		fbb.factorMemory.Close()
		fbb.factorMemory = nil
	}

	if cfg.Balance.CPU.Enable {
		if fbb.factorCPU == nil {
			fbb.factorCPU = NewFactorCPU(fbb.mr)
		}
		fbb.factors = append(fbb.factors, fbb.factorCPU)
	} else if fbb.factorCPU != nil {
		fbb.factorCPU.Close()
		fbb.factorCPU = nil
	}

	if cfg.Balance.Location.Enable && !cfg.Balance.Location.LocationFirst {
		fbb.factors = append(fbb.factors, fbb.factorLocation)
	}

	if fbb.factorConnCount == nil {
		fbb.factorConnCount = NewFactorConnCount()
	}
	fbb.factors = append(fbb.factors, fbb.factorConnCount)

	err := fbb.updateBitNum()
	if err != nil {
		panic(err.Error())
	}
	for _, factor := range fbb.factors {
		factor.SetConfig(cfg)
	}
}

func (fbb *FactorBasedBalance) updateBitNum() error {
	totalBitNum := 0
	for _, factor := range fbb.factors {
		totalBitNum += factor.ScoreBitNum()
	}
	if totalBitNum > maxBitNum {
		return errors.Errorf("the total bit number of factors is %d", totalBitNum)
	}
	fbb.totalBitNum = totalBitNum
	return nil
}

// updateScore updates backend scores.
func (fbb *FactorBasedBalance) updateScore(backends []policy.BackendCtx) []scoredBackend {
	scoredBackends := fbb.cachedList[:0]
	for _, backend := range backends {
		scoredBackends = append(scoredBackends, newScoredBackend(backend))
	}
	for _, factor := range fbb.factors {
		bitNum := factor.ScoreBitNum()
		for j := 0; j < len(scoredBackends); j++ {
			scoredBackends[j].prepareScore(bitNum)
		}
		factor.UpdateScore(scoredBackends)
	}
	return scoredBackends
}

// BackendToRoute returns the idlest backend.
func (fbb *FactorBasedBalance) BackendToRoute(backends []policy.BackendCtx) policy.BackendCtx {
	if len(backends) == 0 {
		return nil
	}
	if len(backends) == 1 {
		return backends[0]
	}
	scoredBackends := fbb.updateScore(backends)

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
	return idlestBackend.BackendCtx
}

// BackendsToBalance returns the busiest/unhealthy backend and the idlest backend.
// balanceCount: the count of connections to migrate in this round. 0 indicates no need to balance.
// reason: the debug information to be logged.
func (fbb *FactorBasedBalance) BackendsToBalance(backends []policy.BackendCtx) (from, to policy.BackendCtx, balanceCount int, reason []zap.Field) {
	if len(backends) <= 1 {
		return
	}
	scoredBackends := fbb.updateScore(backends)

	// Get the unbalanced backends and their scores.
	var idlestBackend, busiestBackend *scoredBackend
	minScore, maxScore := uint64(1<<maxBitNum-1), uint64(0)
	for i := 0; i < len(scoredBackends); i++ {
		backend := &scoredBackends[i]
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
	if idlestBackend == nil || busiestBackend == nil || idlestBackend == busiestBackend {
		return
	}

	// Get the unbalanced factor and the connection count to migrate.
	var factor Factor
	leftBitNum := fbb.totalBitNum
	for _, factor = range fbb.factors {
		bitNum := factor.ScoreBitNum()
		score1 := maxScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		score2 := minScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		if score1 > score2 {
			// The previous factors are ordered, so this factor won't violate them.
			// E.g.
			// backend1 factor scores: 1, 1
			// backend2 factor scores: 0, 0
			// Balancing the second factor won't make the first factor unbalanced.
			balanceCount = factor.BalanceCount(*busiestBackend, *idlestBackend)
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
	return busiestBackend.BackendCtx, idlestBackend.BackendCtx, balanceCount, fields
}

func (fbb *FactorBasedBalance) SetConfig(cfg *config.Config) {
	fbb.setFactors(cfg)
}

func (fbb *FactorBasedBalance) Close() {
	for _, factor := range fbb.factors {
		factor.Close()
	}
}
