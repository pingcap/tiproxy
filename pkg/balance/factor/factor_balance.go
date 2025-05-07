// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"crypto/rand"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

const (
	maxBitNum            = 64
	updateMetricInterval = 10 * time.Second
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
	factorStatus    *FactorStatus
	factorLabel     *FactorLabel
	factorHealth    *FactorHealth
	factorMemory    *FactorMemory
	factorCPU       *FactorCPU
	factorLocation  *FactorLocation
	factorConnCount *FactorConnCount
	totalBitNum     int
	lastMetricTime  time.Time
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

	if fbb.factorStatus == nil {
		fbb.factorStatus = NewFactorStatus(fbb.lg.Named("status"))
	}
	fbb.factors = append(fbb.factors, fbb.factorStatus)

	if cfg.Balance.LabelName != "" {
		if fbb.factorLabel == nil {
			fbb.factorLabel = NewFactorLabel()
		}
		fbb.factors = append(fbb.factors, fbb.factorLabel)
	} else if fbb.factorLabel != nil {
		fbb.factorLabel.Close()
		fbb.factorLabel = nil
	}

	switch cfg.Balance.Policy {
	case config.BalancePolicyResource, config.BalancePolicyLocation:
		if fbb.factorLocation == nil {
			fbb.factorLocation = NewFactorLocation()
		}
		if fbb.factorHealth == nil {
			fbb.factorHealth = NewFactorHealth(fbb.mr, fbb.lg.Named("health"))
		}
		if fbb.factorMemory == nil {
			fbb.factorMemory = NewFactorMemory(fbb.mr, fbb.lg.Named("memory"))
		}
		if fbb.factorCPU == nil {
			fbb.factorCPU = NewFactorCPU(fbb.mr, fbb.lg.Named("cpu"))
		}
	default:
		if fbb.factorLocation != nil {
			fbb.factorLocation.Close()
			fbb.factorLocation = nil
		}
		if fbb.factorHealth != nil {
			fbb.factorHealth.Close()
			fbb.factorHealth = nil
		}
		if fbb.factorMemory != nil {
			fbb.factorMemory.Close()
			fbb.factorMemory = nil
		}
		if fbb.factorCPU != nil {
			fbb.factorCPU.Close()
			fbb.factorCPU = nil
		}
	}

	switch cfg.Balance.Policy {
	case config.BalancePolicyResource:
		fbb.factors = append(fbb.factors, fbb.factorHealth, fbb.factorMemory, fbb.factorCPU, fbb.factorLocation)
	case config.BalancePolicyLocation:
		fbb.factors = append(fbb.factors, fbb.factorLocation, fbb.factorHealth, fbb.factorMemory, fbb.factorCPU)
	}

	if fbb.factorConnCount == nil {
		fbb.factorConnCount = NewFactorConnCount()
	}
	fbb.factors = append(fbb.factors, fbb.factorConnCount)

	err := fbb.updateBitNum()
	if err != nil {
		panic(err.Error())
	}
	metrics.BackendScoreGauge.Reset()
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
		scoredBackends = append(scoredBackends, newScoredBackend(backend, fbb.lg))
	}
	needUpdateMetric := false
	now := time.Now()
	if now.Sub(fbb.lastMetricTime) > updateMetricInterval {
		needUpdateMetric = true
		fbb.lastMetricTime = now
	}
	for _, factor := range fbb.factors {
		bitNum := factor.ScoreBitNum()
		for j := 0; j < len(scoredBackends); j++ {
			scoredBackends[j].prepareScore(bitNum)
		}
		factor.UpdateScore(scoredBackends)
		if needUpdateMetric {
			for j := 0; j < len(scoredBackends); j++ {
				metrics.BackendScoreGauge.WithLabelValues(backends[j].Addr(), factor.Name()).Set(float64(scoredBackends[j].factorScore(bitNum)))
			}
		}
	}
	return scoredBackends
}

// BackendToRoute returns one backend to route a new connection to.
func (fbb *FactorBasedBalance) BackendToRoute(backends []policy.BackendCtx) policy.BackendCtx {
	if len(backends) == 0 {
		return nil
	}
	if len(backends) == 1 {
		return backends[0]
	}

	scoredBackends := fbb.updateScore(backends)
	sort.Slice(scoredBackends, func(i int, j int) bool {
		return scoredBackends[i].scoreBits < scoredBackends[j].scoreBits
	})

	var fields []zap.Field
	for _, backend := range scoredBackends {
		fields = append(fields, zap.String(backend.Addr(), strconv.FormatUint(backend.scoreBits, 16)))
	}

	// Evict the backends that are so busy that it should migrate connections to another, and then randomly choose one.
	// Always choosing the idlest one works badly for short connections because even a little jitter may cause all the connections
	// in the next second route to the same backend.
	endIdx := len(scoredBackends)
	for i := len(scoredBackends) - 1; i > 0; i-- {
		leftBitNum := fbb.totalBitNum
		for _, factor := range fbb.factors {
			bitNum := factor.ScoreBitNum()
			score1 := scoredBackends[i].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			score2 := scoredBackends[0].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			if score1 > score2 {
				// This backend is too busy. If it's routed, migration may happen.
				if balanceCount, balanceFields := factor.BalanceCount(scoredBackends[i], scoredBackends[0]); balanceCount > 0.0001 {
					fields = append(fields, zap.String(scoredBackends[i].Addr(), factor.Name()))
					fields = append(fields, balanceFields...)
					endIdx = i
					break
				}
			} else if score1 < score2 {
				break
			}
			leftBitNum -= bitNum
		}
		// The backend is not evicted in this round, stop.
		if endIdx > i {
			break
		}
	}

	// For the rest backends, choose a random one.
	idx := 0
	if endIdx > 1 {
		// math/rand is faster but can't pass security scanning tools.
		bigInt, _ := rand.Int(rand.Reader, big.NewInt(int64(endIdx)))
		idx = int(bigInt.Int64())
	}
	fields = append(fields, zap.String("target", scoredBackends[idx].Addr()), zap.Int("rand_num", endIdx))
	fbb.lg.Debug("route", fields...)
	return scoredBackends[idx].BackendCtx
}

// BackendsToBalance returns the busiest/unhealthy backend and the idlest backend.
// balanceCount: the count of connections to migrate in this round. 0 indicates no need to balance.
// reason: the debug information to be logged.
func (fbb *FactorBasedBalance) BackendsToBalance(backends []policy.BackendCtx) (from, to policy.BackendCtx, balanceCount float64, reason string, logFields []zap.Field) {
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
		if score > maxScore && backend.ConnScore() > 0 && backend.ConnCount() > 0 {
			maxScore = score
			busiestBackend = backend
		}
	}
	if idlestBackend == nil || busiestBackend == nil || idlestBackend == busiestBackend {
		return
	}

	// Get the unbalanced factor and the connection count to migrate.
	var factor Factor
	var fields []zap.Field
	var score1, score2 uint64
	leftBitNum := fbb.totalBitNum
	for _, factor = range fbb.factors {
		bitNum := factor.ScoreBitNum()
		score1 = maxScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		score2 = minScore << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		if score1 > score2 {
			// The previous factors are ordered, so this factor won't violate them.
			// E.g.
			// backend1 factor scores: 1, 1
			// backend2 factor scores: 0, 0
			// Balancing the second factor won't make the first factor unbalanced.
			balanceCount, fields = factor.BalanceCount(*busiestBackend, *idlestBackend)
			if balanceCount > 0.0001 {
				break
			}
		} else if score1 < score2 {
			// Stop it once a factor is in the opposite order, otherwise a subsequent factor may violate this one.
			// E.g.
			// backend1 factor scores: 1, 0, 1
			// backend2 factor scores: 0, 1, 0
			// Balancing the third factor may make the second factor unbalanced, although it's in the same order with the first factor.
			return
		}
		leftBitNum -= bitNum
	}
	reason = factor.Name()
	fields = append(fields, zap.String("factor", reason),
		zap.String("from_total_score", strconv.FormatUint(maxScore, 16)),
		zap.String("to_total_score", strconv.FormatUint(minScore, 16)),
		zap.Uint64("from_factor_score", score1),
		zap.Uint64("to_factor_score", score2),
		zap.Float64("balance_count", balanceCount))
	return busiestBackend.BackendCtx, idlestBackend.BackendCtx, balanceCount, reason, fields
}

func (fbb *FactorBasedBalance) SetConfig(cfg *config.Config) {
	fbb.setFactors(cfg)
}

func (fbb *FactorBasedBalance) Close() {
	for _, factor := range fbb.factors {
		factor.Close()
	}
}
