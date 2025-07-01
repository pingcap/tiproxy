// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"sort"
	"strconv"
	"sync"
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
type FactorBasedBalance struct {
	sync.Mutex
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
	fbb.Lock()
	defer fbb.Unlock()
	fbb.factors = make([]Factor, 0, 7)
	fbb.setFactors(cfg)
}

func (fbb *FactorBasedBalance) setFactors(cfg *config.Config) {
	fbb.factors = fbb.factors[:0]

	if cfg.Balance.LabelName != "" {
		if fbb.factorLabel == nil {
			fbb.factorLabel = NewFactorLabel()
		}
		fbb.factors = append(fbb.factors, fbb.factorLabel)
	} else if fbb.factorLabel != nil {
		fbb.factorLabel.Close()
		fbb.factorLabel = nil
	}

	if fbb.factorStatus == nil {
		fbb.factorStatus = NewFactorStatus(fbb.lg.Named("status"))
	}
	fbb.factors = append(fbb.factors, fbb.factorStatus)

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

// updateScore updates backend scores and return the sorted backends.
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
	sort.Slice(scoredBackends, func(i int, j int) bool {
		return scoredBackends[i].scoreBits < scoredBackends[j].scoreBits
	})
	return scoredBackends
}

// BackendToRoute returns one backend to route a new connection to.
func (fbb *FactorBasedBalance) BackendToRoute(backends []policy.BackendCtx) policy.BackendCtx {
	fields := []zap.Field{zap.Int("backend_num", len(backends))}
	defer func() {
		fbb.lg.Debug("route", fields...)
	}()

	if len(backends) == 0 {
		return nil
	}

	fbb.Lock()
	defer fbb.Unlock()
	scoredBackends := fbb.updateScore(backends)
	for _, backend := range scoredBackends {
		fields = append(fields, zap.String(backend.Addr(), strconv.FormatUint(backend.scoreBits, 16)))
	}
	if !fbb.canBeRouted(scoredBackends[0].scoreBits) {
		return nil
	}
	if len(backends) == 1 {
		fields = append(fields, zap.String("target", backends[0].Addr()))
		return backends[0]
	}

	// Evict the backends that are so busy that it should migrate connections to another, and then randomly choose one.
	// Always choosing the idlest one works badly for short connections because even a little jitter may cause all the connections
	// in the next second route to the same backend.
	idxes := make([]int, 0, len(scoredBackends))
	for i := len(scoredBackends) - 1; i > 0; i-- {
		leftBitNum := fbb.totalBitNum
		var balanceCount float64
		for _, factor := range fbb.factors {
			bitNum := factor.ScoreBitNum()
			score1 := scoredBackends[i].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			score2 := scoredBackends[0].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			if score1 > score2 {
				var balanceFields []zap.Field
				var advice BalanceAdvice
				advice, balanceCount, balanceFields = factor.BalanceCount(scoredBackends[i], scoredBackends[0])
				if advice == AdvicePositive && balanceCount > 0.0001 {
					// This backend is too busy. If it's routed, migration may happen.
					fields = append(fields, zap.String(scoredBackends[i].Addr(), factor.Name()))
					fields = append(fields, balanceFields...)
					break
				}
			} else if score1 < score2 {
				break
			}
			leftBitNum -= bitNum
		}
		if balanceCount <= 0.0001 {
			idxes = append(idxes, i)
		}
	}
	idxes = append(idxes, 0)

	// For the rest backends, choose a random one.
	idx := 0
	if len(idxes) > 1 {
		// math/rand can not pass security scanning while crypto/rand is too slow for short connections, so use the current time as a seed.
		// Some platforms only produce microseconds, so use microseconds.
		idx = idxes[int(time.Now().UnixMicro()%int64(len(idxes)))]
	}
	fields = append(fields, zap.String("target", scoredBackends[idx].Addr()), zap.Ints("rand", idxes))
	return scoredBackends[idx].BackendCtx
}

// BackendsToBalance returns the busiest/unhealthy backend and the idlest backend.
// balanceCount: the count of connections to migrate in this round. 0 indicates no need to balance.
// reason: the debug information to be logged.
func (fbb *FactorBasedBalance) BackendsToBalance(backends []policy.BackendCtx) (from, to policy.BackendCtx, balanceCount float64, reason string, logFields []zap.Field) {
	if len(backends) <= 1 {
		return
	}

	fbb.Lock()
	defer fbb.Unlock()
	scoredBackends := fbb.updateScore(backends)
	if !fbb.canBeRouted(scoredBackends[0].scoreBits) {
		return
	}
	if scoredBackends[0].scoreBits == scoredBackends[len(scoredBackends)-1].scoreBits {
		return
	}

	// Iterate over all the backends until find unbalance.
	// Even if the busiest backend can not migrate to the idlest one, the second busiest one may migrate.
	// E.g. the factor scores of 3 backends are [1, 0], [0, 100], [0, 0].
	// The first one is the busiest but not busy enough to migrate to the third one, but the second one is.
	for i := len(scoredBackends) - 1; i > 0; i-- {
		// Skip the backends without connections.
		if scoredBackends[i].ConnCount() <= 0 || scoredBackends[i].ConnScore() <= 0 {
			continue
		}
		leftBitNum := fbb.totalBitNum
		var factorFields []zap.Field
		for _, factor := range fbb.factors {
			bitNum := factor.ScoreBitNum()
			score1 := scoredBackends[i].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			score2 := scoredBackends[0].scoreBits << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
			if score1 >= score2 {
				// The factors with higher priorities are ordered, so this factor shouldn't violate them.
				// E.g. if the CPU usage of A is higher than B, don't migrate from B to A even if A is preferred in location.
				var advice BalanceAdvice
				var fields []zap.Field
				advice, balanceCount, fields = factor.BalanceCount(scoredBackends[i], scoredBackends[0])
				if advice == AdviceNegtive {
					// If the factor will be unbalanced after migration, skip the rest factors.
					// E.g. if the CPU usage of A will be much higher than B after migration,
					// don't migrate from B to A even if A is preferred in location.
					break
				}
				factorFields = append(factorFields, fields...)
				if score1 > score2 && advice == AdvicePositive && balanceCount > 0.0001 {
					from, to = scoredBackends[i].BackendCtx, scoredBackends[0].BackendCtx
					reason = factor.Name()
					logFields = append(factorFields, zap.String("factor", reason),
						zap.String("from_total_score", strconv.FormatUint(scoredBackends[i].scoreBits, 16)),
						zap.String("to_total_score", strconv.FormatUint(scoredBackends[0].scoreBits, 16)),
						zap.Uint64("from_factor_score", score1),
						zap.Uint64("to_factor_score", score2),
						zap.Float64("balance_count", balanceCount))
					return
				}
			} else if score1 < score2 {
				// Stop it once a factor is in the opposite order, otherwise a subsequent factor may violate this one.
				// E.g. the factor scores of 2 backends are [1, 0, 1], [0, 1, 0]
				// Balancing the third factor may make the second factor unbalanced, although it's in the same order with the first factor.
				break
			}
			leftBitNum -= bitNum
		}
	}
	return
}

func (fbb *FactorBasedBalance) canBeRouted(score uint64) bool {
	leftBitNum := fbb.totalBitNum
	for _, factor := range fbb.factors {
		bitNum := factor.ScoreBitNum()
		score := score << (maxBitNum - leftBitNum) >> (maxBitNum - bitNum)
		if !factor.CanBeRouted(score) {
			return false
		}
		leftBitNum -= bitNum
	}
	return true
}

func (fbb *FactorBasedBalance) SetConfig(cfg *config.Config) {
	fbb.Lock()
	defer fbb.Unlock()
	fbb.setFactors(cfg)
}

func (fbb *FactorBasedBalance) Close() {
	fbb.Lock()
	defer fbb.Unlock()
	for _, factor := range fbb.factors {
		factor.Close()
	}
}
