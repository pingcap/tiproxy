// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
)

const (
	errMetricExpDuration = 1 * time.Minute
	balanceCount4Err     = 10
)

type valueRange int

const (
	// (-inf, recoverThreshold]
	valueRangeNormal valueRange = iota
	// (recoverThreshold, failThreshold)
	valueRangeMid
	// [failThreshold, +inf)
	valueRangeAbnormal
)

var _ Factor = (*FactorError)(nil)

// The snapshot of backend statistics when the metric was updated.
type errBackendSnapshot struct {
	updatedTime monotime.Time
	valueRange  valueRange
}

type errIndicator struct {
	queryExpr        metricsreader.QueryExpr
	queryResult      metricsreader.QueryResult
	queryID          uint64
	failThreshold    int
	recoverThreshold int
}

type FactorError struct {
	snapshot   map[string]errBackendSnapshot
	indicators []errIndicator
	mr         metricsreader.MetricsReader
	bitNum     int
}

func NewFactorError(mr metricsreader.MetricsReader, cfg *config.Config) *FactorError {
	return &FactorError{
		mr:         mr,
		snapshot:   make(map[string]errBackendSnapshot),
		indicators: initErrIndicator(mr, cfg),
		bitNum:     2,
	}
}

func initErrIndicator(mr metricsreader.MetricsReader, cfg *config.Config) []errIndicator {
	indicatorCfgs := cfg.Balance.Error.Indicators
	indicators := make([]errIndicator, 0, len(indicatorCfgs))
	for _, indicatorDef := range indicatorCfgs {
		indicator := errIndicator{
			queryExpr: metricsreader.QueryExpr{
				PromQL: indicatorDef.QueryExpr,
			},
			failThreshold:    indicatorDef.FailThreshold,
			recoverThreshold: indicatorDef.RecoverThreshold,
		}
		indicator.queryID = mr.AddQueryExpr(indicator.queryExpr)
		indicators = append(indicators, indicator)
	}
	return indicators
}

func (fe *FactorError) Name() string {
	return "error"
}

func (fe *FactorError) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	needUpdateSnapshot, latestTime := false, monotime.Time(0)
	for i := 0; i < len(fe.indicators); i++ {
		qr := fe.mr.GetQueryResult(fe.indicators[i].queryID)
		if qr.Err != nil || qr.Empty() {
			continue
		}
		if fe.indicators[i].queryResult.UpdateTime != qr.UpdateTime {
			fe.indicators[i].queryResult = qr
			needUpdateSnapshot = true
		}
		if qr.UpdateTime > latestTime {
			latestTime = qr.UpdateTime
		}
	}
	if monotime.Since(latestTime) > errMetricExpDuration {
		// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
		return
	}
	if needUpdateSnapshot {
		fe.updateSnapshot(backends)
	}
	for i := 0; i < len(backends); i++ {
		score := fe.caclErrScore(backends[i].Addr())
		backends[i].addScore(score, fe.bitNum)
	}
}

func (fe *FactorError) updateSnapshot(backends []scoredBackend) {
	snapshots := make(map[string]errBackendSnapshot, len(fe.snapshot))
	for _, backend := range backends {
		// Get the current value range.
		updatedTime, valueRange := monotime.Time(0), valueRangeNormal
		for i := 0; i < len(fe.indicators); i++ {
			ts := fe.indicators[i].queryResult.UpdateTime
			if monotime.Since(ts) > errMetricExpDuration {
				// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
				continue
			}
			if ts > updatedTime {
				updatedTime = ts
			}
			sample := fe.indicators[i].queryResult.GetSample4Backend(backend)
			vr := calcValueRange(sample, fe.indicators[i])
			if vr > valueRange {
				valueRange = vr
			}
		}
		// If the metric is unavailable, try to reuse the latest one.
		addr := backend.Addr()
		snapshot, existSnapshot := fe.snapshot[addr]
		if updatedTime == monotime.Time(0) {
			if existSnapshot && monotime.Since(snapshot.updatedTime) < errMetricExpDuration {
				snapshots[addr] = snapshot
			}
			continue
		}
		snapshots[addr] = errBackendSnapshot{
			updatedTime: updatedTime,
			valueRange:  valueRange,
		}
	}
	fe.snapshot = snapshots
}

func calcValueRange(sample *model.Sample, indicator errIndicator) valueRange {
	if sample == nil {
		return valueRangeAbnormal
	}
	if math.IsNaN(float64(sample.Value)) {
		return valueRangeAbnormal
	}
	value := int(sample.Value)
	if indicator.failThreshold > indicator.recoverThreshold {
		switch {
		case value <= indicator.recoverThreshold:
			return valueRangeNormal
		case value >= indicator.failThreshold:
			return valueRangeAbnormal
		}
	} else {
		switch {
		case value >= indicator.recoverThreshold:
			return valueRangeNormal
		case value <= indicator.failThreshold:
			return valueRangeAbnormal
		}
	}
	return valueRangeMid
}

func (fe *FactorError) caclErrScore(addr string) int {
	snapshot, ok := fe.snapshot[addr]
	if !ok {
		return 2
	}
	switch snapshot.valueRange {
	case valueRangeNormal:
		return 0
	case valueRangeMid:
		return 1
	default:
		return 2
	}
}

func (fe *FactorError) ScoreBitNum() int {
	return fe.bitNum
}

func (fe *FactorError) BalanceCount(from, to scoredBackend) int {
	// Only migrate connections when one is valueRangeNormal and the other is valueRangeAbnormal.
	fromScore := fe.caclErrScore(from.Addr())
	toScore := fe.caclErrScore(to.Addr())
	if fromScore-toScore > 1 {
		return balanceCount4Err
	}
	return 0
}

func (fe *FactorError) SetConfig(cfg *config.Config) {
	fe.indicators = initErrIndicator(fe.mr, cfg)
}
