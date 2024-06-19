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
	// balanceSeconds4Health indicates the time (in seconds) to migrate all the connections.
	balanceSeconds4Health = 10
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

type errDefinition struct {
	promQL           string
	failThreshold    int
	recoverThreshold int
}

var (
	// errDefinitions predefines the default error indicators.
	//
	// The chosen metrics must meet some requirements:
	//  1. To treat a backend as normal, all the metrics should be normal.
	//     E.g. tidb_session_schema_lease_error_total is always 0 even if the backend doesn't recover when it has no connection,
	//     so we need other metrics to judge whether the backend recovers.
	//  2. Unstable (not only unavailable) network should also be treated as abnormal.
	//     E.g. Renewing lease may succeed sometimes and `time() - tidb_domain_lease_expire_time` may look normal
	//     even when the network is unstable, so we need other metrics to judge unstable network.
	//  3. `failThreshold - recoverThreshold` should be big enough so that TiProxy won't mistakenly migrate connections.
	//     E.g. If TiKV is unavailable, all backends may report the same errors. We can ensure the error is caused by this TiDB
	//     only when other TiDB report much less errors.
	//  4. The metric value of a normal backend with high CPS should be less than `failThreshold` and the value of an abnormal backend
	//     with 0 CPS should be greater than `recoverThreshold`.
	//     E.g. tidb_tikvclient_backoff_seconds_count may be high when CPS is high on a normal backend, and may be very low
	//     when CPS is 0 on an abnormal backend.
	//  5. Normal metrics must keep for some time before treating the backend as normal to avoid frequent migration.
	//     E.g. Unstable network may lead to repeated fluctuations of error counts.
	errDefinitions = []errDefinition{
		{
			// may be caused by disconnection to PD
			// test with no connection: around 80
			promQL:           `sum(increase(tidb_tikvclient_backoff_seconds_count{type="pdRPC"}[1m])) by (instance)`,
			failThreshold:    100,
			recoverThreshold: 10,
		},
		{
			// may be caused by disconnection to TiKV
			// test with no connection: regionMiss is around 1300, tikvRPC is around 40
			promQL:           `sum(increase(tidb_tikvclient_backoff_seconds_count{type=~"regionMiss|tikvRPC"}[1m])) by (instance)`,
			failThreshold:    1000,
			recoverThreshold: 100,
		},
	}
)

var _ Factor = (*FactorHealth)(nil)

// The snapshot of backend statistics when the metric was updated.
type healthBackendSnapshot struct {
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

type FactorHealth struct {
	snapshot   map[string]healthBackendSnapshot
	indicators []errIndicator
	mr         metricsreader.MetricsReader
	bitNum     int
}

func NewFactorHealth(mr metricsreader.MetricsReader) *FactorHealth {
	return &FactorHealth{
		mr:         mr,
		snapshot:   make(map[string]healthBackendSnapshot),
		indicators: initErrIndicator(mr),
		bitNum:     2,
	}
}

func initErrIndicator(mr metricsreader.MetricsReader) []errIndicator {
	indicators := make([]errIndicator, 0, len(errDefinitions))
	for _, def := range errDefinitions {
		indicator := errIndicator{
			queryExpr: metricsreader.QueryExpr{
				PromQL: def.promQL,
			},
			failThreshold:    def.failThreshold,
			recoverThreshold: def.recoverThreshold,
		}
		indicator.queryID = mr.AddQueryExpr(indicator.queryExpr)
		indicators = append(indicators, indicator)
	}
	return indicators
}

func (fh *FactorHealth) Name() string {
	return "health"
}

func (fh *FactorHealth) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	needUpdateSnapshot, latestTime := false, monotime.Time(0)
	for i := 0; i < len(fh.indicators); i++ {
		qr := fh.mr.GetQueryResult(fh.indicators[i].queryID)
		if qr.Err != nil || qr.Empty() {
			continue
		}
		if fh.indicators[i].queryResult.UpdateTime != qr.UpdateTime {
			fh.indicators[i].queryResult = qr
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
		fh.updateSnapshot(backends)
	}
	for i := 0; i < len(backends); i++ {
		score := fh.caclErrScore(backends[i].Addr())
		backends[i].addScore(score, fh.bitNum)
	}
}

func (fh *FactorHealth) updateSnapshot(backends []scoredBackend) {
	snapshots := make(map[string]healthBackendSnapshot, len(fh.snapshot))
	for _, backend := range backends {
		// Get the current value range.
		updatedTime, valueRange := monotime.Time(0), valueRangeNormal
		for i := 0; i < len(fh.indicators); i++ {
			ts := fh.indicators[i].queryResult.UpdateTime
			if monotime.Since(ts) > errMetricExpDuration {
				// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
				continue
			}
			if ts > updatedTime {
				updatedTime = ts
			}
			sample := fh.indicators[i].queryResult.GetSample4Backend(backend)
			vr := calcValueRange(sample, fh.indicators[i])
			if vr > valueRange {
				valueRange = vr
			}
		}
		// If the metric is unavailable, try to reuse the latest one.
		addr := backend.Addr()
		snapshot, existSnapshot := fh.snapshot[addr]
		if updatedTime == monotime.Time(0) {
			if existSnapshot && monotime.Since(snapshot.updatedTime) < errMetricExpDuration {
				snapshots[addr] = snapshot
			}
			continue
		}
		snapshots[addr] = healthBackendSnapshot{
			updatedTime: updatedTime,
			valueRange:  valueRange,
		}
	}
	fh.snapshot = snapshots
}

func calcValueRange(sample *model.Sample, indicator errIndicator) valueRange {
	// A backend is typically normal, so if its metric misses, take it as normal.
	if sample == nil {
		return valueRangeNormal
	}
	if math.IsNaN(float64(sample.Value)) {
		return valueRangeNormal
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

func (fh *FactorHealth) caclErrScore(addr string) int {
	snapshot, ok := fh.snapshot[addr]
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

func (fh *FactorHealth) ScoreBitNum() int {
	return fh.bitNum
}

func (fh *FactorHealth) BalanceCount(from, to scoredBackend) int {
	// Only migrate connections when one is valueRangeNormal and the other is valueRangeAbnormal.
	fromScore := fh.caclErrScore(from.Addr())
	toScore := fh.caclErrScore(to.Addr())
	if fromScore-toScore > 1 {
		// Assuming that the source and target backends have similar connections at first.
		// We wish the connections to be migrated in 10 seconds but only a few are migrated in each round.
		// If we use from.ConnScore() / 10, the migration will be slower and slower.
		conns := (from.ConnScore() + to.ConnScore()) / (balanceSeconds4Health * 2)
		if conns > 0 {
			return conns
		}
		return 1
	}
	return 0
}

func (fh *FactorHealth) SetConfig(cfg *config.Config) {
}

func (fh *FactorHealth) Close() {
	for _, indicator := range fh.indicators {
		fh.mr.RemoveQueryExpr(indicator.queryID)
	}
}
