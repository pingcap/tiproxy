// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
)

const (
	errMetricExpDuration = 1 * time.Minute
	// balanceSeconds4Health indicates the time (in seconds) to migrate all the connections.
	// Do not migrate too fast because TiProxy may mistakenly migrate connections due to metric delay.
	// E.g. If the PD leader is down, TiDB-0 fails to connect to PD, and after 5 seconds, TiDB-1 also fails.
	balanceSeconds4Health = 60.0
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
	queryFailureRule metricsreader.QueryRule
	queryTotalRule   metricsreader.QueryRule
	failurePromQL    string
	totalPromQL      string
	failureKey       string
	totalKey         string
	failThreshold    float64
	recoverThreshold float64
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
			failureKey:       "failure_pd",
			totalKey:         "total_pd",
			failThreshold:    0.3,
			recoverThreshold: 0.1,
			failurePromQL:    `sum(increase(tidb_tikvclient_backoff_seconds_count{type="pdRPC"}[2m])) by (instance)`,
			queryFailureRule: metricsreader.QueryRule{
				Names:     []string{"tidb_tikvclient_backoff_seconds_count"},
				Retention: 2 * time.Minute,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return generalMetric2Value(mfs, "tidb_tikvclient_backoff_seconds_count", "pdRPC")
				},
				Range2Value: generalRange2Value,
				ResultType:  model.ValVector,
			},
			totalPromQL: `sum(increase(pd_client_request_handle_requests_duration_seconds_count[2m])) by (instance)`,
			queryTotalRule: metricsreader.QueryRule{
				Names:     []string{"pd_client_request_handle_requests_duration_seconds_count"},
				Retention: 2 * time.Minute,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return generalMetric2Value(mfs, "pd_client_request_handle_requests_duration_seconds_count", "")
				},
				Range2Value: generalRange2Value,
				ResultType:  model.ValVector,
			},
		},
		{
			// may be caused by disconnection to TiKV
			failureKey:       "failure_tikv",
			totalKey:         "total_tikv",
			failThreshold:    0.3,
			recoverThreshold: 0.1,
			failurePromQL:    `sum(increase(tidb_tikvclient_backoff_seconds_count{type="tikvRPC"}[2m])) by (instance)`,
			queryFailureRule: metricsreader.QueryRule{
				Names:     []string{"tidb_tikvclient_backoff_seconds_count"},
				Retention: 2 * time.Minute,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return generalMetric2Value(mfs, "tidb_tikvclient_backoff_seconds_count", "tikvRPC")
				},
				Range2Value: generalRange2Value,
				ResultType:  model.ValVector,
			},
			totalPromQL: `sum(increase(tidb_tikvclient_request_seconds_count[2m])) by (instance)`,
			queryTotalRule: metricsreader.QueryRule{
				Names:     []string{"tidb_tikvclient_request_seconds_count"},
				Retention: 2 * time.Minute,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return generalMetric2Value(mfs, "tidb_tikvclient_request_seconds_count", "")
				},
				Range2Value: generalRange2Value,
				ResultType:  model.ValVector,
			},
		},
	}
)

func generalMetric2Value(mfs map[string]*dto.MetricFamily, metricName string, typeValue string) model.SampleValue {
	mt := mfs[metricName].Metric
	total := 0
	for _, m := range mt {
		for _, label := range m.Label {
			if *label.Name == "type" {
				if (typeValue == "" || *label.Value == typeValue) && m.Untyped != nil {
					total += int(*m.Untyped.Value)
				}
				break
			}
		}
	}
	return model.SampleValue(total)
}

func generalRange2Value(pairs []model.SamplePair) model.SampleValue {
	if len(pairs) < 2 {
		return model.SampleValue(math.NaN())
	}
	diff := pairs[len(pairs)-1].Value - pairs[0].Value
	// Maybe the backend just rebooted.
	if diff < 0 {
		return model.SampleValue(math.NaN())
	}
	return diff
}

var _ Factor = (*FactorHealth)(nil)

// The snapshot of backend statistics when the metric was updated.
type healthBackendSnapshot struct {
	updatedTime time.Time
	valueRange  valueRange
	// indicator, failureValue, and totalValue are the info of the failed indicator.
	indicator    string
	failureValue int
	totalValue   int
	// Record the balance count when the backend becomes unhealthy so that it won't be smaller in the next rounds.
	balanceCount float64
}

type errIndicator struct {
	queryFailureRule   metricsreader.QueryRule
	queryTotalRule     metricsreader.QueryRule
	queryFailureExpr   metricsreader.QueryExpr
	queryTotalExpr     metricsreader.QueryExpr
	queryFailureResult metricsreader.QueryResult
	queryTotalResult   metricsreader.QueryResult
	failureKey         string
	totalKey           string
	failThreshold      float64
	recoverThreshold   float64
}

type FactorHealth struct {
	snapshot   map[string]healthBackendSnapshot
	indicators []errIndicator
	mr         metricsreader.MetricsReader
	bitNum     int
	lg         *zap.Logger
}

func NewFactorHealth(mr metricsreader.MetricsReader, lg *zap.Logger) *FactorHealth {
	return &FactorHealth{
		mr:         mr,
		snapshot:   make(map[string]healthBackendSnapshot),
		indicators: initErrIndicator(mr),
		bitNum:     2,
		lg:         lg,
	}
}

func initErrIndicator(mr metricsreader.MetricsReader) []errIndicator {
	indicators := make([]errIndicator, 0, len(errDefinitions))
	for _, def := range errDefinitions {
		indicator := errIndicator{
			queryFailureExpr: metricsreader.QueryExpr{
				PromQL: def.failurePromQL,
			},
			queryTotalExpr: metricsreader.QueryExpr{
				PromQL: def.totalPromQL,
			},
			queryFailureRule: def.queryFailureRule,
			queryTotalRule:   def.queryTotalRule,
			failureKey:       def.failureKey,
			totalKey:         def.totalKey,
			failThreshold:    def.failThreshold,
			recoverThreshold: def.recoverThreshold,
		}
		mr.AddQueryExpr(indicator.failureKey, indicator.queryFailureExpr, indicator.queryFailureRule)
		mr.AddQueryExpr(indicator.totalKey, indicator.queryTotalExpr, indicator.queryTotalRule)
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
	needUpdateSnapshot, latestTime := false, time.Time{}
	for i := 0; i < len(fh.indicators); i++ {
		failureQR := fh.mr.GetQueryResult(fh.indicators[i].failureKey)
		if failureQR.Empty() {
			continue
		}
		totalQR := fh.mr.GetQueryResult(fh.indicators[i].totalKey)
		if totalQR.Empty() {
			continue
		}
		if fh.indicators[i].queryFailureResult.UpdateTime != failureQR.UpdateTime {
			fh.indicators[i].queryFailureResult = failureQR
			needUpdateSnapshot = true
		}
		if failureQR.UpdateTime.After(latestTime) {
			latestTime = failureQR.UpdateTime
		}
		if fh.indicators[i].queryTotalResult.UpdateTime != totalQR.UpdateTime {
			fh.indicators[i].queryTotalResult = totalQR
			needUpdateSnapshot = true
		}
		if totalQR.UpdateTime.After(latestTime) {
			latestTime = totalQR.UpdateTime
		}
	}
	if time.Since(latestTime) > errMetricExpDuration {
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

// - Not exist in the backends or metric is missing for a long time: delete it
// - Metric is missing temporarily or not exist in the backends temporarily: preserve the snapshot
// - Exist in the backends but the metric is not updated: preserve the snapshot
// - Exist in the backends and metric is updated: update the snapshot
func (fh *FactorHealth) updateSnapshot(backends []scoredBackend) {
	now := time.Now()
	for _, backend := range backends {
		addr := backend.Addr()
		// Get the current value range.
		updatedTime, valueRange, indicator, failureValue, totalValue := time.Time{}, valueRangeNormal, "", 0, 0
		for _, ind := range fh.indicators {
			ts := ind.queryFailureResult.UpdateTime
			if ind.queryTotalResult.UpdateTime.After(ts) {
				ts = ind.queryTotalResult.UpdateTime
			}
			if ts.Add(errMetricExpDuration).Before(now) {
				// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
				continue
			}
			if ts.After(updatedTime) {
				updatedTime = ts
			}
			failureSample, totalSample := ind.queryFailureResult.GetSample4Backend(backend), ind.queryTotalResult.GetSample4Backend(backend)
			if failureSample == nil || totalSample == nil {
				continue
			}
			metrics.BackendMetricGauge.WithLabelValues(addr, ind.failureKey).Set(float64(failureSample.Value))
			metrics.BackendMetricGauge.WithLabelValues(addr, ind.totalKey).Set(float64(totalSample.Value))
			vr := calcValueRange(failureSample, totalSample, ind)
			if vr > valueRange {
				valueRange = vr
				failureValue = int(failureSample.Value)
				totalValue = int(totalSample.Value)
				indicator = ind.failureKey
			}
		}
		// If the metric is unavailable, try to reuse the latest one.
		snapshot := fh.snapshot[addr]
		if updatedTime.IsZero() {
			continue
		}
		// Set balance count if the backend is unhealthy, otherwise reset it to 0.
		var balanceCount float64
		if valueRange >= valueRangeAbnormal {
			if snapshot.balanceCount > 0.0001 {
				balanceCount = snapshot.balanceCount
			} else {
				balanceCount = float64(backend.ConnScore()) / balanceSeconds4Health
			}
		}
		// Log it whenever the health changes, even from unhealthy to healthy and no connections.
		if balanceCount != snapshot.balanceCount {
			fh.lg.Info("update health risk",
				zap.String("addr", addr),
				zap.Int("risk", int(valueRange)),
				zap.String("indicator", indicator),
				zap.Int("failure_value", failureValue),
				zap.Int("total_value", totalValue),
				zap.Int("last_risk", int(snapshot.valueRange)),
				zap.Float64("balance_count", balanceCount),
				zap.Int("conn_score", backend.ConnScore()))
		}
		fh.snapshot[addr] = healthBackendSnapshot{
			updatedTime:  updatedTime,
			valueRange:   valueRange,
			indicator:    indicator,
			failureValue: failureValue,
			totalValue:   totalValue,
			balanceCount: balanceCount,
		}
	}

	// Besides missing metrics, backends may also be not in the backend list.
	for addr, backend := range fh.snapshot {
		if backend.updatedTime.Add(errMetricExpDuration).Before(now) {
			delete(fh.snapshot, addr)
		}
	}
}

func calcValueRange(failureSample, totalSample *model.Sample, indicator errIndicator) valueRange {
	// A backend is typically normal, so if its metric misses, take it as normal.
	if failureSample == nil || totalSample == nil {
		return valueRangeNormal
	}
	if math.IsNaN(float64(failureSample.Value)) || math.IsNaN(float64(totalSample.Value)) {
		return valueRangeNormal
	}
	if failureSample.Value == 0 || totalSample.Value == 0 {
		return valueRangeNormal
	}
	value := float64(failureSample.Value) / float64(totalSample.Value)
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
	// If the backend has no metrics (not in snapshot), take it as healthy.
	return int(fh.snapshot[addr].valueRange)
}

func (fh *FactorHealth) ScoreBitNum() int {
	return fh.bitNum
}

func (fh *FactorHealth) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	// Only migrate connections when one is valueRangeNormal and the other is valueRangeAbnormal.
	fromScore := fh.caclErrScore(from.Addr())
	toScore := fh.caclErrScore(to.Addr())
	snapshot := fh.snapshot[from.Addr()]
	var fields []zap.Field
	if snapshot.indicator != "" {
		fields = append(fields,
			zap.String("indicator", snapshot.indicator),
			zap.Int("failure_value", snapshot.failureValue),
			zap.Int("total_value", snapshot.totalValue),
		)
	}
	if fromScore-toScore <= 1 {
		return AdviceNeutral, 0, fields
	}
	return AdvicePositive, snapshot.balanceCount, fields
}

func (fh *FactorHealth) SetConfig(cfg *config.Config) {
}

func (fh *FactorHealth) CanBeRouted(_ uint64) bool {
	return true
}

func (fh *FactorHealth) Close() {
	for _, indicator := range fh.indicators {
		fh.mr.RemoveQueryExpr(indicator.failureKey)
		fh.mr.RemoveQueryExpr(indicator.totalKey)
	}
}
