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
	cpuEwmaAlpha = 0.5
	// If some metrics are missing, we use the old one temporarily for no longer than cpuMetricExpDuration.
	cpuMetricExpDuration = 2 * time.Minute
	cpuScoreStep         = 5
	// 0.001 represents for 0.1%
	minCpuPerConn      = 0.001
	cpuBalancedRatio   = 1.2
	cpuUnbalancedRatio = 1.1
	// If the CPU difference of 2 backends is 30% and we're narrowing it to 20% in 30 seconds,
	// then in each round, we migrate ((30% - 20%) / 2) / usagePerConn / 30 = 1 / usagePerConn / 600 connections.
	balanceRatio4Cpu = 600
)

var _ Factor = (*FactorCPU)(nil)

var (
	cpuQueryExpr = metricsreader.QueryExpr{
		PromQL:   `irate(process_cpu_seconds_total{%s="tidb"}[30s])/tidb_server_maxprocs`,
		HasLabel: true,
		Range:    1 * time.Minute,
	}
	cpuQueryRule = metricsreader.QueryRule{
		Names:     []string{"process_cpu_seconds_total", "tidb_server_maxprocs"},
		Retention: 1 * time.Minute,
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			if mfs["process_cpu_seconds_total"] == nil || len(mfs["process_cpu_seconds_total"].Metric) == 0 {
				return model.SampleValue(math.NaN())
			}
			if mfs["tidb_server_maxprocs"] == nil || len(mfs["tidb_server_maxprocs"].Metric) == 0 {
				return model.SampleValue(math.NaN())
			}
			cpuTotal := mfs["process_cpu_seconds_total"].Metric[0].Untyped
			maxProcs := mfs["tidb_server_maxprocs"].Metric[0].Untyped
			if cpuTotal == nil || maxProcs == nil {
				return model.SampleValue(math.NaN())
			}
			return model.SampleValue(*cpuTotal.Value / *maxProcs.Value)
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			if len(pairs) < 2 {
				return model.SampleValue(math.NaN())
			}
			lastPair := pairs[len(pairs)-1]
			for i := len(pairs) - 2; i >= 0; i-- {
				pair := pairs[i]
				// Skip this one if the interval is too short.
				seconds := float64(lastPair.Timestamp-pair.Timestamp) / 1000.0
				if seconds < 1 {
					continue
				}
				// Maybe the backend just rebooted.
				if pair.Value > lastPair.Value {
					return model.SampleValue(math.NaN())
				}
				return (lastPair.Value - pair.Value) / model.SampleValue(seconds)
			}
			return model.SampleValue(math.NaN())
		},
		ResultType: model.ValMatrix,
	}
)

type cpuBackendSnapshot struct {
	// the latest time in the query result
	updatedTime time.Time
	// smoothed CPU usage, used to decide whether to migrate
	avgUsage float64
	// timely CPU usage, used to score and decide the balance count
	latestUsage float64
	connCount   int
}

type FactorCPU struct {
	// The snapshot of backend statistics when the matrix was updated.
	snapshot map[string]cpuBackendSnapshot
	// The updated time of the metric that we've read last time.
	lastMetricTime time.Time
	// The estimated average CPU usage used by one connection.
	usagePerConn        float64
	mr                  metricsreader.MetricsReader
	bitNum              int
	migrationsPerSecond float64
	lg                  *zap.Logger
}

func NewFactorCPU(mr metricsreader.MetricsReader, lg *zap.Logger) *FactorCPU {
	fc := &FactorCPU{
		mr:       mr,
		bitNum:   5,
		snapshot: make(map[string]cpuBackendSnapshot),
		lg:       lg,
	}
	mr.AddQueryExpr(fc.Name(), cpuQueryExpr, cpuQueryRule)
	return fc
}

func (fc *FactorCPU) Name() string {
	return "cpu"
}

func (fc *FactorCPU) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	qr := fc.mr.GetQueryResult(fc.Name())
	if qr.Empty() {
		return
	}

	if qr.UpdateTime != fc.lastMetricTime {
		// Metrics have updated.
		fc.lastMetricTime = qr.UpdateTime
		fc.updateSnapshot(qr, backends)
		fc.updateCpuPerConn()
	}
	if time.Since(fc.lastMetricTime) > cpuMetricExpDuration {
		// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
		return
	}

	for i := 0; i < len(backends); i++ {
		_, latestUsage := fc.getUsage(backends[i])
		backends[i].addScore(int(latestUsage*100)/cpuScoreStep, fc.bitNum)
	}
}

func (fc *FactorCPU) updateSnapshot(qr metricsreader.QueryResult, backends []scoredBackend) {
	now := time.Now()
	for _, backend := range backends {
		addr := backend.Addr()
		// If a backend exists in metrics but not in the backend list, ignore it for this round.
		// The backend will be in the next round if it's healthy.
		pairs := qr.GetSamplePair4Backend(backend)
		if len(pairs) <= 0 {
			continue
		}
		updateTime := time.UnixMilli(int64(pairs[len(pairs)-1].Timestamp))
		// The time point of updating each backend is different, so only partial of the backends are updated every time.
		// If this backend is not updated, ignore it.
		snapshot := fc.snapshot[addr]
		if !snapshot.updatedTime.Before(updateTime) {
			continue
		}
		avgUsage, latestUsage := calcAvgUsage(pairs)
		if avgUsage < 0 {
			continue
		}
		metrics.BackendMetricGauge.WithLabelValues(addr, "cpu").Set(avgUsage)
		fc.snapshot[addr] = cpuBackendSnapshot{
			avgUsage:    avgUsage,
			latestUsage: latestUsage,
			connCount:   backend.ConnCount(),
			updatedTime: updateTime,
		}
	}

	// Besides missing metrics, backends may also be not in the backend list.
	for addr, backend := range fc.snapshot {
		if backend.updatedTime.Add(cpuMetricExpDuration).Before(now) {
			delete(fc.snapshot, addr)
		}
	}
}

func calcAvgUsage(usageHistory []model.SamplePair) (avgUsage, latestUsage float64) {
	avgUsage, latestUsage = -1, -1
	if len(usageHistory) == 0 {
		return
	}
	// The CPU usage may jitter, so use the EWMA algorithm to make it smooth.
	for _, usage := range usageHistory {
		value := float64(usage.Value)
		if math.IsNaN(value) {
			continue
		}
		latestUsage = value
		if avgUsage < 0 {
			avgUsage = value
		} else {
			avgUsage = avgUsage*(1-cpuEwmaAlpha) + value*cpuEwmaAlpha
		}
	}
	if avgUsage > 1 {
		avgUsage = 1
	}
	return
}

// Estimate the average CPU usage used by one connection.
// Don't fetch the global connection count (tidb_server_connections) because the connScore is also based on the local count.
// Don't estimate it based on each backend because background jobs may use much CPU.
// E.g. auto-analyze uses 30% CPU and the backend has 1 connection. You may mistakenly think the connection uses 30% CPU.
func (fc *FactorCPU) updateCpuPerConn() {
	totalUsage, totalConns := 0.0, 0
	for _, backend := range fc.snapshot {
		if backend.latestUsage > 0 && backend.connCount > 0 {
			totalUsage += backend.latestUsage
			totalConns += backend.connCount
		}
	}
	if totalConns > 0 {
		usagePerConn := totalUsage / float64(totalConns)
		// When the cluster is idle and the clients are connecting to it all at once (e.g. when sysbench starts),
		// the CPU usage lags behind, so the usagePerConn may be very low. In this case, all the connections may be
		// routed to the same backend just because the CPU usage of the backend is a little lower.
		if usagePerConn < minCpuPerConn && totalUsage/float64(len(fc.snapshot)) <= 0.1 {
			usagePerConn = fc.usagePerConn
		}
		if fc.usagePerConn != 0 && math.Abs(usagePerConn/fc.usagePerConn-1) > 0.05 {
			fc.lg.Debug("update CPU usage per connection",
				zap.Float64("usage_per_conn", usagePerConn),
				zap.Int("total_conns", totalConns),
				zap.Float64("total_usage", totalUsage))
		}
		fc.usagePerConn = usagePerConn
	}
	if fc.usagePerConn <= 0 {
		fc.usagePerConn = minCpuPerConn
	}
}

// Estimate the current cpu usage by the latest CPU usage, the latest connection count, and the current connection count.
func (fc *FactorCPU) getUsage(backend scoredBackend) (avgUsage, latestUsage float64) {
	snapshot, ok := fc.snapshot[backend.Addr()]
	if !ok || snapshot.avgUsage < 0 || latestUsage < 0 {
		// The metric has missed for minutes.
		return 1, 1
	}
	avgUsage = snapshot.avgUsage
	latestUsage = snapshot.latestUsage + float64(backend.ConnScore()-snapshot.connCount)*fc.usagePerConn
	latestUsage = math.Max(math.Min(latestUsage, 1), 0)
	return
}

func (fc *FactorCPU) ScoreBitNum() int {
	return fc.bitNum
}

func (fc *FactorCPU) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	fromAvgUsage, fromLatestUsage := fc.getUsage(from)
	toAvgUsage, toLatestUsage := fc.getUsage(to)
	fields := []zap.Field{
		zap.Float64("from_avg_usage", fromAvgUsage),
		zap.Float64("from_latest_usage", fromLatestUsage),
		zap.Int("from_snapshot_conn", fc.snapshot[from.Addr()].connCount),
		zap.Int("from_conn", from.ConnScore()),
		zap.Float64("to_avg_usage", toAvgUsage),
		zap.Float64("to_latest_usage", toLatestUsage),
		zap.Int("to_snapshot_conn", fc.snapshot[to.Addr()].connCount),
		zap.Int("to_conn", to.ConnScore()),
		zap.Float64("usage_per_conn", fc.usagePerConn),
	}
	// Reject migration if it will make the target backend even much busier than the source.
	if (1.3-(toAvgUsage+fc.usagePerConn))*cpuUnbalancedRatio < 1.3-(fromAvgUsage-fc.usagePerConn) ||
		(1.3-(toLatestUsage+fc.usagePerConn))*cpuUnbalancedRatio < 1.3-(fromLatestUsage-fc.usagePerConn) {
		return AdviceNegtive, 0, fields
	}
	// The higher the CPU usage, the more sensitive the load balance should be.
	// E.g. 10% vs 25% don't need rebalance, but 80% vs 95% need rebalance.
	// Use the average usage to avoid thrash when CPU jitters too much and use the latest usage to avoid migrate too many connections.
	if 1.3-toAvgUsage < (1.3-fromAvgUsage)*cpuBalancedRatio || 1.3-toLatestUsage < (1.3-fromLatestUsage)*cpuBalancedRatio {
		return AdviceNeutral, 0, fields
	}
	if fc.migrationsPerSecond > 0 {
		return AdvicePositive, fc.migrationsPerSecond, fields
	}
	return AdvicePositive, 1 / fc.usagePerConn / balanceRatio4Cpu, fields
}

func (fc *FactorCPU) SetConfig(cfg *config.Config) {
	fc.migrationsPerSecond = cfg.Balance.CPU.MigrationsPerSecond
}

func (fc *FactorCPU) CanBeRouted(_ uint64) bool {
	return true
}

func (fc *FactorCPU) Close() {
	fc.mr.RemoveQueryExpr(fc.Name())
}
