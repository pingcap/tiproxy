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
	// The backend sometimes OOM before reaching 100% memory usage. oomMemoryUsage indicates the memory usage when OOM.
	oomMemoryUsage = 0.9
	// If some metrics are missing, we use the old one temporarily for no longer than memMetricExpDuration.
	memMetricExpDuration = 1 * time.Minute
	// balanceSeconds4HighMemory indicates the time (in seconds) to migrate all the connections when memory is high.
	balanceSeconds4HighMemory = 60.0
	// balanceSeconds4OOMRisk indicates the time (in seconds) to migrate all the connections when there's an OOM risk.
	balanceSeconds4OOMRisk = 10.0
)

var _ Factor = (*FactorCPU)(nil)

var (
	memQueryExpr = metricsreader.QueryExpr{
		PromQL:   `process_resident_memory_bytes{%s="tidb"}/tidb_server_memory_quota_bytes`,
		HasLabel: true,
		Range:    1 * time.Minute,
	}
	memoryQueryRule = metricsreader.QueryRule{
		Names:     []string{"process_resident_memory_bytes", "tidb_server_memory_quota_bytes"},
		Retention: 1 * time.Minute,
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			memoryUsage := mfs["process_resident_memory_bytes"].Metric[0].Untyped
			memoryQuota := mfs["tidb_server_memory_quota_bytes"].Metric[0].Untyped
			if memoryUsage == nil || memoryQuota == nil {
				return model.SampleValue(math.NaN())
			}
			return model.SampleValue(*memoryUsage.Value / *memoryQuota.Value)
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			if len(pairs) < 1 {
				return model.SampleValue(math.NaN())
			}
			return pairs[len(pairs)-1].Value
		},
		ResultType: model.ValMatrix,
	}
)

type oomRiskLevel struct {
	// The current memory usage.
	memUsage float64
	// Left estimated time to OOM according to the current memory increasing speed.
	timeToOOM time.Duration
}

// For memory-based balance, we don't need to balance memory all the time.
// We only need to rescue as many connections as possible when the backend is going OOM.
var (
	oomRiskLevels = []oomRiskLevel{
		{memUsage: 0.75, timeToOOM: 45 * time.Second},
		{memUsage: 0.6, timeToOOM: 3 * time.Minute},
	}
)

// 2: high risk
// 1: low risk
// 0: no risk
func getRiskLevel(usage float64, timeToOOM time.Duration) int {
	level := 0
	for j := 0; j < len(oomRiskLevels); j++ {
		if timeToOOM < oomRiskLevels[j].timeToOOM {
			return len(oomRiskLevels) - j
		}
		if usage > oomRiskLevels[j].memUsage {
			return len(oomRiskLevels) - j
		}
	}
	return level
}

type memBackendSnapshot struct {
	updatedTime time.Time
	memUsage    float64
	timeToOOM   time.Duration
	riskLevel   int
	// Record the balance count when the backend has OOM risk so that it won't be smaller in the next rounds.
	balanceCount float64
}

type FactorMemory struct {
	// The snapshot of backend statistics when the matrix was updated.
	snapshot map[string]memBackendSnapshot
	// The updated time of the metric that we've read last time.
	lastMetricTime time.Time
	mr             metricsreader.MetricsReader
	bitNum         int
	lg             *zap.Logger
}

func NewFactorMemory(mr metricsreader.MetricsReader, lg *zap.Logger) *FactorMemory {
	bitNum := 0
	for levels := len(oomRiskLevels); ; bitNum++ {
		if levels == 0 {
			break
		}
		levels = levels >> 1
	}
	fm := &FactorMemory{
		mr:       mr,
		bitNum:   bitNum,
		snapshot: make(map[string]memBackendSnapshot),
		lg:       lg,
	}
	mr.AddQueryExpr(fm.Name(), memQueryExpr, memoryQueryRule)
	return fm
}

func (fm *FactorMemory) Name() string {
	return "memory"
}

func (fm *FactorMemory) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	qr := fm.mr.GetQueryResult(fm.Name())
	if qr.Empty() {
		return
	}

	if qr.UpdateTime != fm.lastMetricTime {
		// Metrics have updated.
		fm.lastMetricTime = qr.UpdateTime
		fm.updateSnapshot(qr, backends)
	}
	if time.Since(fm.lastMetricTime) > memMetricExpDuration {
		// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
		return
	}

	for i := 0; i < len(backends); i++ {
		addr := backends[i].Addr()
		// If the backend is new or the backend misses metrics, take it safe.
		score := fm.snapshot[addr].riskLevel
		backends[i].addScore(score, fm.bitNum)
	}
}

// - Not exist in the backends or metric is missing for a long time: delete it
// - Metric is missing temporarily or not exist in the backends temporarily: preserve the snapshot
// - Exist in the backends but the metric is not updated: perserve the snapshot
// - Exist in the backends and metric is updated: update the snapshot
func (fm *FactorMemory) updateSnapshot(qr metricsreader.QueryResult, backends []scoredBackend) {
	now := time.Now()
	for _, backend := range backends {
		addr := backend.Addr()
		snapshot := fm.snapshot[addr]
		// If a backend exists in metrics but not in the backend list, ignore it for this round.
		// The backend will be in the next round if it's healthy.
		pairs := qr.GetSamplePair4Backend(backend)
		if len(pairs) == 0 {
			continue
		}
		updateTime := time.UnixMilli(int64(pairs[len(pairs)-1].Timestamp))
		// If this backend is not updated, ignore it.
		if !snapshot.updatedTime.Before(updateTime) {
			continue
		}
		latestUsage, timeToOOM := calcMemUsage(pairs)
		if latestUsage < 0 {
			continue
		}
		metrics.BackendMetricGauge.WithLabelValues(addr, "memory").Set(latestUsage)
		riskLevel := getRiskLevel(latestUsage, timeToOOM)
		balanceCount := fm.calcBalanceCount(backend, riskLevel, timeToOOM)
		// Log it whenever the risk changes, even from high risk to no risk and no connections.
		if riskLevel != snapshot.riskLevel {
			fm.lg.Info("update memory risk",
				zap.String("addr", addr),
				zap.Int("risk", riskLevel),
				zap.Float64("mem_usage", latestUsage),
				zap.Duration("time_to_oom", timeToOOM),
				zap.Int("last_risk", snapshot.riskLevel),
				zap.Float64("last_mem_usage", snapshot.memUsage),
				zap.Duration("last_time_to_oom", snapshot.timeToOOM),
				zap.Float64("balance_count", balanceCount),
				zap.Int("conn_score", backend.ConnScore()))
		}
		fm.snapshot[addr] = memBackendSnapshot{
			updatedTime:  updateTime,
			memUsage:     latestUsage,
			timeToOOM:    timeToOOM,
			balanceCount: balanceCount,
			riskLevel:    riskLevel,
		}
	}

	// Besides missing metrics, backends may also be not in the backend list.
	for addr, backend := range fm.snapshot {
		if backend.updatedTime.Add(memMetricExpDuration).Before(now) {
			delete(fm.snapshot, addr)
		}
	}
}

func calcMemUsage(usageHistory []model.SamplePair) (latestUsage float64, timeToOOM time.Duration) {
	latestUsage = -1
	timeToOOM = time.Duration(math.MaxInt64)
	if len(usageHistory) == 0 {
		return
	}
	var latestTime model.Time
	for i := len(usageHistory) - 1; i >= 0; i-- {
		value := float64(usageHistory[i].Value)
		if math.IsNaN(value) {
			continue
		}
		if value > oomMemoryUsage {
			value = oomMemoryUsage
		}
		if latestUsage < 0 {
			latestUsage = value
			latestTime = usageHistory[i].Timestamp
		} else {
			timeDiff := latestTime.Sub(usageHistory[i].Timestamp)
			if timeDiff < 10*time.Second {
				// Skip this one is the interval is too short.
				continue
			}
			usageDiff := latestUsage - value
			if usageDiff > 1e-4 {
				timeToOOM = timeDiff * time.Duration((oomMemoryUsage-latestUsage)/usageDiff)
			}
			break
		}
	}
	return
}

func (fm *FactorMemory) calcBalanceCount(backend scoredBackend, riskLevel int, timeToOOM time.Duration) float64 {
	if riskLevel < 2 {
		return 0
	}
	// Assuming all backends have high memory and the user scales out a new backend, we don't want all the connections
	// are migrated all at once.
	seconds := balanceSeconds4HighMemory
	if timeToOOM < oomRiskLevels[0].timeToOOM {
		seconds = balanceSeconds4OOMRisk
	}
	balanceCount := float64(backend.ConnScore()) / seconds
	// If the migration started eariler, reuse the balance count.
	if snapshot := fm.snapshot[backend.Addr()]; snapshot.balanceCount > balanceCount {
		return snapshot.balanceCount
	}
	return balanceCount
}

func (fm *FactorMemory) ScoreBitNum() int {
	return fm.bitNum
}

func (fm *FactorMemory) BalanceCount(from, to scoredBackend) (float64, []zap.Field) {
	// The risk level may change frequently, e.g. last time timeToOOM was 30s and connections were migrated away,
	// then this time it becomes 60s and the connections are migrated back.
	// So we only rebalance when the difference of risk levels of 2 backends is big enough.
	fromSnapshot := fm.snapshot[from.Addr()]
	toSnapshot := fm.snapshot[to.Addr()]
	if fromSnapshot.riskLevel-toSnapshot.riskLevel <= 1 {
		return 0, nil
	}
	fields := []zap.Field{
		zap.Duration("from_time_to_oom", fromSnapshot.timeToOOM),
		zap.Float64("from_mem_usage", fromSnapshot.memUsage),
		zap.Duration("to_time_to_oom", toSnapshot.timeToOOM),
		zap.Float64("to_mem_usage", toSnapshot.memUsage)}
	return fromSnapshot.balanceCount, fields
}

func (fm *FactorMemory) SetConfig(cfg *config.Config) {
}

func (fm *FactorMemory) Close() {
	fm.mr.RemoveQueryExpr(fm.Name())
}
