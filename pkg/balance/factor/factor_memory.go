// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
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
	balanceSeconds4OOMRisk = 5.0
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
		{memUsage: 0.75, timeToOOM: time.Minute},
		{memUsage: 0.6, timeToOOM: 5 * time.Minute},
	}
)

// 2: high risk
// 1: low risk
// 0: no risk
func getRiskLevel(usage float64, timeToOOM time.Duration) (int, bool) {
	level := 0
	for j := 0; j < len(oomRiskLevels); j++ {
		if timeToOOM < oomRiskLevels[j].timeToOOM {
			return len(oomRiskLevels) - j, true
		}
		if usage > oomRiskLevels[j].memUsage {
			return len(oomRiskLevels) - j, false
		}
	}
	return level, false
}

type memBackendSnapshot struct {
	updatedTime time.Time
	memUsage    float64
	timeToOOM   time.Duration
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
		score, _ := fm.calcMemScore(addr)
		backends[i].addScore(score, fm.bitNum)
	}
}

func (fm *FactorMemory) updateSnapshot(qr metricsreader.QueryResult, backends []scoredBackend) {
	snapshots := make(map[string]memBackendSnapshot, len(fm.snapshot))
	for _, backend := range backends {
		addr := backend.Addr()
		valid := false
		// If a backend exists in metrics but not in the backend list, ignore it for this round.
		// The backend will be in the next round if it's healthy.
		pairs := qr.GetSamplePair4Backend(backend)
		snapshot, existsSnapshot := fm.snapshot[addr]
		if len(pairs) > 0 {
			updateTime := time.UnixMilli(int64(pairs[len(pairs)-1].Timestamp))
			// If this backend is not updated, ignore it.
			if !existsSnapshot || snapshot.updatedTime.Before(updateTime) {
				latestUsage, timeToOOM := calcMemUsage(pairs)
				if latestUsage >= 0 {
					snapshots[addr] = memBackendSnapshot{
						updatedTime:  updateTime,
						memUsage:     latestUsage,
						timeToOOM:    timeToOOM,
						balanceCount: fm.calcBalanceCount(backend, latestUsage, timeToOOM),
					}
					valid = true
				}
			}
		}
		// Merge the old snapshot just in case some metrics have missed for a short period.
		if !valid && existsSnapshot {
			if time.Since(snapshot.updatedTime) < memMetricExpDuration {
				snapshots[addr] = snapshot
			}
		}
	}
	fm.snapshot = snapshots
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

func (fm *FactorMemory) calcMemScore(addr string) (int, bool) {
	usage := 1.0
	timeToOOM := time.Duration(0)
	snapshot, ok := fm.snapshot[addr]
	if ok {
		usage = snapshot.memUsage
		timeToOOM = snapshot.timeToOOM
	}
	// If one backend misses metric, maybe its version < v8.0.0. When the backends rolling upgrade to v8.1.0,
	// we don't want the connections are migrated all at once because of memory imbalance. Typical score is 0,
	// so give it 0.
	if !ok || usage < 0 {
		return 0, false
	}
	return getRiskLevel(usage, timeToOOM)
}

func (fm *FactorMemory) calcBalanceCount(backend scoredBackend, usage float64, timeToOOM time.Duration) float64 {
	score, isOOM := getRiskLevel(usage, timeToOOM)
	if score < 2 {
		return 0
	}
	// Assuming all backends have high memory and the user scales out a new backend, we don't want all the connections
	// are migrated all at once.
	seconds := balanceSeconds4HighMemory
	if isOOM {
		seconds = balanceSeconds4OOMRisk
	}
	balanceCount := float64(backend.ConnScore()) / seconds
	// If the migration started eariler, reuse the balance count.
	snapshot := fm.snapshot[backend.Addr()]
	if snapshot.balanceCount > balanceCount {
		balanceCount = snapshot.balanceCount
	}
	fm.lg.Debug("update balance count for memory", zap.String("addr", backend.Addr()), zap.Float64("balanceCount", balanceCount),
		zap.Int("connScore", backend.ConnScore()), zap.Duration("timeToOOM", timeToOOM), zap.Float64("memUsage", usage), zap.Float64("snapshot", snapshot.balanceCount))
	return balanceCount
}

func (fm *FactorMemory) ScoreBitNum() int {
	return fm.bitNum
}

func (fm *FactorMemory) BalanceCount(from, to scoredBackend) float64 {
	// The risk level may change frequently, e.g. last time timeToOOM was 30s and connections were migrated away,
	// then this time it becomes 60s and the connections are migrated back.
	// So we only rebalance when the difference of risk levels of 2 backends is big enough.
	fromScore, _ := fm.calcMemScore(from.Addr())
	toScore, _ := fm.calcMemScore(to.Addr())
	if fromScore-toScore <= 1 {
		return 0
	}
	return fm.snapshot[from.Addr()].balanceCount
}

func (fm *FactorMemory) SetConfig(cfg *config.Config) {
}

func (fm *FactorMemory) Close() {
	fm.mr.RemoveQueryExpr(fm.Name())
}
