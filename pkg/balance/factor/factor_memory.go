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
	// If some metrics are missing, we use the old one temporarily for no longer than memMetricExpDuration.
	memMetricExpDuration = 1 * time.Minute
	// balanceCount4Mem indicates how many connections to balance in each round.
	balanceCount4Mem = 10
)

var _ Factor = (*FactorCPU)(nil)

var (
	memQueryExpr = metricsreader.QueryExpr{
		PromQL:   `process_resident_memory_bytes{%s="tidb"}/tidb_server_memory_quota_bytes`,
		HasLabel: true,
		Range:    1 * time.Minute,
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
		{memUsage: 0.8, timeToOOM: 45 * time.Second},
		{memUsage: 0.6, timeToOOM: 3 * time.Minute},
	}
)

type memBackendSnapshot struct {
	updatedTime monotime.Time
	memUsage    float64
	timeToOOM   time.Duration
}

type FactorMemory struct {
	// The snapshot of backend statistics when the matrix was updated.
	snapshot map[string]memBackendSnapshot
	// The updated time of the metric that we've read last time.
	lastMetricTime monotime.Time
	mr             metricsreader.MetricsReader
	queryID        uint64
	bitNum         int
}

func NewFactorMemory(mr metricsreader.MetricsReader) *FactorMemory {
	bitNum := 0
	for levels := len(oomRiskLevels); ; bitNum++ {
		if levels == 0 {
			break
		}
		levels = levels >> 1
	}
	return &FactorMemory{
		mr:       mr,
		queryID:  mr.AddQueryExpr(memQueryExpr),
		bitNum:   bitNum,
		snapshot: make(map[string]memBackendSnapshot),
	}
}

func (fm *FactorMemory) Name() string {
	return "memory"
}

func (fm *FactorMemory) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	qr := fm.mr.GetQueryResult(fm.queryID)
	if qr.Err != nil || qr.Empty() {
		return
	}

	if qr.UpdateTime != fm.lastMetricTime {
		// Metrics have updated.
		fm.lastMetricTime = qr.UpdateTime
		fm.updateSnapshot(qr, backends)
	}
	if monotime.Since(fm.lastMetricTime) > memMetricExpDuration {
		// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
		return
	}

	for i := 0; i < len(backends); i++ {
		addr := backends[i].Addr()
		score := fm.calcMemScore(addr)
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
		if len(pairs) > 0 {
			latestUsage, timeToOOM := calcMemUsage(pairs)
			if latestUsage >= 0 {
				snapshots[addr] = memBackendSnapshot{
					updatedTime: qr.UpdateTime,
					memUsage:    latestUsage,
					timeToOOM:   timeToOOM,
				}
				valid = true
			}
		}
		// Merge the old snapshot just in case some metrics have missed for a short period.
		if !valid {
			if snapshot, ok := fm.snapshot[addr]; ok {
				if monotime.Since(snapshot.updatedTime) < memMetricExpDuration {
					snapshots[addr] = snapshot
				}
			}
		}
	}
	fm.snapshot = snapshots
}

func calcMemUsage(usageHistory []model.SamplePair) (latestUsage float64, timeToOOM time.Duration) {
	latestUsage = -1
	timeToOOM = time.Duration(0)
	if len(usageHistory) == 0 {
		return
	}
	var latestTime model.Time
	for i := len(usageHistory) - 1; i >= 0; i-- {
		value := float64(usageHistory[i].Value)
		if math.IsNaN(value) {
			continue
		}
		if value > 1 {
			value = 1
		}
		if latestUsage < 0 {
			latestUsage = value
			latestTime = usageHistory[i].Timestamp
		} else {
			diff := latestUsage - value
			if diff > 0.0001 {
				timeToOOM = time.Duration(float64(latestTime-usageHistory[i].Timestamp)*(1.0-latestUsage)/diff) * time.Millisecond
			} else {
				timeToOOM = time.Duration(math.MaxInt64)
			}
			break
		}
	}
	return
}

func (fm *FactorMemory) calcMemScore(addr string) int {
	usage := 1.0
	timeToOOM := time.Duration(0)
	if snapshot, ok := fm.snapshot[addr]; ok {
		usage = snapshot.memUsage
		timeToOOM = snapshot.timeToOOM
	}
	// If the metric of one backend is missing, treat it as unhealthy.
	// If the metrics of all backends are missing, give them the same scores.
	if usage < 0 || usage > 1 {
		usage = 1
	}
	score := 0
	for j := 0; j < len(oomRiskLevels); j++ {
		if usage > oomRiskLevels[j].memUsage || timeToOOM < oomRiskLevels[j].timeToOOM {
			score = len(oomRiskLevels) - j
			break
		}
	}
	return score
}

func (fm *FactorMemory) ScoreBitNum() int {
	return fm.bitNum
}

func (fm *FactorMemory) BalanceCount(from, to scoredBackend) int {
	// The risk level may change frequently, e.g. last time timeToOOM was 30s and connections were migrated away,
	// then this time it becomes 60s and the connections are migrated back.
	// So we only rebalance when the difference of risk levels of 2 backends is big enough.
	fromScore := fm.calcMemScore(from.Addr())
	toScore := fm.calcMemScore(to.Addr())
	if fromScore-toScore > 1 {
		return balanceCount4Mem
	}
	return 0
}

func (fm *FactorMemory) SetConfig(cfg *config.Config) {
}
