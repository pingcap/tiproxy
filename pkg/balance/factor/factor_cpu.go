// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"math"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
)

const (
	cpuEwmaAlpha = 0.5
	// If some metrics are missing, we use the old one temporarily for no longer than metricExpDuration.
	metricExpDuration = 2 * time.Minute
	cpuScoreStep      = 5
	// 0.001 represents for 0.1%
	minCpuPerConn    = 0.001
	cpuBalancedRatio = 1.3
	balanceCount4Cpu = 1
)

var _ Factor = (*FactorCPU)(nil)

var (
	cpuQueryExpr = metricsreader.QueryExpr{
		PromQL:   `rate(process_cpu_seconds_total/tidb_server_maxprocs{%s="tidb"}[15s])`,
		HasLabel: true,
		Range:    3 * time.Minute,
	}
)

type backendSnapshot struct {
	updatedTime monotime.Time
	avgUsage    float64
	latestUsage float64
	connCount   int
}

type FactorCPU struct {
	// The snapshot of backend statistics when the matrix was updated.
	snapshot map[string]backendSnapshot
	// The updated time of the metric that we've read last time.
	lastMetricTime monotime.Time
	// The estimated average CPU usage used by one connection.
	usagePerConn float64
	mr           metricsreader.MetricsReader
	queryID      uint64
	bitNum       int
}

func NewFactorCPU(mr metricsreader.MetricsReader) *FactorCPU {
	return &FactorCPU{
		mr:       mr,
		queryID:  mr.AddQueryExpr(cpuQueryExpr),
		bitNum:   5,
		snapshot: make(map[string]backendSnapshot),
	}
}

func (fc *FactorCPU) Name() string {
	return "cpu"
}

func (fc *FactorCPU) UpdateScore(backends []scoredBackend) {
	if len(backends) <= 1 {
		return
	}
	qr := fc.mr.GetQueryResult(fc.queryID)
	if qr.Err != nil || qr.Empty() {
		return
	}

	if qr.UpdateTime != fc.lastMetricTime {
		// Metrics have updated.
		fc.lastMetricTime = qr.UpdateTime
		fc.updateSnapshot(qr, backends)
		fc.updateCpuPerConn()
	}
	if monotime.Since(fc.lastMetricTime) > metricExpDuration {
		// The metrics have not been updated for a long time (maybe Prometheus is unavailable).
		return
	}

	for i := 0; i < len(backends); i++ {
		// Negative indicates missing metric.
		avgUsage := -1.0
		addr := backends[i].Addr()
		// Estimate the current cpu usage by the latest CPU usage, the latest connection count, and the current connection count.
		if snapshot, ok := fc.snapshot[addr]; ok {
			histConnCount := snapshot.connCount
			curConnCount := backends[i].ConnScore()
			if snapshot.avgUsage >= 0 {
				avgUsage = snapshot.avgUsage + float64(curConnCount-histConnCount)*fc.usagePerConn
				if avgUsage < 0 {
					avgUsage = 0
				}
			}
		}
		// If the metric of one backend is missing, treat it as unhealthy.
		// If the metrics of all backends are missing, give them the same scores.
		if avgUsage < 0 || avgUsage > 1 {
			avgUsage = 1
		}
		backends[i].addScore(int(avgUsage*100)/cpuScoreStep, fc.bitNum)
	}
}

func (fc *FactorCPU) updateSnapshot(qr metricsreader.QueryResult, backends []scoredBackend) {
	snapshots := make(map[string]backendSnapshot, len(fc.snapshot))
	for _, backend := range backends {
		addr := backend.Addr()
		valid := false
		// If a backend exists in metrics but not in the backend list, ignore it for this round.
		// The backend will be in the next round if it's healthy.
		pairs := qr.GetMetric4Backend(backend)
		if len(pairs) > 0 {
			avgUsage, latestUsage := calcAvgUsage(pairs)
			if avgUsage >= 0 {
				snapshots[addr] = backendSnapshot{
					avgUsage:    avgUsage,
					latestUsage: latestUsage,
					connCount:   backend.ConnCount(),
					updatedTime: qr.UpdateTime,
				}
				valid = true
			}
		}
		// Merge the old snapshot just in case some metrics have missed for a short period.
		if !valid {
			if snapshot, ok := fc.snapshot[addr]; ok {
				if monotime.Since(snapshot.updatedTime) < metricExpDuration {
					snapshots[addr] = snapshot
				}
			}
		}
	}
	fc.snapshot = snapshots
}

func calcAvgUsage(usageHistory []model.SamplePair) (avgUsage, latestUsage float64) {
	avgUsage, latestUsage = -1, -1
	if len(usageHistory) == 0 {
		return
	}
	// The CPU usage jitters too much, so use the EWMA algorithm to make it smooth.
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
		if usagePerConn < minCpuPerConn {
			// If the average usage is below 10%, we take the cluster as just started and don't update usagePerConn.
			if totalUsage/float64(len(fc.snapshot)) > 0.1 {
				fc.usagePerConn = usagePerConn
			}
		} else {
			fc.usagePerConn = usagePerConn
		}
	}
	if fc.usagePerConn <= 0 {
		fc.usagePerConn = minCpuPerConn
	}
}

func (fc *FactorCPU) ScoreBitNum() int {
	return fc.bitNum
}

func (fc *FactorCPU) BalanceCount(from, to scoredBackend) int {
	var fromUsage, toUsage float64
	if fromSnapshot, ok := fc.snapshot[from.Addr()]; !ok || fromSnapshot.avgUsage < 0 {
		// The metric has missed for minutes.
		fromUsage = 1
	} else {
		fromUsage = fromSnapshot.avgUsage + float64(from.ConnScore()-fromSnapshot.connCount)*fc.usagePerConn
		if fromUsage > 1 {
			fromUsage = 1
		}
	}
	if toSnapshot, ok := fc.snapshot[to.Addr()]; !ok || toSnapshot.avgUsage < 0 {
		// impossible
		return 0
	} else {
		toUsage = toSnapshot.avgUsage + float64(to.ConnScore()-toSnapshot.connCount)*fc.usagePerConn
		if toUsage > 1 {
			toUsage = 1
		}
	}
	// The higher the CPU usage, the more sensitive the load balance should be.
	// E.g. 10% vs 25% don't need rebalance, but 80% vs 95% need rebalance.
	if 1.1-toUsage > (1.1-fromUsage)*cpuBalancedRatio {
		return balanceCount4Cpu
	}
	return 0
}
