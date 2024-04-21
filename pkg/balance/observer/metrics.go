// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
)

const (
	healthyLabel   = "healthy"
	unhealthyLabel = "down"
)

func updateBackendStatusMetrics(addr string, prevHealthy, curHealthy bool) {
	metrics.BackendStatusGauge.WithLabelValues(addr, getHealthLabel(prevHealthy)).Set(0)
	metrics.BackendStatusGauge.WithLabelValues(addr, getHealthLabel(curHealthy)).Set(1)
}

func checkBackendStatusMetrics(addr string, healthy bool) bool {
	val, err := metrics.ReadGauge(metrics.BackendStatusGauge.WithLabelValues(addr, getHealthLabel(healthy)))
	if err != nil {
		return false
	}
	return int(val) == 1
}

func getHealthLabel(healthy bool) string {
	if healthy {
		return healthyLabel
	}
	return unhealthyLabel
}

func setPingBackendMetrics(addr string, startTime monotime.Time) {
	cost := monotime.Since(startTime)
	metrics.PingBackendGauge.WithLabelValues(addr).Set(cost.Seconds())
}

func readHealthCheckCycle() (time.Duration, error) {
	seconds, err := metrics.ReadGauge(metrics.HealthCheckCycleGauge)
	return time.Duration(int(seconds * float64(time.Second))), err
}
