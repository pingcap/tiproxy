// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
)

func updateBackendStatusMetrics(addr string, prevStatus, curStatus BackendStatus) {
	metrics.BackendStatusGauge.WithLabelValues(addr, prevStatus.String()).Set(0)
	metrics.BackendStatusGauge.WithLabelValues(addr, curStatus.String()).Set(1)
}

func checkBackendStatusMetrics(addr string, status BackendStatus) bool {
	val, err := metrics.ReadGauge(metrics.BackendStatusGauge.WithLabelValues(addr, status.String()))
	if err != nil {
		return false
	}
	return int(val) == 1
}

func setPingBackendMetrics(addr string, startTime monotime.Time) {
	cost := monotime.Since(startTime)
	metrics.PingBackendGauge.WithLabelValues(addr).Set(cost.Seconds())
}

func readHealthCheckCycle() (time.Duration, error) {
	seconds, err := metrics.ReadGauge(metrics.HealthCheckCycleGauge)
	return time.Duration(int(seconds * float64(time.Second))), err
}
