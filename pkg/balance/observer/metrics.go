// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
)

func updateBackendStatusMetrics(addr string, healthy bool) {
	val := 0
	if healthy {
		val = 1
	}
	metrics.BackendStatusGauge.WithLabelValues(addr).Set(float64(val))
}

func checkBackendStatusMetrics(addr string, healthy bool) bool {
	val, err := metrics.ReadGauge(metrics.BackendStatusGauge.WithLabelValues(addr))
	if err != nil {
		return false
	}
	expected := 0
	if healthy {
		expected = 1
	}
	return int(val) == expected
}

func setPingBackendMetrics(addr string, startTime time.Time) {
	cost := time.Since(startTime)
	metrics.PingBackendGauge.WithLabelValues(addr).Set(cost.Seconds())
}

func readHealthCheckCycle() (time.Duration, error) {
	seconds, err := metrics.ReadGauge(metrics.HealthCheckCycleGauge)
	return time.Duration(int(seconds * float64(time.Second))), err
}
