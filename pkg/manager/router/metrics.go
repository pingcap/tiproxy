// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
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

func setBackendConnMetrics(addr string, conns int) {
	metrics.BackendConnGauge.WithLabelValues(addr).Set(float64(conns))
}

func readBackendConnMetrics(addr string) (int, error) {
	val, err := metrics.ReadGauge(metrics.BackendConnGauge.WithLabelValues(addr))
	return int(val), err
}

func succeedToLabel(succeed bool) string {
	if succeed {
		return "succeed"
	}
	return "fail"
}

func addMigrateMetrics(from, to string, succeed bool, startTime time.Time) {
	resLabel := succeedToLabel(succeed)
	metrics.MigrateCounter.WithLabelValues(from, to, resLabel).Inc()

	cost := time.Since(startTime)
	metrics.MigrateDurationHistogram.WithLabelValues(from, to, resLabel).Observe(cost.Seconds())
}

func readMigrateCounter(from, to string, succeed bool) (int, error) {
	return metrics.ReadCounter(metrics.MigrateCounter.WithLabelValues(from, to, succeedToLabel(succeed)))
}

func setPingBackendMetrics(addr string, succeed bool, startTime time.Time) {
	cost := time.Since(startTime)
	metrics.PingBackendGauge.WithLabelValues(addr).Set(cost.Seconds())
}

func readHealthCheckCycle() (time.Duration, error) {
	seconds, err := metrics.ReadGauge(metrics.HealthCheckCycleGauge)
	return time.Duration(int(seconds * float64(time.Second))), err
}
