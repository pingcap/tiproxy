// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
)

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

func addMigrateMetrics(from, to, reason string, succeed bool, startTime time.Time) {
	resLabel := succeedToLabel(succeed)
	metrics.MigrateCounter.WithLabelValues(from, to, reason, resLabel).Inc()
	metrics.PendingMigrateGuage.WithLabelValues(from, to, reason).Dec()

	cost := time.Since(startTime)
	metrics.MigrateDurationHistogram.WithLabelValues(from, to, resLabel).Observe(cost.Seconds())
}

func readMigrateCounter(from, to string, succeed bool) (int, error) {
	v1, err := metrics.ReadCounter(metrics.MigrateCounter.WithLabelValues(from, to, "status", succeedToLabel(succeed)))
	if err != nil {
		return v1, err
	}
	v2, err := metrics.ReadCounter(metrics.MigrateCounter.WithLabelValues(from, to, "conn", succeedToLabel(succeed)))
	if err != nil {
		return v2, err
	}
	return v1 + v2, nil
}
