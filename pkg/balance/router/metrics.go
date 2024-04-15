// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
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

func addMigrateMetrics(from, to string, succeed bool, startTime monotime.Time) {
	resLabel := succeedToLabel(succeed)
	metrics.MigrateCounter.WithLabelValues(from, to, resLabel).Inc()

	cost := monotime.Since(startTime)
	metrics.MigrateDurationHistogram.WithLabelValues(from, to, resLabel).Observe(cost.Seconds())
}

func readMigrateCounter(from, to string, succeed bool) (int, error) {
	return metrics.ReadCounter(metrics.MigrateCounter.WithLabelValues(from, to, succeedToLabel(succeed)))
}
