// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"time"

	"github.com/pingcap/TiProxy/pkg/metrics"
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
	return val == 1
}

func addBackendConnMetrics(addr string) {
	metrics.BackendConnGauge.WithLabelValues(addr).Add(1)
}

func subBackendConnMetrics(addr string) {
	metrics.BackendConnGauge.WithLabelValues(addr).Sub(1)
}

func readBackendConnMetrics(addr string) (int, error) {
	return metrics.ReadGauge(metrics.BackendConnGauge.WithLabelValues(addr))
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
	metrics.MigrateDurationHistogram.WithLabelValues(from, to, resLabel).Observe(float64(cost.Milliseconds()))
}

func readMigrateCounter(from, to string, succeed bool) (int, error) {
	return metrics.ReadCounter(metrics.MigrateCounter.WithLabelValues(from, to, succeedToLabel(succeed)))
}
