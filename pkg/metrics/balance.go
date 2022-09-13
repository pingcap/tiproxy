// Copyright 2020 Ipalfish, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Label constants.
const (
	LblBackend       = "backend"
	LblFrom          = "from"
	LblTo            = "to"
	LblStatus        = "status"
	LblMigrateResult = "migrate_res"
)

var (
	BackendStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "b_status",
			Help:      "Gauge of backend status.",
		}, []string{LblBackend, LblStatus})

	BackendConnGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "b_conn",
			Help:      "Number of backend connections.",
		}, []string{LblBackend})

	MigrateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "migrate_total",
			Help:      "Number and result of session migration.",
		}, []string{LblFrom, LblTo, LblMigrateResult})

	MigrateDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "migrate_duration_millis",
			Help:      "Bucketed histogram of migrating time (s) of sessions.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 26), // 0.1ms ~ 1h
		}, []string{LblFrom, LblTo, LblMigrateResult})
)
