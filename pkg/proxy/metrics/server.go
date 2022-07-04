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

var (
	// PanicCounter measures the count of panics.
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelServer,
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblCluster, LblType})

	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelServer,
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblCluster, LblType, LblResult})

	ExecuteErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelServer,
			Name:      "execute_error_total",
			Help:      "Counter of execute errors.",
		}, []string{LblCluster, LblType})

	ConnGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelServer,
			Name:      "connections",
			Help:      "Number of connections.",
		}, []string{LblCluster})

	EventStart        = "start"
	EventGracefulDown = "graceful_shutdown"
	// Eventkill occurs when the server.Kill() function is called.
	EventKill  = "kill"
	EventClose = "close"
)
