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

import "github.com/prometheus/client_golang/prometheus"

const (
	BackendEventIniting = "initing"
	BackendEventInited  = "inited"
	BackendEventClosing = "closing"
	BackendEventClosed  = "closed"
)

var (
	BackendEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "backend_event_total",
			Help:      "Counter of backend event.",
		}, []string{LblCluster, LblNamespace, LblType})

	BackendQueryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "b_conn_cnt",
			Help:      "Counter of backend query count.",
		}, []string{LblCluster, LblNamespace, LblBackendAddr})

	BackendConnInUseGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "b_conn_in_use",
			Help:      "Number of backend conn in use.",
		}, []string{LblCluster, LblNamespace, LblBackendAddr})
)
