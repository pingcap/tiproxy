// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	LblRes        = "res"
	LblMetricName = "metric"
)

var (
	BackendStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "b_status",
			Help:      "Gauge of backend status.",
		}, []string{LblBackend})

	GetBackendHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "get_backend_duration_seconds",
			Help:      "Bucketed histogram of time (s) for getting an available backend.",
			Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 26), // 1us ~ 30s
		})

	GetBackendCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "get_backend",
			Help:      "Counter of getting backend.",
		}, []string{LblRes})

	DialBackendFailCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "dial_backend_fail",
			Help:      "Counter of failing to dial backends.",
		}, []string{LblBackend})

	PingBackendGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "ping_duration_seconds",
			Help:      "Time (s) of pinging the SQL port of each backend.",
		}, []string{LblBackend})

	HealthCheckCycleGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "health_check_seconds",
			Help:      "Time (s) of each health check cycle.",
		})

	BackendMetricGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBackend,
			Name:      "backend_metric",
			Help:      "The backend metric.",
		}, []string{LblBackend, LblMetricName})
)
