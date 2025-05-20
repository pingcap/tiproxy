// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Label constants.
const (
	LblBackend       = "backend"
	LblFrom          = "from"
	LblTo            = "to"
	LblReason        = "reason"
	LblMigrateResult = "migrate_res"
	LblFactor        = "factor"
)

var (
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
		}, []string{LblFrom, LblTo, LblReason, LblMigrateResult})

	MigrateDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "migrate_duration_seconds",
			Help:      "Bucketed histogram of migrating time (s) of sessions.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 26), // 0.1ms ~ 1h
		}, []string{LblFrom, LblTo, LblMigrateResult})

	PendingMigrateGuage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "pending_migrate",
			Help:      "Number of pending session migration.",
		}, []string{LblFrom, LblTo, LblReason})

	BackendScoreGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelBalance,
			Name:      "b_score",
			Help:      "Gauge of backend scores.",
		}, []string{LblBackend, LblFactor})
)
