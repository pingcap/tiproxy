// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// LblCmdType is the label constant.
const (
	LblCmdType = "cmd_type"
)

var (
	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblBackend, LblCmdType})

	QueryDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblBackend, LblCmdType})

	HandshakeDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "handshake_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handshakes.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblBackend})

	QueryTimeSinceConnCreationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "query_time_since_conn_creation_seconds",
			Help:      "Bucketed histogram of query start time (s) since connection creation.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21), // 1s ~ 24days
		})

	ConnLifetimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "conn_lifetime_seconds",
			Help:      "Bucketed histogram of connection lifetime (s).",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 25), // 1s ~ 38days
		})
)
