// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// LblCmdType and LblSQLType are label constants.
const (
	LblCmdType = "cmd_type"
	LblSQLType = "sql_type"
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

	QueryInteractionDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "query_interaction_duration_seconds",
			Help:      "Bucketed histogram of request to first response latency (s) for handled commands.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblBackend, LblCmdType, LblSQLType})

	HandshakeDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelSession,
			Name:      "handshake_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handshakes.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblBackend})
)
