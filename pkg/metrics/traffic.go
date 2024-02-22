// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ClientPacketsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "client_packets",
			Help:      "Counter of packets from clients.",
		})

	BackendPacketsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "backend_packets",
			Help:      "Counter of packets from backends.",
		}, []string{LblBackend})

	ClientBytesCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "client_bytes",
			Help:      "Counter of bytes from clients.",
		})

	BackendBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "backend_bytes",
			Help:      "Counter of bytes from backends.",
		}, []string{LblBackend})
)
