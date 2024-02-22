// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	InboundBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "inbound_bytes",
			Help:      "Counter of bytes from backends.",
		}, []string{LblBackend})

	InboundPacketsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "inbound_packets",
			Help:      "Counter of packets from backends.",
		}, []string{LblBackend})

	OutboundBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "outbound_bytes",
			Help:      "Counter of bytes to backends.",
		}, []string{LblBackend})

	OutboundPacketsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelTraffic,
			Name:      "outbound_packets",
			Help:      "Counter of packets to backends.",
		}, []string{LblBackend})
)
