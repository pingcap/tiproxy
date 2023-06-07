// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	LblType = "type"

	EventStart = "start"
	EventClose = "close"
)

var (
	ConnGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelServer,
			Name:      "connections",
			Help:      "Number of connections.",
		})

	MaxProcsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelServer,
			Name:      "maxprocs",
			Help:      "The value of GOMAXPROCS.",
		})

	ServerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelServer,
			Name:      "event",
			Help:      "Counter of TiProxy event.",
		}, []string{LblType})

	ServerErrCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelServer,
			Name:      "err",
			Help:      "Counter of server error.",
		}, []string{LblType})

	TimeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelMonitor,
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})

	KeepAliveCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelMonitor,
			Name:      "keep_alive_total",
			Help:      "Counter of proxy keep alive.",
		})
)
