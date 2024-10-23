// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ReplayPendingCmdsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelReplay,
			Name:      "pending_cmds",
			Help:      "Counter of pending commands.",
		})

	ReplayWaitTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelReplay,
			Name:      "wait_time",
			Help:      "Wait time of replaying commands.",
		})
)
