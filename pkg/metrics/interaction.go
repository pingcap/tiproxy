// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync/atomic"
	"time"
)

var queryInteractionEnabled atomic.Bool
var queryInteractionSlowLogThreshold atomic.Int64
var backendMetricsGCInterval atomic.Int64
var backendMetricsGCIdleTTL atomic.Int64

func init() {
	queryInteractionSlowLogThreshold.Store(int64(200 * time.Millisecond))
	backendMetricsGCInterval.Store(int64(5 * time.Minute))
	backendMetricsGCIdleTTL.Store(int64(time.Hour))
}

// SetQueryInteractionEnabled updates whether per-interaction query latency metrics are emitted.
func SetQueryInteractionEnabled(enabled bool) {
	queryInteractionEnabled.Store(enabled)
}

// QueryInteractionEnabled returns whether per-interaction query latency metrics are enabled.
func QueryInteractionEnabled() bool {
	return queryInteractionEnabled.Load()
}

// SetQueryInteractionSlowLogThreshold updates the slow log threshold of per-interaction latency.
func SetQueryInteractionSlowLogThreshold(threshold time.Duration) {
	queryInteractionSlowLogThreshold.Store(int64(threshold))
}

// QueryInteractionSlowLogThreshold returns the slow log threshold of per-interaction latency.
func QueryInteractionSlowLogThreshold() time.Duration {
	return time.Duration(queryInteractionSlowLogThreshold.Load())
}

// SetBackendMetricsGCInterval updates how often backend metric labels are GC'ed.
func SetBackendMetricsGCInterval(interval time.Duration) {
	backendMetricsGCInterval.Store(int64(interval))
}

// BackendMetricsGCInterval returns the GC interval for backend metric labels.
func BackendMetricsGCInterval() time.Duration {
	return time.Duration(backendMetricsGCInterval.Load())
}

// SetBackendMetricsGCIdleTTL updates the idle TTL for backend metric labels.
func SetBackendMetricsGCIdleTTL(ttl time.Duration) {
	backendMetricsGCIdleTTL.Store(int64(ttl))
}

// BackendMetricsGCIdleTTL returns the idle TTL for backend metric labels.
func BackendMetricsGCIdleTTL() time.Duration {
	return time.Duration(backendMetricsGCIdleTTL.Load())
}
