// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"path"
	"strings"
	"sync/atomic"
	"time"
)

var queryInteractionEnabled atomic.Bool
var queryInteractionSlowLogThreshold atomic.Int64
var queryInteractionSlowLogOnlyDigest atomic.Bool
var backendMetricsGCInterval atomic.Int64
var backendMetricsGCIdleTTL atomic.Int64
var queryInteractionUserMatcherPtr atomic.Pointer[queryInteractionUserMatcher]

type queryInteractionUserMatcher struct {
	patterns []string
}

func init() {
	queryInteractionSlowLogThreshold.Store(int64(200 * time.Millisecond))
	backendMetricsGCInterval.Store(int64(5 * time.Minute))
	backendMetricsGCIdleTTL.Store(int64(time.Hour))
	SetQueryInteractionUserPatterns("")
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

// SetQueryInteractionSlowLogOnlyDigest updates whether slow interaction logs should only print SQL digest
// and omit the normalized query text.
func SetQueryInteractionSlowLogOnlyDigest(onlyDigest bool) {
	queryInteractionSlowLogOnlyDigest.Store(onlyDigest)
}

// QueryInteractionSlowLogOnlyDigest returns whether slow interaction logs should only print SQL digest.
func QueryInteractionSlowLogOnlyDigest() bool {
	return queryInteractionSlowLogOnlyDigest.Load()
}

// SetQueryInteractionUserPatterns updates username glob filters for per-interaction metrics.
// Comma separates multiple patterns; empty means allowing all users.
func SetQueryInteractionUserPatterns(patterns string) {
	filterPatterns := make([]string, 0, 4)
	for _, pattern := range strings.Split(patterns, ",") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		filterPatterns = append(filterPatterns, pattern)
	}
	queryInteractionUserMatcherPtr.Store(&queryInteractionUserMatcher{
		patterns: filterPatterns,
	})
}

// ShouldCollectQueryInteractionForUser reports whether per-interaction metrics should be emitted for this user.
func ShouldCollectQueryInteractionForUser(user string) bool {
	matched, _ := MatchQueryInteractionUserPattern(user)
	return matched
}

// MatchQueryInteractionUserPattern reports whether the user matches configured patterns
// and returns the matched pattern when available.
func MatchQueryInteractionUserPattern(user string) (matched bool, pattern string) {
	matcher := queryInteractionUserMatcherPtr.Load()
	if matcher == nil || len(matcher.patterns) == 0 {
		return true, ""
	}
	for _, pattern := range matcher.patterns {
		matched, err := path.Match(pattern, user)
		if err != nil {
			continue
		}
		if matched {
			return true, pattern
		}
	}
	return false, ""
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
