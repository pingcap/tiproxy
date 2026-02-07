// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInteractionSettings(t *testing.T) {
	defer SetQueryInteractionUserPatterns("")

	SetQueryInteractionEnabled(false)
	require.False(t, QueryInteractionEnabled())
	SetQueryInteractionEnabled(true)
	require.True(t, QueryInteractionEnabled())

	threshold := 321 * time.Millisecond
	SetQueryInteractionSlowLogThreshold(threshold)
	require.Equal(t, threshold, QueryInteractionSlowLogThreshold())

	interval := 123 * time.Second
	SetBackendMetricsGCInterval(interval)
	require.Equal(t, interval, BackendMetricsGCInterval())

	ttl := 456 * time.Second
	SetBackendMetricsGCIdleTTL(ttl)
	require.Equal(t, ttl, BackendMetricsGCIdleTTL())

	SetQueryInteractionUserPatterns("app_*, readonly")
	require.True(t, ShouldCollectQueryInteractionForUser("app_0"))
	require.True(t, ShouldCollectQueryInteractionForUser("readonly"))
	require.False(t, ShouldCollectQueryInteractionForUser("root"))

	SetQueryInteractionUserPatterns("")
	require.True(t, ShouldCollectQueryInteractionForUser("any-user"))
}
