// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteSingleDashLongFlags(t *testing.T) {
	require.Equal(t,
		[]string{"--query-interaction-metrics", "--config", "a.toml"},
		rewriteSingleDashLongFlags([]string{"-query-interaction-metrics", "-config", "a.toml"}),
	)
	require.Equal(t,
		[]string{"--query-interaction-metrics=true"},
		rewriteSingleDashLongFlags([]string{"-query_interaction_metrics=true"}),
	)
	// Unknown flags and real short flags should be left untouched.
	require.Equal(t,
		[]string{"-v", "-x", "--log_level", "info"},
		rewriteSingleDashLongFlags([]string{"-v", "-x", "--log_level", "info"}),
	)
}
