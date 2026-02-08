// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

func TestSlowInteractionLogSQLDigestOnly(t *testing.T) {
	originEnabled := metrics.QueryInteractionEnabled()
	originThreshold := metrics.QueryInteractionSlowLogThreshold()
	originOnlyDigest := metrics.QueryInteractionSlowLogOnlyDigest()
	defer metrics.SetQueryInteractionEnabled(originEnabled)
	defer metrics.SetQueryInteractionSlowLogThreshold(originThreshold)
	defer metrics.SetQueryInteractionSlowLogOnlyDigest(originOnlyDigest)
	defer metrics.SetQueryInteractionUserPatterns("")

	metrics.SetQueryInteractionEnabled(true)
	metrics.SetQueryInteractionSlowLogThreshold(time.Nanosecond)
	metrics.SetQueryInteractionSlowLogOnlyDigest(true)
	metrics.SetQueryInteractionUserPatterns("")

	tc := newTCPConnSuite(t)
	ts, clean := newTestSuite(t, tc, func(cfg *testConfig) {
		cfg.clientConfig.cmd = pnet.ComQuery
		cfg.clientConfig.sql = "select 1"
		cfg.backendConfig.respondType = responseTypeOK
	})
	defer clean()
	ts.authenticateFirstTime(t, nil)
	ts.executeCmd(t, nil)

	logText := ts.mp.text.String()
	require.Contains(t, logText, "slow mysql interaction")
	require.Contains(t, logText, "sql_digest")
	require.False(t, strings.Contains(logText, "select ?"), logText)
}

func TestSlowInteractionLogSQLDigestAndQuery(t *testing.T) {
	originEnabled := metrics.QueryInteractionEnabled()
	originThreshold := metrics.QueryInteractionSlowLogThreshold()
	originOnlyDigest := metrics.QueryInteractionSlowLogOnlyDigest()
	defer metrics.SetQueryInteractionEnabled(originEnabled)
	defer metrics.SetQueryInteractionSlowLogThreshold(originThreshold)
	defer metrics.SetQueryInteractionSlowLogOnlyDigest(originOnlyDigest)
	defer metrics.SetQueryInteractionUserPatterns("")

	metrics.SetQueryInteractionEnabled(true)
	metrics.SetQueryInteractionSlowLogThreshold(time.Nanosecond)
	metrics.SetQueryInteractionSlowLogOnlyDigest(false)
	metrics.SetQueryInteractionUserPatterns("")

	tc := newTCPConnSuite(t)
	ts, clean := newTestSuite(t, tc, func(cfg *testConfig) {
		cfg.clientConfig.cmd = pnet.ComQuery
		cfg.clientConfig.sql = "select 1"
		cfg.backendConfig.respondType = responseTypeOK
	})
	defer clean()
	ts.authenticateFirstTime(t, nil)
	ts.executeCmd(t, nil)

	logText := ts.mp.text.String()
	require.Contains(t, logText, "slow mysql interaction")
	require.Contains(t, logText, "sql_digest")
	require.True(t, strings.Contains(logText, "select ?"), logText)
}
