// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestParseCIDR(t *testing.T) {
	tests := []struct {
		cidrs   []string
		success bool
	}{
		{
			cidrs:   []string{"1.1.1.1"},
			success: false,
		},
		{
			cidrs:   []string{"1.1.1.1/32"},
			success: true,
		},
		{
			cidrs:   []string{"1.1.1.1/33"},
			success: false,
		},
		{
			cidrs:   []string{"1.1.1.1/31"},
			success: true,
		},
		{
			cidrs:   []string{"1.1.1.1/30", "abc"},
			success: false,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for _, test := range tests {
		g, err := NewGroup(test.cidrs, func(lg *zap.Logger) policy.BalancePolicy {
			return nil
		}, MatchCIDR, lg)
		if test.success {
			require.NoError(t, err)
			require.Equal(t, len(test.cidrs), len(g.cidrList))
			require.True(t, g.EqualValues(test.cidrs))
		} else {
			require.Error(t, err)
		}
	}
}

func TestMatchIP(t *testing.T) {
	tests := []struct {
		ip      string
		cidrs   []string
		success bool
	}{
		{
			ip:      "1.1.1.1",
			cidrs:   []string{"1.1.1.1/32"},
			success: true,
		},
		{
			ip:      "1.1.1.2",
			cidrs:   []string{"1.1.1.1/30"},
			success: true,
		},
		{
			ip:      "1.1.1.100",
			cidrs:   []string{"1.1.1.1/30"},
			success: false,
		},
		{
			ip:      "1.1.1.100",
			cidrs:   []string{"1.1.1.1/30", "1.1.1.101/30"},
			success: true,
		},
		{
			ip:      "abc",
			cidrs:   []string{"1.1.1.1/30"},
			success: false,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for _, test := range tests {
		g, err := NewGroup(test.cidrs, func(lg *zap.Logger) policy.BalancePolicy {
			return nil
		}, MatchCIDR, lg)
		require.NoError(t, err)
		require.Equal(t, test.success, g.Match(test.ip))
	}
}
