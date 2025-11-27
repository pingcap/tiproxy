// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"net"
	"strings"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var nopBpCreator = func(*zap.Logger) policy.BalancePolicy {
	return nil
}

func TestParseCIDR(t *testing.T) {
	tests := []struct {
		cidrs   []string
		success bool
	}{
		{
			cidrs:   []string{"1.1.1.1"},
			success: true,
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
		g, err := NewGroup(test.cidrs, nopBpCreator, MatchClientCIDR, lg)
		if test.success {
			require.NoError(t, err)
			require.Equal(t, len(test.cidrs), len(g.cidrList))
			require.EqualValues(t, test.cidrs, g.values)
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
	for _, matchType := range []MatchType{MatchClientCIDR, MatchProxyCIDR} {
		for _, test := range tests {
			g, err := NewGroup(test.cidrs, nopBpCreator, matchType, lg)
			require.NoError(t, err)
			ci := ClientInfo{}
			addr := &net.TCPAddr{IP: net.ParseIP(test.ip), Port: 10000}
			if matchType == MatchProxyCIDR {
				ci.ProxyAddr = addr
			} else {
				ci.ClientAddr = addr
			}
			require.Equal(t, test.success, g.Match(ci))
		}
	}
}

func TestRefreshCidr(t *testing.T) {
	tests := []struct {
		cidrs1    []string
		cidrs2    []string
		final     []string
		intersect bool
	}{
		{
			cidrs1:    []string{"1.1.1.1/32"},
			cidrs2:    []string{"1.1.1.1/32"},
			final:     []string{"1.1.1.1/32"},
			intersect: true,
		},
		{
			cidrs1:    []string{"1.1.1.1/32"},
			cidrs2:    []string{"1.1.1.2/32"},
			intersect: false,
		},
		{
			cidrs1:    []string{"1.1.1.1/24", "1.1.2.1/24"},
			cidrs2:    []string{"1.1.1.1/24", "1.1.2.1/24"},
			final:     []string{"1.1.1.1/24", "1.1.2.1/24"},
			intersect: true,
		},
		{
			cidrs1:    []string{"1.1.1.1/24", "1.1.2.1/24"},
			cidrs2:    []string{"1.1.1.1/24"},
			final:     []string{"1.1.1.1/24", "1.1.2.1/24"},
			intersect: true,
		},
		{
			cidrs1:    []string{"1.1.1.1/24"},
			cidrs2:    []string{"1.1.1.1/24", "1.1.2.1/24"},
			final:     []string{"1.1.1.1/24", "1.1.2.1/24"},
			intersect: true,
		},
		{
			cidrs1:    []string{"1.1.1.1/24", "1.1.2.1/24"},
			cidrs2:    []string{"1.1.1.1/24", "1.1.3.1/24"},
			final:     []string{"1.1.1.1/24", "1.1.2.1/24", "1.1.3.1/24"},
			intersect: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for _, test := range tests {
		g1, err := NewGroup(test.cidrs1, nopBpCreator, MatchClientCIDR, lg)
		require.NoError(t, err)
		require.Equal(t, test.intersect, g1.Intersect(test.cidrs2))
		if !test.intersect {
			continue
		}

		b1, b2 := &backendWrapper{}, &backendWrapper{}
		b1.mu.BackendHealth.Labels = map[string]string{config.CidrLabelName: strings.Join(test.cidrs1, ",")}
		b2.mu.BackendHealth.Labels = map[string]string{config.CidrLabelName: strings.Join(test.cidrs2, ",")}
		g1.AddBackend("1", b1)
		g1.AddBackend("2", b2)
		g1.RefreshCidr()
		require.True(t, g1.EqualValues(test.final))
		require.Equal(t, len(g1.values), len(g1.cidrList))
	}
}
