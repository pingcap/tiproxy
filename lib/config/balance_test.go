// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckBalance(t *testing.T) {
	balances := []Balance{
		{
			Policy: "test",
		},
		{
			RoutingPolicy: "test",
		},
		{
			Status: StatusFactor{MigrationsPerSecond: -1},
		},
		{
			Health: Factor{MigrationsPerSecond: -1},
		},
		{
			Memory: Factor{MigrationsPerSecond: -1},
		},
		{
			CPU: CPUFactor{MigrationsPerSecond: -1},
		},
		{
			CPU: CPUFactor{MinBalanceUsage: -0.1},
		},
		{
			CPU: CPUFactor{MinBalanceUsage: 1.1},
		},
		{
			CPU: CPUFactor{MaxUsageGap: 0.04},
		},
		{
			CPU: CPUFactor{MaxUsageGap: 1.1},
		},
		{
			Location: Factor{MigrationsPerSecond: -1},
		},
		{
			ConnCount: ConnCountFactor{MigrationsPerSecond: -1},
		},
		{
			ConnCount: ConnCountFactor{CountRatioThreshold: -1},
		},
	}

	for i, balance := range balances {
		require.Error(t, balance.Check(), "%d", i)
	}

	balance := Balance{}
	require.NoError(t, (&balance).Check())
	require.Equal(t, 1.0, balance.CPU.MaxUsageGap)
	balance = DefaultBalance()
	require.NoError(t, (&balance).Check())
	balance.RoutingPolicy = RoutingPolicyIdlest
	require.NoError(t, (&balance).Check())

	balance = DefaultBalance()
	balance.RoutingRule = MatchPortStr
	require.NoError(t, balance.Check())

	balance.RoutingRule = "unknown"
	require.ErrorIs(t, balance.Check(), ErrInvalidConfigValue)
}

func TestMigrationsPerSecondLimit(t *testing.T) {
	setters := []struct {
		name string
		set  func(*Balance, float64)
	}{
		{"status", func(b *Balance, v float64) { b.Status.MigrationsPerSecond = v }},
		{"health", func(b *Balance, v float64) { b.Health.MigrationsPerSecond = v }},
		{"memory", func(b *Balance, v float64) { b.Memory.MigrationsPerSecond = v }},
		{"cpu", func(b *Balance, v float64) { b.CPU.MigrationsPerSecond = v }},
		{"location", func(b *Balance, v float64) { b.Location.MigrationsPerSecond = v }},
		{"conn-count", func(b *Balance, v float64) { b.ConnCount.MigrationsPerSecond = v }},
	}
	for _, setter := range setters {
		balance := DefaultBalance()
		setter.set(&balance, maxMigrationsPerSecond)
		require.NoError(t, balance.Check(), setter.name)

		for _, v := range []float64{maxMigrationsPerSecond + 1, math.NaN(), math.Inf(1)} {
			balance = DefaultBalance()
			setter.set(&balance, v)
			require.ErrorIs(t, balance.Check(), ErrInvalidConfigValue, "%s %v", setter.name, v)
		}
	}
}

func TestOptionalFactorEnabled(t *testing.T) {
	balance := DefaultBalance()
	require.True(t, balance.Health.Enabled)
	require.True(t, balance.Memory.Enabled)
	require.True(t, balance.CPU.Enabled)
	require.Zero(t, balance.CPU.MinBalanceUsage)
	require.Equal(t, 1.0, balance.CPU.MaxUsageGap)
	require.True(t, balance.Location.Enabled)
}
