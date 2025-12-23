// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
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
			Status: Factor{MigrationsPerSecond: -1},
		},
		{
			Health: Factor{MigrationsPerSecond: -1},
		},
		{
			Memory: Factor{MigrationsPerSecond: -1},
		},
		{
			CPU: Factor{MigrationsPerSecond: -1},
		},
		{
			Location: Factor{MigrationsPerSecond: -1},
		},
		{
			ConnCount: ConnCountFactor{Factor: Factor{MigrationsPerSecond: -1}},
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
	balance = DefaultBalance()
	require.NoError(t, (&balance).Check())
}
