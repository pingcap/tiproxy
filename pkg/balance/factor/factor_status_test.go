// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFactorStatus(t *testing.T) {
	tests := []struct {
		healthy       bool
		expectedScore uint64
	}{
		{
			healthy:       true,
			expectedScore: 0,
		},
		{
			healthy:       false,
			expectedScore: 1,
		},
	}

	fs := NewFactorStatus(zap.NewNop())
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backend := scoredBackend{
			BackendCtx: &mockBackend{
				healthy: test.healthy,
			},
		}
		backends = append(backends, backend)
	}
	fs.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}

func TestStatusBalanceCount(t *testing.T) {
	tests := []struct {
		conn    int
		healthy bool
		count   float64
	}{
		{
			conn:    100,
			healthy: false,
			count:   100 / balanceSeconds4Status,
		},
		{
			conn:    50,
			healthy: false,
			count:   100 / balanceSeconds4Status,
		},
		{
			conn:    100,
			healthy: true,
			count:   0,
		},
		{
			conn:    50,
			healthy: false,
			count:   50 / balanceSeconds4Status,
		},
	}

	backends := make([]scoredBackend, 0, 2)
	backends = append(backends, createBackend(0, 0, 0))
	backends = append(backends, createBackend(1, 0, 0))
	unhealthyBackend := backends[0].BackendCtx.(*mockBackend)
	fs := NewFactorStatus(zap.NewNop())
	for i, test := range tests {
		unhealthyBackend.healthy = test.healthy
		unhealthyBackend.connScore = test.conn
		fs.UpdateScore(backends)
		if test.count == 0 {
			continue
		}
		advice, count, _ := fs.BalanceCount(backends[0], backends[1])
		require.Equal(t, AdvicePositive, advice, "test idx: %d", i)
		require.Equal(t, test.count, count, "test idx: %d", i)
	}
}

func TestMissBackendInStatus(t *testing.T) {
	backends := []scoredBackend{createBackend(0, 0, 0), createBackend(1, 0, 0)}
	unhealthyBackend := backends[0].BackendCtx.(*mockBackend)
	unhealthyBackend.connScore = 100
	unhealthyBackend.healthy = false

	fs := NewFactorStatus(zap.NewNop())
	fs.UpdateScore(backends)
	advice, count, _ := fs.BalanceCount(backends[0], backends[1])
	require.Equal(t, AdvicePositive, advice)
	require.Equal(t, 100/balanceSeconds4Status, count)

	// Miss the first backend but the snapshot should be preserved.
	fs.UpdateScore(backends[1:])
	unhealthyBackend.connScore = 50
	fs.UpdateScore(backends)
	advice, count, _ = fs.BalanceCount(backends[0], backends[1])
	require.Equal(t, AdvicePositive, advice)
	require.Equal(t, 100/balanceSeconds4Status, count)
}

func TestFactorStatusConfig(t *testing.T) {
	tests := []struct {
		migrations float64
		speed      float64
	}{
		{
			migrations: 0,
			speed:      20,
		},
		{
			migrations: 10,
			speed:      10,
		},
	}

	for i, test := range tests {
		backends := make([]scoredBackend, 0, 2)
		backends = append(backends, createBackend(0, 100, 100))
		backends = append(backends, createBackend(1, 100, 100))
		fs := NewFactorStatus(zap.NewNop())
		fs.SetConfig(&config.Config{Balance: config.Balance{Status: config.Factor{MigrationsPerSecond: test.migrations}}})
		require.EqualValues(t, test.migrations, fs.migrationsPerSecond)
		backends[0].BackendCtx.(*mockBackend).healthy = false
		fs.UpdateScore(backends)
		_, count, _ := fs.BalanceCount(backends[0], backends[1])
		require.Equal(t, test.speed, count, "test idx: %d", i)
	}
}
