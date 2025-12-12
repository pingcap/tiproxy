// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/stretchr/testify/require"
)

func TestFactorLocationScore(t *testing.T) {
	tests := []struct {
		local         bool
		expectedScore uint64
	}{
		{
			local:         false,
			expectedScore: 1,
		},
		{
			local:         true,
			expectedScore: 0,
		},
	}

	factor := NewFactorLocation()
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backends = append(backends, scoredBackend{
			BackendCtx: &mockBackend{
				local: test.local,
			},
		})
	}
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}

func TestFactorLocationConfig(t *testing.T) {
	tests := []struct {
		local      []bool
		migrations float64
		speed      float64
	}{
		{
			local:      []bool{true, false},
			migrations: 0,
			speed:      1,
		},
		{
			local:      []bool{true, false},
			migrations: 10,
			speed:      10,
		},
	}

	for i, test := range tests {
		factor := NewFactorLocation()
		factor.SetConfig(&config.Config{Balance: config.Balance{Location: config.Factor{MigrationsPerSecond: test.migrations}}})
		require.EqualValues(t, test.migrations, factor.migrationsPerSecond)
		backends := make([]scoredBackend, 0, len(test.local))
		for _, local := range test.local {
			backends = append(backends, scoredBackend{
				BackendCtx: &mockBackend{
					local: local,
				},
			})
		}
		factor.UpdateScore(backends)
		_, balanceCount, _ := factor.BalanceCount(backends[1], backends[0])
		require.EqualValues(t, balanceCount, test.speed, "case id: %d", i)
	}
}
