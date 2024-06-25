// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/stretchr/testify/require"
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

	fs := NewFactorStatus()
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
	fs := NewFactorStatus()
	for i, test := range tests {
		unhealthyBackend.healthy = test.healthy
		unhealthyBackend.connScore = test.conn
		fs.UpdateScore(backends)
		if test.count == 0 {
			continue
		}
		count := fs.BalanceCount(backends[0], backends[1])
		require.Equal(t, test.count, count, "test idx: %d", i)
	}
}
