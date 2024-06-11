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
		conns    []int
		minCount int
		maxCount int
	}{
		{
			conns:    []int{1, 0},
			minCount: 1,
			maxCount: 1,
		},
		{
			conns:    []int{10, 0},
			minCount: 1,
			maxCount: 5,
		},
		{
			conns:    []int{10, 10},
			minCount: 1,
			maxCount: 5,
		},
		{
			conns:    []int{100, 10},
			minCount: 10,
			maxCount: 40,
		},
		{
			conns:    []int{1000, 100},
			minCount: 100,
			maxCount: 200,
		},
		{
			conns:    []int{100, 1000},
			minCount: 100,
			maxCount: 200,
		},
		{
			conns:    []int{10000, 10000},
			minCount: 1000,
			maxCount: 2000,
		},
	}

	fs := NewFactorStatus()
	for i, test := range tests {
		backends := []scoredBackend{
			{
				BackendCtx: &mockBackend{
					healthy:   false,
					connScore: test.conns[0],
				},
			},
			{
				BackendCtx: &mockBackend{
					healthy:   true,
					connScore: test.conns[1],
				},
			},
		}
		count := fs.BalanceCount(backends[0], backends[1])
		require.GreaterOrEqual(t, count, test.minCount, "test idx: %d", i)
		require.LessOrEqual(t, count, test.maxCount, "test idx: %d", i)
	}
}
