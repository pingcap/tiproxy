// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddScore(t *testing.T) {
	tests := []struct {
		scores        []int
		bitNums       []int
		expectedScore uint64
	}{
		{
			scores:        []int{10, 8},
			bitNums:       []int{4, 10},
			expectedScore: 10<<10 + 8,
		},
		{
			scores:        []int{1, 0, 10},
			bitNums:       []int{1, 1, 10},
			expectedScore: 1<<11 + 10,
		},
	}
	for _, test := range tests {
		backend := &backendWrapper{}
		for i := 0; i < len(test.scores); i++ {
			backend.addScore(test.scores[i], test.bitNums[i])
		}
		require.Equal(t, test.expectedScore, backend.score())
		backend.clearScore()
		require.Equal(t, uint64(0), backend.score())
	}
}
