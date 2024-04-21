// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

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
		{
			scores:        []int{100, 100},
			bitNums:       []int{3, 5},
			expectedScore: (1<<3-1)<<5 + 1<<5 - 1,
		},
	}
	for idx, test := range tests {
		backend := newScoredBackend(nil)
		for i := 0; i < len(test.scores); i++ {
			backend.addScore(test.scores[i], test.bitNums[i])
		}
		require.Equal(t, test.expectedScore, backend.score(), "test idx: %d", idx)
	}
}
