// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFactorConnCount(t *testing.T) {
	factor := NewFactorConnCount()
	tests := []struct {
		connScore     int
		expectedScore uint64
	}{
		{
			connScore:     0,
			expectedScore: 0,
		},
		{
			connScore:     1,
			expectedScore: 1,
		},
		{
			connScore:     9999999,
			expectedScore: 1<<factor.bitNum - 1,
		},
	}
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backends = append(backends, scoredBackend{
			BackendCtx: &mockBackend{
				connScore: test.connScore,
			},
		})
	}
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}
