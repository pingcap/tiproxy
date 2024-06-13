// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

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
