// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFactorHealth(t *testing.T) {
	factor := NewFactorHealth()
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
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backend := scoredBackend{
			BackendCtx: &mockBackend{
				healthy: test.healthy,
			},
		}
		backends = append(backends, backend)
	}
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}
