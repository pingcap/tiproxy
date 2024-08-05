// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateSalt(t *testing.T) {
	var buffers [100][20]byte
	for i := range buffers {
		err := GenerateSalt(&buffers[i])
		require.NoError(t, err)
		for j := range buffers[i] {
			require.True(t, buffers[i][j] <= 127 && buffers[i][j] > 0 && buffers[i][j] != '$')
		}
	}

	for i := 0; i < len(buffers)-1; i++ {
		for j := i + 1; j < len(buffers); j++ {
			require.NotEqual(t, buffers[i], buffers[j])
		}
	}
}
