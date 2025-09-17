// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/stretchr/testify/require"
)

func TestBackendWrapper(t *testing.T) {
	b := &backendWrapper{}
	b.mu.BackendHealth = observer.BackendHealth{
		Healthy:            true,
		ServerVersion:      "1.0",
		SupportRedirection: false,
		Local:              false,
		BackendInfo: observer.BackendInfo{
			Labels: map[string]string{
				"keyspace": "a",
				"zone":     "b",
			},
		},
	}
	require.Equal(t, "a", b.Keyspace())
	require.Equal(t, "1.0", b.ServerVersion())
	require.True(t, b.Healthy())
	require.False(t, b.SupportRedirection())
	require.False(t, b.Local())
	require.Equal(t, "a", b.GetBackendInfo().Labels["keyspace"])
	require.Equal(t, "b", b.GetBackendInfo().Labels["zone"])
}
