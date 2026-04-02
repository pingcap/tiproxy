// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBackendID(t *testing.T) {
	tests := []struct {
		name            string
		id              string
		expectedCluster string
		expectedAddr    string
	}{
		{
			name:            "round trip",
			id:              backendID("cluster-a", "10.0.0.1:4000"),
			expectedCluster: "cluster-a",
			expectedAddr:    "10.0.0.1:4000",
		},
		{
			name:            "cluster name contains slash",
			id:              backendID("tenant-a/cluster-a", "10.0.0.1:4000"),
			expectedCluster: "tenant-a/cluster-a",
			expectedAddr:    "10.0.0.1:4000",
		},
		{
			name:            "no cluster name",
			id:              "10.0.0.1:4000",
			expectedCluster: "",
			expectedAddr:    "10.0.0.1:4000",
		},
		{
			name:            "empty id",
			id:              "",
			expectedCluster: "",
			expectedAddr:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterName, addr := ParseBackendID(tt.id)
			require.Equal(t, tt.expectedCluster, clusterName)
			require.Equal(t, tt.expectedAddr, addr)
		})
	}
}
