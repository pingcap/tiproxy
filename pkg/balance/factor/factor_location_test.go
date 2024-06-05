// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/stretchr/testify/require"
)

func TestFactorLocationOneBackend(t *testing.T) {
	tests := []struct {
		selfLocation    string
		backendLocation string
		expectedScore   uint64
	}{
		{},
		{
			selfLocation:  "az1",
			expectedScore: 1,
		},
		{
			backendLocation: "az1",
		},
		{
			selfLocation:    "az1",
			backendLocation: "az2",
			expectedScore:   1,
		},
		{
			selfLocation:    "az1",
			backendLocation: "az1",
		},
	}

	factor := NewFactorLocation()
	for i, test := range tests {
		var backendLabels map[string]string
		if test.backendLocation != "" {
			backendLabels = map[string]string{
				locationLabelName: test.backendLocation,
			}
		}
		backendCtx := &mockBackend{
			BackendInfo: observer.BackendInfo{Labels: backendLabels},
		}
		// Create 2 backends so that UpdateScore won't skip calculating scores.
		backends := []scoredBackend{
			{
				BackendCtx: backendCtx,
			},
			{
				BackendCtx: backendCtx,
			},
		}
		var selfLabels map[string]string
		if test.selfLocation != "" {
			selfLabels = map[string]string{
				locationLabelName: test.selfLocation,
			}
		}
		factor.SetConfig(&config.Config{
			Labels: selfLabels,
		})
		factor.UpdateScore(backends)
		for _, backend := range backends {
			require.Equal(t, test.expectedScore, backend.score(), "test idx: %d", i)
		}
	}
}

func TestFactorLocationMultiBackends(t *testing.T) {
	tests := []struct {
		labels        map[string]string
		expectedScore uint64
	}{
		{
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				locationLabelName: "az1",
			},
			expectedScore: 0,
		},
		{
			labels: map[string]string{
				"z": "az1",
			},
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				locationLabelName: "az2",
				"z":               "az1",
			},
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				locationLabelName: "az1",
				"z":               "az2",
			},
			expectedScore: 0,
		},
	}
	backends := make([]scoredBackend, 0, len(tests))
	for _, test := range tests {
		backend := scoredBackend{
			BackendCtx: &mockBackend{
				BackendInfo: observer.BackendInfo{Labels: test.labels},
			},
		}
		backends = append(backends, backend)
	}
	factor := NewFactorLocation()
	factor.SetConfig(&config.Config{
		Labels: map[string]string{locationLabelName: "az1"},
	})
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}
