// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/stretchr/testify/require"
)

func TestFactorLabelOneBackend(t *testing.T) {
	tests := []struct {
		labelName       string
		selfLabelVal    string
		backendLabelVal string
		expectedScore   uint64
	}{
		{},
		{
			labelName: "k1",
		},
		{
			selfLabelVal: "v1",
		},
		{
			labelName:     "k1",
			selfLabelVal:  "v1",
			expectedScore: 1,
		},
		{
			labelName:       "k1",
			selfLabelVal:    "v1",
			backendLabelVal: "v2",
			expectedScore:   1,
		},
		{
			labelName:       "k1",
			selfLabelVal:    "v1",
			backendLabelVal: "v1",
			expectedScore:   0,
		},
	}

	factor := NewFactorLabel()
	for i, test := range tests {
		backendLabels := make(map[string]string)
		if test.backendLabelVal != "" {
			backendLabels[test.labelName] = test.backendLabelVal
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
		selfLabels := make(map[string]string)
		if test.labelName != "" && test.selfLabelVal != "" {
			selfLabels[test.labelName] = test.selfLabelVal
		}
		factor.SetConfig(&config.Config{
			Labels: selfLabels,
			Balance: config.Balance{
				LabelName: test.labelName,
			},
		})
		factor.UpdateScore(backends)
		for _, backend := range backends {
			require.Equal(t, test.expectedScore, backend.score(), "test idx: %d", i)
		}
	}
}

func TestFactorLabelMultiBackends(t *testing.T) {
	tests := []struct {
		labels        map[string]string
		expectedScore uint64
	}{
		{
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				"k1": "v1",
			},
			expectedScore: 0,
		},
		{
			labels: map[string]string{
				"k2": "v1",
			},
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				"k1": "v2",
				"k2": "v1",
			},
			expectedScore: 1,
		},
		{
			labels: map[string]string{
				"k1": "v1",
				"k2": "v2",
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
	factor := NewFactorLabel()
	factor.SetConfig(&config.Config{
		Labels: map[string]string{"k1": "v1"},
		Balance: config.Balance{
			LabelName: "k1",
		},
	})
	factor.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.expectedScore, backends[i].score(), "test idx: %d", i)
	}
}
