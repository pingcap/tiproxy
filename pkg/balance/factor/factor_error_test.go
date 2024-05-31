// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestErrorScore(t *testing.T) {
	tests := []struct {
		errCount []float64
		score    uint64
	}{
		{
			errCount: []float64{},
			score:    2,
		},
	}

	backends := make([]scoredBackend, 0, len(tests))
	values := make([]*model.SampleStream, 0, len(tests))
	for i, test := range tests {
		backends = append(backends, createBackend(i, 0, 0))
		values = append(values, createSampleStream(test.errCount, i))
	}
	mmr := &mockMetricsReader{
		qr: metricsreader.QueryResult{
			UpdateTime: monotime.Now(),
			Value:      model.Matrix(values),
		},
	}
	fm := NewFactorError(mmr)
	fm.UpdateScore(backends)
	for i, test := range tests {
		require.Equal(t, test.score, backends[i].score(), "test index %d", i)
	}
}
