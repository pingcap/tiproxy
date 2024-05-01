// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestParseMatrix(t *testing.T) {
	tests := []struct {
		jsonRes       string
		expectedArray []model.SamplePair
	}{
		{
			jsonRes:       `[{"metric":{"__name__":"go_goroutines","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712742184.054,"229"],[1712742199.054,"229"],[1712742214.054,"229"],[1712742229.054,"229"],[1712742244.054,"229"]]}]`,
			expectedArray: []model.SamplePair{{Timestamp: 1712742184054, Value: 229}, {Timestamp: 1712742199054, Value: 229}, {Timestamp: 1712742214054, Value: 229}, {Timestamp: 1712742229054, Value: 229}, {Timestamp: 1712742244054, Value: 229}},
		},
		{
			jsonRes:       `[]`,
			expectedArray: nil,
		},
	}

	for _, test := range tests {
		var m model.Matrix
		require.NoError(t, json.Unmarshal([]byte(test.jsonRes), &m))
		qr := QueryResult{
			Value: m,
		}
		if len(test.expectedArray) == 0 {
			require.Len(t, qr.Value.(model.Matrix), 0)
			continue
		}
		pairs := qr.Value.(model.Matrix)[0].Values
		require.Equal(t, test.expectedArray, pairs)
	}
}
