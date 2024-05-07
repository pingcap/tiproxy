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

func TestMatchLabel(t *testing.T) {
	tests := []struct {
		jsonRes       string
		addr          string
		ip            string
		port          uint
		expectedPairs []model.SamplePair
	}{
		{
			jsonRes:       `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10080","job":"tidb"},"values":[[1712700000,"100"]]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10081","job":"tidb"},"values":[[1712700000,"200"]]}]`,
			addr:          "10.10.11.1:4000",
			ip:            "10.10.11.1",
			port:          10080,
			expectedPairs: []model.SamplePair{{Timestamp: 1712700000000, Value: 100}},
		},
		{
			jsonRes:       `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10080","job":"tidb"},"values":[[1712700000,"100"]]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10081","job":"tidb"},"values":[[1712700000,"200"]]}]`,
			addr:          "10.10.11.1:4000",
			ip:            "10.10.11.1",
			port:          10082,
			expectedPairs: nil,
		},
		{
			jsonRes:       `[]`,
			addr:          "10.10.11.1:4000",
			ip:            "10.10.11.1",
			port:          10080,
			expectedPairs: nil,
		},
		{
			jsonRes:       `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tidb-peer-0:10080","job":"tidb"},"values":[[1712700000,"100"]]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tidb-peer-1:10081","job":"tidb"},"values":[[1712700000,"200"]]}]`,
			addr:          "tidb-peer-0:4000",
			ip:            "tidb-peer-0",
			port:          10080,
			expectedPairs: []model.SamplePair{{Timestamp: 1712700000000, Value: 100}},
		},
		{
			jsonRes:       `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-0","job":"tidb"},"values":[[1712700000,"100"]]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-1","job":"tidb"},"values":[[1712700000,"200"]]}]`,
			addr:          "tc-tidb-0.tc-tidb-peer.ns.svc:4000",
			ip:            "tc-tidb-0.tc-tidb-peer.ns.svc",
			port:          10080,
			expectedPairs: []model.SamplePair{{Timestamp: 1712700000000, Value: 100}},
		},
		{
			jsonRes:       `[]`,
			addr:          "tc-tidb-0.tc-tidb-peer.ns.svc:4000",
			ip:            "tc-tidb-0.tc-tidb-peer.ns.svc",
			port:          10080,
			expectedPairs: nil,
		},
	}

	for i, test := range tests {
		var m model.Matrix
		require.NoError(t, json.Unmarshal([]byte(test.jsonRes), &m))
		qr := QueryResult{
			Value: m,
		}
		backend := newMockBackend(test.addr, test.ip, test.port)
		pairs := qr.GetMetric4Backend(backend)
		require.Equal(t, test.expectedPairs, pairs, "test index %d", i)
	}
}
