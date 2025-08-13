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

func TestMatrixMatchLabel(t *testing.T) {
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
			jsonRes:       `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-0","job":"tidb"},"values":[[1712700000,"100"]]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-1","job":"tidb"},"values":[[1712700000,"200"]]}]`,
			addr:          "tc-tidb-0.tc-tidb-peer.ns.svc.cluster.local:4000",
			ip:            "tc-tidb-0.tc-tidb-peer.ns.svc.cluster.local",
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
		pairs := qr.GetSamplePair4Backend(backend)
		require.Equal(t, test.expectedPairs, pairs, "test index %d", i)
	}
}

func TestVectorMatchLabel(t *testing.T) {
	tests := []struct {
		jsonRes        string
		addr           string
		ip             string
		port           uint
		expectedSample *model.Sample
	}{
		{
			jsonRes:        `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10080","job":"tidb"},"value":[1712700000,"100"]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10081","job":"tidb"},"value":[1712700000,"200"]}]`,
			addr:           "10.10.11.1:4000",
			ip:             "10.10.11.1",
			port:           10080,
			expectedSample: &model.Sample{Timestamp: 1712700000000, Value: 100},
		},
		{
			jsonRes:        `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10080","job":"tidb"},"value":[1712700000,"100"]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"10.10.11.1:10081","job":"tidb"},"value":[1712700000,"200"]}]`,
			addr:           "10.10.11.1:4000",
			ip:             "10.10.11.1",
			port:           10082,
			expectedSample: nil,
		},
		{
			jsonRes:        `[]`,
			addr:           "10.10.11.1:4000",
			ip:             "10.10.11.1",
			port:           10080,
			expectedSample: nil,
		},
		{
			jsonRes:        `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tidb-peer-0:10080","job":"tidb"},"value":[1712700000,"100"]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tidb-peer-1:10081","job":"tidb"},"value":[1712700000,"200"]}]`,
			addr:           "tidb-peer-0:4000",
			ip:             "tidb-peer-0",
			port:           10080,
			expectedSample: &model.Sample{Timestamp: 1712700000000, Value: 100},
		},
		{
			jsonRes:        `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-0","job":"tidb"},"value":[1712700000,"100"]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-1","job":"tidb"},"value":[1712700000,"200"]}]`,
			addr:           "tc-tidb-0.tc-tidb-peer.ns.svc:4000",
			ip:             "tc-tidb-0.tc-tidb-peer.ns.svc",
			port:           10080,
			expectedSample: &model.Sample{Timestamp: 1712700000000, Value: 100},
		},
		{
			jsonRes:        `[{"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-0","job":"tidb"},"value":[1712700000,"100"]}, {"metric":{"__name__":"process_cpu_seconds_total","instance":"tc-tidb-1","job":"tidb"},"value":[1712700000,"200"]}]`,
			addr:           "tc-tidb-0.tc-tidb-peer.ns.svc.cluster.local:4000",
			ip:             "tc-tidb-0.tc-tidb-peer.ns.svc.cluster.local",
			port:           10080,
			expectedSample: &model.Sample{Timestamp: 1712700000000, Value: 100},
		},
		{
			jsonRes:        `[]`,
			addr:           "tc-tidb-0.tc-tidb-peer.ns.svc:4000",
			ip:             "tc-tidb-0.tc-tidb-peer.ns.svc",
			port:           10080,
			expectedSample: nil,
		},
	}

	for i, test := range tests {
		var v model.Vector
		require.NoError(t, json.Unmarshal([]byte(test.jsonRes), &v))
		qr := QueryResult{
			Value: v,
		}
		backend := newMockBackend(test.addr, test.ip, test.port)
		sample := qr.GetSample4Backend(backend)
		if test.expectedSample == nil {
			require.Nil(t, sample, "test index %d", i)
			continue
		}
		require.NotNil(t, sample, "test index %d", i)
		require.Equal(t, test.expectedSample.Timestamp, sample.Timestamp, "test index %d", i)
		require.Equal(t, test.expectedSample.Value, sample.Value, "test index %d", i)
	}
}

func TestAddrMatchLabel(t *testing.T) {
	tests := []struct {
		addr  string
		label string
	}{
		{
			addr:  "10.10.11.1:3080",
			label: "10.10.11.1:3080",
		},
		{
			addr:  "tc-tidb-0.tc-tidb-peer.ns.svc:3080",
			label: "tc-tidb-0",
		},
		{
			addr:  "tc-tidb-0.tc-tidb-peer.ns.svc.cluster.local:3080",
			label: "tc-tidb-0",
		},
		{
			addr:  "12345678-tidb-0.svc.peer:3080",
			label: "12345678-tidb-0.svc.peer:3080",
		},
		{
			addr:  "tc-tidb-0.peer:3080",
			label: "tc-tidb-0.peer:3080",
		},
	}

	for i, test := range tests {
		require.Equal(t, test.label, getLabel4Addr(test.addr), "test index %d", i)
	}
}
