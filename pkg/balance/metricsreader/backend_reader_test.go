// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// test getBackendAddrs
func TestGetBackendAddrs(t *testing.T) {
	tests := []struct {
		backends     map[string]*infosync.TiDBTopologyInfo
		excludeZones []string
		hasErr       bool
		expected     []string
	}{
		{
			backends: map[string]*infosync.TiDBTopologyInfo{},
			expected: []string{},
		},
		{
			backends: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					IP:         "1.1.1.1",
					StatusPort: 10080,
				},
			},
			expected: []string{"1.1.1.1:10080"},
		},
		{
			backends: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					IP:         "1.1.1.1",
					StatusPort: 10080,
				},
				"2.2.2.2:4000": {
					IP:         "2.2.2.2",
					StatusPort: 10080,
				},
			},
			expected: []string{"1.1.1.1:10080", "2.2.2.2:10080"},
		},
		{
			backends: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					IP:         "1.1.1.1",
					StatusPort: 10080,
					Labels:     map[string]string{config.LocationLabelName: "z1"},
				},
				"2.2.2.2:4000": {
					IP:         "2.2.2.2",
					StatusPort: 10080,
					Labels:     map[string]string{config.LocationLabelName: "z2"},
				},
			},
			excludeZones: []string{"z1"},
			expected:     []string{"2.2.2.2:10080"},
		},
		{
			backends: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					IP:         "1.1.1.1",
					StatusPort: 10080,
					Labels:     map[string]string{config.LocationLabelName: "z1"},
				},
				"2.2.2.2:4000": {
					IP:         "2.2.2.2",
					StatusPort: 10080,
					Labels:     map[string]string{config.LocationLabelName: "z2"},
				},
			},
			excludeZones: []string{"z1", "z2"},
			expected:     []string{},
		},
		{
			backends: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					IP:         "1.1.1.1",
					StatusPort: 10080,
				},
				"2.2.2.2:4000": {
					IP:         "2.2.2.2",
					StatusPort: 10080,
					Labels:     map[string]string{config.LocationLabelName: "z2"},
				},
			},
			excludeZones: []string{"z1", "z2"},
			expected:     []string{"1.1.1.1:10080"},
		},
		{
			hasErr: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	fetcher := newMockBackendFetcher(nil, nil)
	br := NewBackendReader(lg, nil, nil, nil, fetcher, nil)
	for i, test := range tests {
		fetcher.infos = test.backends
		if test.hasErr {
			fetcher.err = errors.New("mock err")
		} else {
			fetcher.err = nil
		}
		addrs, err := br.getBackendAddrs(context.Background(), test.excludeZones)
		if test.hasErr {
			require.Error(t, err, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
			slices.Sort(addrs)
			require.Equal(t, test.expected, addrs, "case %d", i)
		}
	}
}

// test filterMetrics and parseMetrics
func TestFilterAndParse(t *testing.T) {
	tests := []struct {
		names        []string
		input        string
		nameInResult []bool
		hasErr       bool
	}{
		{
			names:        []string{"any"},
			input:        "",
			nameInResult: []bool{false},
		},
		{
			names: []string{"process_cpu_seconds_total", "tidb_server_maxprocs"},
			input: `# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 19.57
# HELP tidb_server_maxprocs The value of GOMAXPROCS.
# TYPE tidb_server_maxprocs gauge
tidb_server_maxprocs 2
`,
			nameInResult: []bool{true, true},
		},
		{
			names: []string{"tidb_tikvclient_backoff_seconds_count"},
			input: `tidb_tikvclient_backoff_seconds_sum{type=""} 0
tidb_tikvclient_backoff_seconds_count{type=""} 0
tidb_tikvclient_backoff_seconds_bucket{type="dataNotReady",le="0.0005"} 0
tidb_tikvclient_backoff_seconds_count{type="dataNotReady"} 0
tidb_tikvclient_backoff_seconds_bucket{type="isWitness",le="0.0005"} 0
tidb_tikvclient_backoff_seconds_count{type="isWitness"} 0
tidb_tikvclient_backoff_seconds_bucket{type="pdRPC",le="0.0005"} 0
tidb_tikvclient_backoff_seconds_count{type="pdRPC"} 0
tidb_tikvclient_backoff_seconds_bucket{type="regionMiss",le="0.0005"} 0
tidb_tikvclient_backoff_seconds_count{type="regionMiss"} 53
tidb_tikvclient_backoff_seconds_count{type="tikvRPC"} 0
tidb_tikvclient_backoff_seconds_bucket{type="tikvRPC",le="0.0005"} 0
`,
			nameInResult: []bool{true, true},
		},
		{
			names: []string{"process_resident_memory_bytes", "tidb_server_memory_quota_bytes"},
			input: `# HELP process_resident_memory_bytes Resident memory size in bytes.
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 4.10677248e+08
`,
			nameInResult: []bool{true, false},
		},
		{
			names:        []string{"any"},
			input:        "any with wrong format",
			hasErr:       true,
			nameInResult: []bool{false},
		},
	}

	for i, test := range tests {
		output := filterMetrics(test.input, test.names)
		mf, err := parseMetrics(output)
		require.Equal(t, test.hasErr, err != nil, "case %d", i)
		if test.hasErr {
			continue
		}
		containedNames := 0
		for j, keyword := range test.names {
			_, keywordInResult := mf[keyword]
			require.Equal(t, test.nameInResult[j], keywordInResult, "case %d", i)
			if keywordInResult {
				containedNames++
			}
		}
		require.Len(t, mf, containedNames, "case %d", i)
	}
}

// test readBackendMetric
func TestReadBackendMetric(t *testing.T) {
	tests := []struct {
		resp   string
		hasErr bool
	}{
		{
			resp: "response",
		},
		{
			hasErr: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	httpHandler := newMockHttpHandler(t)
	port := httpHandler.Start()
	t.Cleanup(httpHandler.Close)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	cli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	br := NewBackendReader(lg, nil, cli, nil, nil, cfg)
	for i, test := range tests {
		statusCode := http.StatusOK
		if test.hasErr {
			statusCode = http.StatusInternalServerError
		}
		httpHandler.statusCode.Store(int32(statusCode))
		f := func(reqBody string) string {
			return test.resp
		}
		httpHandler.getRespBody.Store(&f)
		res, err := br.readBackendMetric(context.Background(), addr)
		if test.hasErr {
			require.NotNil(t, err, "case %d", i)
		} else {
			require.Nil(t, err, "case %d", i)
			require.Equal(t, test.resp, string(res), "case %d", i)
		}
	}
}

// test metric2History
func TestOneRuleOneHistory(t *testing.T) {
	tests := []struct {
		names      []string
		step1Value model.SampleValue
		step2Value model.SampleValue
		history    backendHistory
	}{
		{
			names: []string{"name1", "name2", "name3"},
		},
		{
			names:      []string{"name1", "name2"},
			step1Value: model.SampleValue(math.NaN()),
		},
		{
			names:      []string{"name1", "name2"},
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(math.NaN()),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}},
			},
		},
		{
			names:      []string{"name1", "name2"},
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(2),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}},
				Step2History: []model.SamplePair{{Value: 2}},
			},
		},
	}

	mfs := mockMfs()
	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		br := NewBackendReader(lg, nil, nil, nil, nil, nil)
		br.queryRules = map[string]QueryRule{
			"key": {
				Names: test.names,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return test.step1Value
				},
				Range2Value: func(pairs []model.SamplePair) model.SampleValue {
					return test.step2Value
				},
			},
		}

		br.metric2History(mfs, "backend")
		beHistory := br.history["key"]["backend"]
		require.Equal(t, len(test.history.Step1History), len(beHistory.Step1History), "case %d", i)
		for j := range test.history.Step1History {
			require.Equal(t, test.history.Step1History[j].Value, beHistory.Step1History[j].Value, "case %d", i)
			require.Greater(t, beHistory.Step1History[j].Timestamp, model.Time(0), "case %d", i)
		}
		require.Equal(t, len(test.history.Step2History), len(beHistory.Step2History))
		for j := range test.history.Step2History {
			require.Equal(t, test.history.Step2History[j].Value, beHistory.Step2History[j].Value, "case %d", i)
			require.Greater(t, beHistory.Step2History[j].Timestamp, model.Time(0), "case %d", i)
		}
	}
}

// test metric2History
func TestOneRuleMultiHistory(t *testing.T) {
	tests := []struct {
		step1Value model.SampleValue
		step2Value model.SampleValue
		history    backendHistory
	}{
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(math.NaN()),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}},
			},
		},
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(2),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}, {Value: 1}},
				Step2History: []model.SamplePair{{Value: 2}},
			},
		},
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(math.NaN()),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}, {Value: 1}, {Value: 1}},
				Step2History: []model.SamplePair{{Value: 2}},
			},
		},
		{
			step1Value: model.SampleValue(2),
			step2Value: model.SampleValue(3),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}, {Value: 1}, {Value: 1}, {Value: 2}},
				Step2History: []model.SamplePair{{Value: 2}, {Value: 3}},
			},
		},
		{
			step1Value: model.SampleValue(3),
			step2Value: model.SampleValue(4),
			history: backendHistory{
				Step1History: []model.SamplePair{{Value: 1}, {Value: 1}, {Value: 1}, {Value: 2}, {Value: 3}},
				Step2History: []model.SamplePair{{Value: 2}, {Value: 3}, {Value: 4}},
			},
		},
	}

	mfs := mockMfs()
	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil, nil)
	for i, test := range tests {
		br.queryRules = map[string]QueryRule{
			"key": {
				Names: []string{"name1", "name2"},
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return test.step1Value
				},
				Range2Value: func(pairs []model.SamplePair) model.SampleValue {
					return test.step2Value
				},
			},
		}

		br.metric2History(mfs, "backend")
		beHistory := br.history["key"]["backend"]
		require.Equal(t, len(test.history.Step1History), len(beHistory.Step1History), "case %d", i)
		var lastTs model.Time
		for j := range test.history.Step1History {
			require.Equal(t, test.history.Step1History[j].Value, beHistory.Step1History[j].Value, "case %d", i)
			require.GreaterOrEqual(t, beHistory.Step1History[j].Timestamp, lastTs, "case %d", i)
			lastTs = beHistory.Step1History[j].Timestamp
		}
		lastTs = model.Time(0)
		require.Equal(t, len(test.history.Step2History), len(beHistory.Step2History))
		for j := range test.history.Step2History {
			require.Equal(t, test.history.Step2History[j].Value, beHistory.Step2History[j].Value, "case %d", i)
			require.GreaterOrEqual(t, beHistory.Step2History[j].Timestamp, lastTs, "case %d", i)
			lastTs = beHistory.Step2History[j].Timestamp
		}
	}
}

// test metric2History
func TestMultiRules(t *testing.T) {
	returnedValues := []model.SampleValue{model.SampleValue(1), model.SampleValue(2)}
	rule1 := QueryRule{
		Names: []string{"name1"},
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			return 1
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			return returnedValues[0]
		},
	}
	rule2 := QueryRule{
		Names: []string{"name2"},
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			return 2
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			return returnedValues[1]
		},
	}
	tests := []struct {
		hasRule1   bool
		hasRule2   bool
		rule1Value model.SampleValue
		rule2Value model.SampleValue
	}{
		{
			hasRule1:   true,
			rule1Value: 1,
		},
		{
			hasRule1:   true,
			hasRule2:   true,
			rule1Value: 2,
			rule2Value: 3,
		},
		{
			hasRule2:   true,
			rule2Value: 4,
		},
	}

	mfs := mockMfs()
	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil, nil)
	for i, test := range tests {
		if test.hasRule1 {
			br.AddQueryRule("key1", rule1)
			returnedValues[0] = test.rule1Value
		} else {
			br.RemoveQueryRule("key1")
		}
		if test.hasRule2 {
			br.AddQueryRule("key2", rule2)
			returnedValues[1] = test.rule2Value
		} else {
			br.RemoveQueryRule("key2")
		}
		br.metric2History(mfs, "backend")

		if test.hasRule1 {
			res := br.history["key1"]["backend"]
			require.Equal(t, model.SampleValue(test.rule1Value), res.Step2History[len(res.Step2History)-1].Value, "case %d", i)
			require.Greater(t, res.Step2History[len(res.Step2History)-1].Timestamp, model.Time(0), "case %d", i)
		}
		if test.hasRule2 {
			res := br.history["key2"]["backend"]
			require.Equal(t, model.SampleValue(test.rule2Value), res.Step2History[len(res.Step2History)-1].Value, "case %d", i)
			require.Greater(t, res.Step2History[len(res.Step2History)-1].Timestamp, model.Time(0), "case %d", i)
		}
	}
}

// test history2QueryResult
func TestHistory2QueryResult(t *testing.T) {
	tests := []struct {
		history     map[string]map[string]backendHistory
		queryRules  map[string]QueryRule
		queryResult map[string]model.Value
	}{
		{
			history: map[string]map[string]backendHistory{
				"key1": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}},
					},
				},
				"key2": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: 1}},
					},
				},
			},
			queryRules: map[string]QueryRule{
				"key1": {ResultType: model.ValVector},
				"key2": {ResultType: model.ValVector},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(2), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Vector{{Value: model.SampleValue(3), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend1"}}},
			},
		},
		{
			history: map[string]map[string]backendHistory{
				"key1": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(1), Timestamp: 1}, {Value: model.SampleValue(2), Timestamp: 2}},
					},
				},
				"key2": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: 1}, {Value: model.SampleValue(4), Timestamp: 2}},
					},
					"backend2": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(5), Timestamp: 1}, {Value: model.SampleValue(6), Timestamp: 2}},
					},
				},
			},
			queryRules: map[string]QueryRule{
				"key1": {ResultType: model.ValVector},
				"key2": {ResultType: model.ValVector},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(2), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Vector{
					{Value: model.SampleValue(4), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend1"}},
					{Value: model.SampleValue(6), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend2"}},
				},
			},
		},
		{
			history: map[string]map[string]backendHistory{
				"key1": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(1), Timestamp: 1}},
					},
				},
				"key2": {
					"backend1": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}},
					},
					"backend2": {
						Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: 1}, {Value: model.SampleValue(4), Timestamp: 2}},
					},
				},
			},
			queryRules: map[string]QueryRule{
				"key1": {ResultType: model.ValVector},
				"key2": {ResultType: model.ValMatrix},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(1), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Matrix{
					{Values: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}}, Metric: model.Metric{LabelNameInstance: "backend1"}},
					{Values: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: 1}, {Value: model.SampleValue(4), Timestamp: 2}}, Metric: model.Metric{LabelNameInstance: "backend2"}},
				},
			},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil, nil)
	for i, test := range tests {
		br.history = test.history
		br.queryRules = test.queryRules
		br.history2QueryResult()
		require.Len(t, br.queryResults, len(test.queryResult), "case %d", i)
		for ruleKey, expectedValue := range test.queryResult {
			qr := br.GetQueryResult(ruleKey)
			require.Greater(t, qr.UpdateTime, monotime.Time(0), "case %d", i)
			require.NotEmpty(t, qr.Value, "case %d", i)
			require.Equal(t, expectedValue.Type(), qr.Value.Type(), "case %d", i)
			switch expectedValue.Type() {
			case model.ValVector:
				sort.Slice(qr.Value.(model.Vector), func(x, y int) bool {
					return qr.Value.(model.Vector)[x].Metric[LabelNameInstance] < qr.Value.(model.Vector)[y].Metric[LabelNameInstance]
				})
				require.Equal(t, expectedValue.(model.Vector), qr.Value.(model.Vector), "case %d", i)
			case model.ValMatrix:
				sort.Slice(qr.Value.(model.Matrix), func(x, y int) bool {
					return qr.Value.(model.Matrix)[x].Metric[LabelNameInstance] < qr.Value.(model.Matrix)[y].Metric[LabelNameInstance]
				})
				require.Equal(t, expectedValue.(model.Matrix), qr.Value.(model.Matrix), "case %d", i)
			}
		}
	}
}

func TestMergeHistory(t *testing.T) {
	tests := []struct {
		oldHistory map[string]map[string]backendHistory
		newHistory map[string]map[string]backendHistory
		result     map[string]map[string]backendHistory
	}{
		{
			oldHistory: map[string]map[string]backendHistory{},
			result:     map[string]map[string]backendHistory{},
		},
		{
			oldHistory: map[string]map[string]backendHistory{},
			newHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
		},
		{
			oldHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
		},
		{
			oldHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			newHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend2": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
					"backend2": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
		},
		{
			oldHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
					"backend2": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			newHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend2": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
					"backend2": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
		},
		{
			oldHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			newHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
		},
		{
			oldHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
			newHistory: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 3}, {Value: 4, Timestamp: 4}},
					},
				},
			},
			result: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{{Value: 1, Timestamp: 10}, {Value: 2, Timestamp: 20}},
						Step2History: []model.SamplePair{{Value: 3, Timestamp: 30}, {Value: 4, Timestamp: 40}},
					},
				},
			},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil, nil)
	for i, test := range tests {
		br.history = test.oldHistory
		br.mergeHistory(test.newHistory)
		require.Equal(t, test.result, br.history, "case %d", i)
	}
}

// test purgeHistory
func TestPurgeHistory(t *testing.T) {
	now := model.TimeFromUnixNano(time.Now().UnixNano())
	tests := []struct {
		backendHistory  map[string]backendHistory
		expectedHistory map[string]backendHistory
	}{
		{
			backendHistory: map[string]backendHistory{
				"backend1": {
					Step1History: []model.SamplePair{{Value: model.SampleValue(1), Timestamp: now.Add(-2 * time.Minute)}, {Value: model.SampleValue(2), Timestamp: now.Add(-30 * time.Second)}},
					Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: now.Add(-2 * time.Minute)}, {Value: model.SampleValue(4), Timestamp: now.Add(-30 * time.Second)}},
				},
				"backend2": {
					Step1History: []model.SamplePair{{Value: model.SampleValue(5), Timestamp: now.Add(-2 * time.Minute)}},
					Step2History: []model.SamplePair{{Value: model.SampleValue(7), Timestamp: now.Add(-30 * time.Second)}},
				},
			},
			expectedHistory: map[string]backendHistory{
				"backend1": {
					Step1History: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: now.Add(-30 * time.Second)}},
					Step2History: []model.SamplePair{{Value: model.SampleValue(4), Timestamp: now.Add(-30 * time.Second)}},
				},
				"backend2": {
					Step1History: []model.SamplePair{},
					Step2History: []model.SamplePair{{Value: model.SampleValue(7), Timestamp: now.Add(-30 * time.Second)}},
				},
			},
		},
		{
			backendHistory: map[string]backendHistory{
				"backend1": {
					Step1History: []model.SamplePair{{Value: model.SampleValue(1), Timestamp: now.Add(-2 * time.Minute)}},
					Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: now.Add(-2 * time.Minute)}},
				},
				"backend2": {
					Step1History: []model.SamplePair{{Value: model.SampleValue(1), Timestamp: now.Add(-2 * time.Minute)}},
					Step2History: []model.SamplePair{{Value: model.SampleValue(3), Timestamp: now.Add(-2 * time.Minute)}},
				},
			},
			expectedHistory: map[string]backendHistory{},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil, nil)
	for i, test := range tests {
		br.AddQueryRule(strconv.Itoa(i), QueryRule{
			Retention: time.Minute,
		})
		br.history[strconv.Itoa(i)] = test.backendHistory
	}

	br.purgeHistory()
	for i, test := range tests {
		actualHistory := br.history[strconv.Itoa(i)]
		require.Equal(t, test.expectedHistory, actualHistory, "case %d", i)
	}
}

func TestQueryBackendConcurrently(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	const initialRules, initialBackends = 3, 3
	var buf strings.Builder
	for i := 0; i < initialRules+1; i++ {
		buf.WriteString(fmt.Sprintf("name%d 100\n", i))
	}
	resp := buf.String()

	// create 3 backends
	httpHandlers := make([]*mockHttpHandler, 0)
	infos := make(map[string]*infosync.TiDBTopologyInfo)
	for i := 0; i < 3; i++ {
		httpHandler := newMockHttpHandler(t)
		f := func(reqBody string) string {
			return resp
		}
		httpHandler.getRespBody.Store(&f)
		port := httpHandler.Start()
		infos[strconv.Itoa(i)] = &infosync.TiDBTopologyInfo{
			IP:         "127.0.0.1",
			StatusPort: uint(port),
		}
		httpHandlers = append(httpHandlers, httpHandler)
	}
	t.Cleanup(func() {
		for _, h := range httpHandlers {
			h.Close()
		}
	})

	fetcher := newMockBackendFetcher(infos, nil)
	cli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	br := NewBackendReader(lg, nil, cli, nil, fetcher, cfg)
	// create 3 rules
	addRule := func(id int) {
		rule := QueryRule{
			Names: []string{fmt.Sprintf("name%d", id)},
			Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
				return model.SampleValue(rand.Float64())
			},
			Range2Value: func(pairs []model.SamplePair) model.SampleValue {
				return model.SampleValue(rand.Float64())
			},
		}
		if id%2 == 0 {
			rule.ResultType = model.ValVector
		} else {
			rule.ResultType = model.ValMatrix
		}
		br.AddQueryRule(strconv.Itoa(id), rule)
	}
	removeRule := func(id int) {
		br.RemoveQueryRule(strconv.Itoa(id))
	}
	for i := 0; i < initialRules; i++ {
		addRule(i)
	}

	// start a goroutine to query metrics
	var wg waitgroup.WaitGroup
	childCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	// fill the initial query result to ensure the result is always non-empty
	err := br.readFromBackends(childCtx, nil)
	require.NoError(t, err)
	wg.Run(func() {
		for childCtx.Err() == nil {
			err := br.readFromBackends(childCtx, nil)
			require.NoError(t, err)
			br.purgeHistory()
		}
	})

	// start a goroutine to add and remove rules
	wg.Run(func() {
		var idx int
		for childCtx.Err() == nil {
			select {
			case <-time.After(10 * time.Millisecond):
				if idx%2 == 0 {
					addRule(initialRules)
				} else {
					removeRule(initialRules)
				}
				idx++
			case <-childCtx.Done():
				return
			}
		}
	})

	// start a goroutine to query results
	wg.Run(func() {
		for childCtx.Err() == nil {
			select {
			case <-time.After(1 * time.Millisecond):
				for i := 0; i < initialRules; i++ {
					qr := br.GetQueryResult(strconv.Itoa(i))
					require.Greater(t, qr.UpdateTime, monotime.Time(0))
					require.NotNil(t, qr.Value)
					// read the values to ensure that they are not updated after the query results are returned
					if i%2 == 0 {
						require.Equal(t, model.ValVector, qr.Value.Type())
						require.Len(t, qr.Value.(model.Vector), initialBackends)
						for j := 0; j < initialBackends; j++ {
							require.NotEmpty(t, qr.Value.(model.Vector)[j].Value)
						}
					} else {
						require.Equal(t, model.ValMatrix, qr.Value.Type())
						require.Len(t, qr.Value.(model.Matrix), initialBackends)
						for j := 0; j < initialBackends; j++ {
							require.NotEmpty(t, qr.Value.(model.Matrix)[j].Values)
							for k := 0; k < len(qr.Value.(model.Matrix)[j].Values); k++ {
								require.NotEmpty(t, qr.Value.(model.Matrix)[j].Values[k].Value)
							}
						}
					}
				}
			case <-childCtx.Done():
				return
			}
		}
	})

	// start a goroutine to marshal history
	wg.Run(func() {
		for childCtx.Err() == nil {
			select {
			case <-time.After(10 * time.Millisecond):
				br.GetBackendMetrics()
			case <-childCtx.Done():
				return
			}
		}
	})
	wg.Wait()
	cancel()
}

func TestReadFromOwner(t *testing.T) {
	tests := []struct {
		history  map[string]map[string]backendHistory
		expected map[string]map[string]backendHistory
		backends []string
	}{
		{
			history:  make(map[string]map[string]backendHistory),
			expected: make(map[string]map[string]backendHistory),
			backends: []string{},
		},
		{
			history: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 100.0},
							{Timestamp: 234, Value: 200.0},
						},
						Step2History: []model.SamplePair{
							{Timestamp: 123, Value: 200.0},
							{Timestamp: 234, Value: 300.0},
						},
					},
					"backend2": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 400.0},
						},
					},
					"backend3": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 400.0},
						},
					},
				},
				"rule_id2": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 500.0},
						},
					},
				},
			},
			expected: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 100.0},
							{Timestamp: 234, Value: 200.0},
						},
						Step2History: []model.SamplePair{
							{Timestamp: 123, Value: 200.0},
							{Timestamp: 234, Value: 300.0},
						},
					},
					"backend2": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 400.0},
						},
					},
				},
				"rule_id2": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 500.0},
						},
					},
				},
			},
			backends: []string{"backend1", "backend2"},
		},
		{
			history: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 100.0},
							{Timestamp: 234, Value: 200.0},
						},
					},
				},
			},
			expected: map[string]map[string]backendHistory{
				"rule_id1": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 100.0},
							{Timestamp: 234, Value: 200.0},
						},
					},
				},
			},
			backends: []string{"backend1", "backend2"},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	cli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	ownerBr := NewBackendReader(lg, nil, nil, nil, nil, cfg)
	memberBr := NewBackendReader(lg, nil, cli, nil, nil, cfg)
	httpHandler := newMockHttpHandler(t)
	port := httpHandler.Start()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	t.Cleanup(httpHandler.Close)

	for i, test := range tests {
		// marshal
		ownerBr.history = test.history
		err := ownerBr.marshalHistory(test.backends)
		require.NoError(t, err, "case %d", i)
		data := ownerBr.GetBackendMetrics()

		f := func(_ string) string {
			return string(data)
		}
		httpHandler.getRespBody.Store(&f)

		// unmarshal
		memberBr.history = make(map[string]map[string]backendHistory)
		err = memberBr.readFromOwner(context.Background(), addr)
		require.NoError(t, err, "case %d", i)
		require.Equal(t, test.expected, memberBr.history, "case %d", i)

		// marshal again
		data2 := ownerBr.GetBackendMetrics()
		require.Equal(t, data, data2, "case %d", i)
	}
}

// test queryAllOwners
func TestQueryAllOwners(t *testing.T) {
	tests := []struct {
		keys   []string
		values []string
		owners []string
		zones  []string
	}{
		{},
		{
			keys:   []string{"/owner/1111", "/owner/2222"},
			values: []string{"backend1", "backend2"},
			owners: []string{"backend1"},
		},
		{
			keys:   []string{"/east/owner/1111", "/east/owner/2222"},
			values: []string{"backend1", "backend2"},
			owners: []string{"backend1"},
			zones:  []string{"east"},
		},
		{
			keys:   []string{"/east/owner/1111", "/west/owner/2222"},
			values: []string{"backend1", "backend2"},
			owners: []string{"backend1", "backend2"},
			zones:  []string{"east", "west"},
		},
		{
			keys:   []string{"/east/owner/1111", "/owner/2222"},
			values: []string{"backend1", "backend2"},
			owners: []string{"backend1", "backend2"},
			zones:  []string{"east"},
		},
		{
			keys:   []string{"/east/owner/1111", "/owner/2222"},
			values: []string{"backend1", "backend1"},
			owners: []string{"backend1"},
			zones:  []string{"east"},
		},
		{
			keys:   []string{"", "//owner/2222", "/east/3333"},
			values: []string{"backend1", "backend2", "backend3"},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	suite := newEtcdTestSuite(t)
	defer suite.close()
	br := NewBackendReader(lg, nil, nil, suite.client, nil, nil)
	for i, test := range tests {
		for i, key := range test.keys {
			key = fmt.Sprintf("%s%s", readerOwnerKeyPrefix, key)
			suite.putKV(key, test.values[i])
		}
		zones, owners, err := br.queryAllOwners(context.Background())
		require.NoError(t, err, "case %d", i)
		if test.owners == nil {
			require.Empty(t, owners, "case %d", i)
		} else {
			slices.Sort(owners)
			require.Equal(t, test.owners, owners, "case %d", i)
		}
		if test.zones == nil {
			require.Empty(t, zones, "case %d", i)
		} else {
			slices.Sort(zones)
			require.Equal(t, test.zones, zones, "case %d", i)
		}
		suite.delKV(readerOwnerKeyPrefix)
	}
}

func TestUpdateLabel(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	suite := newEtcdTestSuite(t)
	defer suite.close()
	cfg := config.NewConfig()
	cfgGetter := newMockConfigGetter(cfg)
	healthCfg := newHealthCheckConfigForTest()
	fetcher := newMockBackendFetcher(map[string]*infosync.TiDBTopologyInfo{}, nil)
	br := NewBackendReader(lg, cfgGetter, nil, suite.client, fetcher, healthCfg)
	err := br.Start(context.Background())
	require.NoError(t, err)
	defer br.Close()

	checkKeyPrefix := func(prefix string) bool {
		kvs := suite.getKV(readerOwnerKeyPrefix)
		if len(kvs) != 1 {
			return false
		}
		return strings.HasPrefix(string(kvs[0].Key), prefix)
	}

	// campaign for the global owner
	prefix := fmt.Sprintf("%s/%s", readerOwnerKeyPrefix, readerOwnerKeySuffix)
	require.Eventually(t, func() bool {
		return checkKeyPrefix(prefix)
	}, 3*time.Second, 10*time.Millisecond)

	// retire the global owner and campaign for the zonal owner
	cfg.Labels = map[string]string{config.LocationLabelName: "east"}
	err = br.ReadMetrics(context.Background())
	require.NoError(t, err)
	prefix = fmt.Sprintf("%s/east/%s", readerOwnerKeyPrefix, readerOwnerKeySuffix)
	require.Eventually(t, func() bool {
		return checkKeyPrefix(prefix)
	}, 3*time.Second, 10*time.Millisecond)
}

func TestElection(t *testing.T) {
	// setup backend
	backendPort, infos := setupTypicalBackendListener(t, "cpu 80.0\n")

	// setup history
	now := model.Time(time.Now().UnixMilli())
	backendKey := net.JoinHostPort("127.0.0.1", strconv.Itoa(backendPort))
	history := map[string]map[string]backendHistory{
		"rule_id1": {
			backendKey: {
				Step1History: []model.SamplePair{
					{Timestamp: now, Value: 100.0},
				},
				Step2History: []model.SamplePair{
					{Timestamp: now, Value: 100.0},
				},
			},
		},
	}
	hitoryText, err := json.Marshal(history)
	require.NoError(t, err)

	// setup rule
	rule := QueryRule{
		Names:     []string{"cpu"},
		Retention: time.Minute,
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			return model.SampleValue(*mfs["cpu"].Metric[0].Untyped.Value)
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			return pairs[len(pairs)-1].Value
		},
		ResultType: model.ValVector,
	}

	// setup owner listener
	ownerHttpHandler := newMockHttpHandler(t)
	ownerPort := ownerHttpHandler.Start()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(ownerPort))
	ownerFunc := func(_ string) string {
		return string(hitoryText)
	}
	ownerHttpHandler.getRespBody.Store(&ownerFunc)
	t.Cleanup(ownerHttpHandler.Close)

	// setup etcd
	suite := newEtcdTestSuite(t)
	t.Cleanup(suite.close)
	ownerKey := fmt.Sprintf("%s/%s", readerOwnerKeyPrefix, readerOwnerKeySuffix)
	suite.putKV(ownerKey, addr)
	require.Eventually(t, func() bool {
		kvs := suite.getKV(ownerKey)
		return len(kvs) == 1 && string(kvs[0].Value) == addr
	}, 3*time.Second, 10*time.Millisecond)

	// setup backend reader
	lg, _ := logger.CreateLoggerForTest(t)
	healthCfg := newHealthCheckConfigForTest()
	cfg := config.NewConfig()
	cfgGetter := newMockConfigGetter(cfg)
	fetcher := newMockBackendFetcher(infos, nil)
	httpCli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	br := NewBackendReader(lg, cfgGetter, httpCli, suite.client, fetcher, healthCfg)
	err = br.Start(context.Background())
	require.NoError(t, err)
	br.AddQueryRule("rule_id1", rule)
	t.Cleanup(br.Close)

	// test not owner
	ts := monotime.Now()
	err = br.ReadMetrics(context.Background())
	require.NoError(t, err)
	qr := br.GetQueryResult("rule_id1")
	require.False(t, qr.Empty())
	require.Equal(t, model.SampleValue(100.0), qr.Value.(model.Vector)[0].Value)
	require.GreaterOrEqual(t, qr.UpdateTime, ts)
	ts = qr.UpdateTime

	// test owner
	suite.delKV(ownerKey)
	require.Eventually(t, func() bool {
		kvs := suite.getKV(ownerKey)
		return len(kvs) == 1 && strings.HasSuffix(string(kvs[0].Value), "3080")
	}, 3*time.Second, 10*time.Millisecond)
	err = br.ReadMetrics(context.Background())
	require.NoError(t, err)
	qr = br.GetQueryResult("rule_id1")
	require.False(t, qr.Empty())
	require.Equal(t, model.SampleValue(80.0), qr.Value.(model.Vector)[0].Value)
	require.GreaterOrEqual(t, qr.UpdateTime, ts)
}

func setupTypicalBackendListener(t *testing.T, respBody string) (backendPort int, infos map[string]*infosync.TiDBTopologyInfo) {
	// setup backend listener
	backendHttpHandler := newMockHttpHandler(t)
	backendFunc := func(reqBody string) string {
		return respBody
	}
	backendHttpHandler.getRespBody.Store(&backendFunc)
	backendPort = backendHttpHandler.Start()
	infos = map[string]*infosync.TiDBTopologyInfo{
		"127.0.0.1:4000": {
			IP:         "127.0.0.1",
			StatusPort: uint(backendPort),
		},
	}
	t.Cleanup(backendHttpHandler.Close)
	return
}
