// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// test getBackendAddrs
func TestGetBackendAddrs(t *testing.T) {
	tests := []struct {
		backends map[string]*infosync.TiDBTopologyInfo
		hasErr   bool
		expected []string
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
			hasErr: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	fetcher := newMockBackendFetcher(nil, nil)
	br := NewBackendReader(lg, nil, nil, fetcher, nil)
	for i, test := range tests {
		fetcher.infos = test.backends
		if test.hasErr {
			fetcher.err = errors.New("mock err")
		} else {
			fetcher.err = nil
		}
		addrs, err := br.getBackendAddrs(context.Background())
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
	br := NewBackendReader(lg, nil, cli, nil, cfg)
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

// test groupMetricsByRule
func TestOneRuleOneHistory(t *testing.T) {
	tests := []struct {
		names      []string
		step1Value model.SampleValue
		step2Value model.SampleValue
		returnType model.ValueType
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
		},
		{
			names:      []string{"name1", "name2"},
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(2),
			returnType: model.ValMatrix,
		},
		{
			names:      []string{"name1", "name2"},
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(2),
			returnType: model.ValVector,
		},
	}

	mfs := mockMfs()
	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		br := NewBackendReader(lg, nil, nil, nil, nil)
		br.queryRules = map[string]QueryRule{
			"key": {
				Names: test.names,
				Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
					return test.step1Value
				},
				Range2Value: func(pairs []model.SamplePair) model.SampleValue {
					return test.step2Value
				},
				ResultType: test.returnType,
			},
		}

		res := br.groupMetricsByRule(mfs, "backend")
		value := res["key"]
		switch test.returnType {
		case model.ValVector:
			require.Equal(t, test.returnType, value.Type(), "case %d", i)
			require.Len(t, value.(model.Vector), 1, "case %d", i)
			sample := value.(model.Vector)[0]
			require.Equal(t, model.LabelValue("backend"), sample.Metric[LabelNameInstance], "case %d", i)
			require.Equal(t, test.step2Value, sample.Value, "case %d", i)
			require.Greater(t, sample.Timestamp, int64(0), "case %d", i)
		case model.ValMatrix:
			require.Equal(t, test.returnType, value.Type(), "case %d", i)
			require.Len(t, value.(model.Matrix), 1, "case %d", i)
			require.Equal(t, model.LabelValue("backend"), value.(model.Matrix)[0].Metric[LabelNameInstance], "case %d", i)
			require.Len(t, value.(model.Matrix)[0].Values, 1, "case %d", i)
			pair := value.(model.Matrix)[0].Values[0]
			require.Equal(t, test.step2Value, pair.Value, "case %d", i)
			require.Greater(t, pair.Timestamp, int64(0), "case %d", i)
		default:
			require.Nil(t, value, "case %d", i)
		}
	}
}

// test groupMetricsByRule
func TestOneRuleMultiHistory(t *testing.T) {
	tests := []struct {
		step1Value model.SampleValue
		step2Value model.SampleValue
	}{
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(math.NaN()),
		},
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(2),
		},
		{
			step1Value: model.SampleValue(1),
			step2Value: model.SampleValue(math.NaN()),
		},
		{
			step1Value: model.SampleValue(2),
			step2Value: model.SampleValue(3),
		},
		{
			step1Value: model.SampleValue(3),
			step2Value: model.SampleValue(4),
		},
	}

	mfs := mockMfs()
	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil)
	var lastTs model.Time
	var length int
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
				ResultType: model.ValMatrix,
			},
		}

		res := br.groupMetricsByRule(mfs, "backend")
		value := res["key"]
		if math.IsNaN(float64(test.step2Value)) {
			require.Nil(t, value, "case %d", i)
			continue
		}
		length++
		require.Equal(t, model.ValMatrix, value.Type(), "case %d", i)
		matrix := value.(model.Matrix)
		require.Len(t, matrix, 1, "case %d", i)
		require.Equal(t, model.LabelValue("backend"), matrix[0].Metric[LabelNameInstance], "case %d", i)
		require.Len(t, matrix[0].Values, length, "case %d", i)
		pair := matrix[0].Values[len(matrix[0].Values)-1]
		require.Equal(t, test.step2Value, pair.Value, "case %d", i)
		require.GreaterOrEqual(t, pair.Timestamp, lastTs, "case %d", i)
		lastTs = pair.Timestamp
	}
}

// test groupMetricsByRule
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
		ResultType: model.ValMatrix,
	}
	rule2 := QueryRule{
		Names: []string{"name2"},
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			return 2
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			return returnedValues[1]
		},
		ResultType: model.ValVector,
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
	br := NewBackendReader(lg, nil, nil, nil, nil)
	for i, test := range tests {
		if test.hasRule1 {
			br.queryRules["key1"] = rule1
			returnedValues[0] = test.rule1Value
		} else {
			delete(br.queryRules, "key1")
		}
		if test.hasRule2 {
			br.queryRules["key2"] = rule2
			returnedValues[1] = test.rule2Value
		} else {
			delete(br.queryRules, "key2")
		}
		res := br.groupMetricsByRule(mfs, "backend")
		require.Len(t, res, len(br.queryRules), "case %d", i)

		if test.hasRule1 {
			value := res["key1"]
			require.Equal(t, rule1.ResultType, value.Type(), "case %d", i)
			matrix := value.(model.Matrix)
			require.Len(t, matrix, 1, "case %d", i)
			require.Equal(t, model.LabelValue("backend"), matrix[0].Metric[LabelNameInstance], "case %d", i)
			pair := matrix[0].Values[len(matrix[0].Values)-1]
			require.Equal(t, test.rule1Value, pair.Value, "case %d", i)
			require.Greater(t, pair.Timestamp, int64(0), "case %d", i)
		}
		if test.hasRule2 {
			value := res["key2"]
			require.Equal(t, rule2.ResultType, value.Type(), "case %d", i)
			require.Len(t, value.(model.Vector), 1, "case %d", i)
			sample := value.(model.Vector)[0]
			require.Equal(t, model.LabelValue("backend"), sample.Metric[LabelNameInstance], "case %d", i)
			require.Equal(t, test.rule2Value, sample.Value, "case %d", i)
			require.Greater(t, sample.Timestamp, int64(0), "case %d", i)
		}
	}
}

// test mergeQueryResult
func TestMergeQueryResult(t *testing.T) {
	tests := []struct {
		backend       string
		backendValues map[string]model.Value
		queryResult   map[string]model.Value
	}{
		{
			backend: "backend1",
			backendValues: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(1), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}}, Metric: model.Metric{LabelNameInstance: "backend1"}}},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(1), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}}, Metric: model.Metric{LabelNameInstance: "backend1"}}},
			},
		},
		{
			backend: "backend2",
			backendValues: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(3), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend2"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(4), Timestamp: 1}}, Metric: model.Metric{LabelNameInstance: "backend2"}}},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(3), Timestamp: 1, Metric: model.Metric{LabelNameInstance: "backend2"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(4), Timestamp: 1}}, Metric: model.Metric{LabelNameInstance: "backend2"}}},
			},
		},
		{
			backend: "backend1",
			backendValues: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(5), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}, {Value: model.SampleValue(6), Timestamp: 2}}, Metric: model.Metric{LabelNameInstance: "backend1"}}},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(5), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend1"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(2), Timestamp: 1}, {Value: model.SampleValue(6), Timestamp: 2}}, Metric: model.Metric{LabelNameInstance: "backend1"}}},
			},
		},
		{
			backend: "backend3",
			backendValues: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(7), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend3"}}},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(7), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend3"}}},
			},
		},
		{
			backend: "backend3",
			backendValues: map[string]model.Value{
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(8), Timestamp: 2}}, Metric: model.Metric{LabelNameInstance: "backend3"}}},
			},
			queryResult: map[string]model.Value{
				"key1": model.Vector{{Value: model.SampleValue(7), Timestamp: 2, Metric: model.Metric{LabelNameInstance: "backend3"}}},
				"key2": model.Matrix{{Values: []model.SamplePair{{Value: model.SampleValue(8), Timestamp: 2}}, Metric: model.Metric{LabelNameInstance: "backend3"}}},
			},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	br := NewBackendReader(lg, nil, nil, nil, nil)
	for i, test := range tests {
		br.mergeQueryResult(test.backendValues, test.backend)
		for id, expectedValue := range test.queryResult {
			qr := br.GetQueryResult(id)
			require.NotNil(t, qr.Value, "case %d", i)
			require.Equal(t, expectedValue.Type(), qr.Value.Type(), "case %d", i)
			switch expectedValue.Type() {
			case model.ValVector:
				var sample *model.Sample
				for _, v := range qr.Value.(model.Vector) {
					if v.Metric[LabelNameInstance] == model.LabelValue(test.backend) {
						require.Nil(t, sample, "case %d", i)
						sample = v
					}
				}
				require.Equal(t, expectedValue.(model.Vector)[0], sample, "case %d", i)
			case model.ValMatrix:
				var sample *model.SampleStream
				for _, v := range qr.Value.(model.Matrix) {
					if v.Metric[LabelNameInstance] == model.LabelValue(test.backend) {
						require.Nil(t, sample, "case %d", i)
						sample = v
					}
				}
				require.Equal(t, expectedValue.(model.Matrix)[0], sample, "case %d", i)
			}
		}
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
	br := NewBackendReader(lg, nil, nil, nil, nil)
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
	br := NewBackendReader(lg, nil, cli, fetcher, cfg)
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
	err := br.readFromBackends(childCtx)
	require.NoError(t, err)
	wg.Run(func() {
		for childCtx.Err() == nil {
			err := br.readFromBackends(childCtx)
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
					require.NotNil(t, qr.Value)
					if i%2 == 0 {
						require.Equal(t, model.ValVector, qr.Value.Type())
						require.Len(t, qr.Value.(model.Vector), initialBackends)
					} else {
						require.Equal(t, model.ValMatrix, qr.Value.Type())
						require.Len(t, qr.Value.(model.Matrix), initialBackends)
						for j := 0; j < initialBackends; j++ {
							require.NotEmpty(t, qr.Value.(model.Matrix)[j].Values)
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
				_, err := br.GetBackendMetrics()
				require.NoError(t, err)
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
		history    map[string]map[string]backendHistory
		statusCode int32
	}{
		{
			history:    make(map[string]map[string]backendHistory),
			statusCode: http.StatusOK,
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
				},
				"rule_id2": {
					"backend1": {
						Step1History: []model.SamplePair{
							{Timestamp: 123, Value: 500.0},
						},
					},
				},
			},
			statusCode: http.StatusOK,
		},
		{
			history:    make(map[string]map[string]backendHistory),
			statusCode: http.StatusNotFound,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	cli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	br := NewBackendReader(lg, nil, cli, nil, cfg)
	httpHandler := newMockHttpHandler(t)
	port := httpHandler.Start()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	t.Cleanup(httpHandler.Close)

	for i, test := range tests {
		// marshal
		br.history = test.history
		br.marshalledHistory = nil
		data, err := br.GetBackendMetrics()
		require.NoError(t, err, "case %d", i)

		f := func(_ string) string {
			return string(data)
		}
		httpHandler.getRespBody.Store(&f)
		httpHandler.statusCode.Store(test.statusCode)

		// unmarshal
		br.history = make(map[string]map[string]backendHistory)
		err = br.readFromOwner(context.Background(), addr)
		if test.statusCode != http.StatusOK {
			require.Error(t, err, "case %d", i)
			continue
		}
		require.NoError(t, err, "case %d", i)
		require.Equal(t, test.history, br.history, "case %d", i)

		// marshal again
		data2, err := br.GetBackendMetrics()
		require.NoError(t, err, "case %d", i)
		require.Equal(t, data, data2, "case %d", i)
	}
}
