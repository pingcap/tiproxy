// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestReadMetrics(t *testing.T) {
	httpHandler, mr := setupTypicalMetricsReader(t)
	ch := mr.Subscribe("test")

	tests := []struct {
		promQL         string
		hasLabel       bool
		respBody       string
		expectedString string
		expectedType   model.ValueType
	}{
		{
			promQL:         `tidb_server_connections`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"tidb_server_connections","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712738879.406,"0"],[1712738894.406,"0"],[1712738909.406,"0"],[1712738924.406,"0"],[1712738939.406,"0"]]}]}}`,
			expectedString: "tidb_server_connections{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n0 @[1712738879.406]\n0 @[1712738894.406]\n0 @[1712738909.406]\n0 @[1712738924.406]\n0 @[1712738939.406]",
			expectedType:   model.ValMatrix,
		},
		{
			promQL:         `go_goroutines{%s="tidb"}`,
			hasLabel:       true,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"go_goroutines","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712742184.054,"229"],[1712742199.054,"229"],[1712742214.054,"229"],[1712742229.054,"229"],[1712742244.054,"229"]]}]}}`,
			expectedString: "go_goroutines{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n229 @[1712742184.054]\n229 @[1712742199.054]\n229 @[1712742214.054]\n229 @[1712742229.054]\n229 @[1712742244.054]",
			expectedType:   model.ValMatrix,
		},
		{
			promQL:         `unknown`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			expectedString: ``,
			expectedType:   model.ValMatrix,
		},
	}

	for i, test := range tests {
		id := mr.AddQueryExpr(QueryExpr{
			PromQL:   test.promQL,
			Range:    time.Minute,
			HasLabel: test.hasLabel,
		})
		f := func(reqBody string) string {
			return test.respBody
		}
		httpHandler.getRespBody.Store(&f)
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		var qr QueryResult
		require.Eventually(t, func() bool {
			select {
			case <-ch:
			case <-time.After(3 * time.Second):
				t.Fatal("timeout")
			}
			qr = mr.GetQueryResult(id)
			return qr.Value != nil || qr.Err != nil
		}, 3*time.Second, time.Millisecond, msg)
		require.NoError(t, qr.Err, msg)
		require.Equal(t, test.expectedType, qr.Value.Type(), msg)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
		mr.RemoveQueryExpr(id)
	}
}

func TestMultiExprs(t *testing.T) {
	httpHandler, mr := setupTypicalMetricsReader(t)
	ch := mr.Subscribe("test")

	tests := []struct {
		promQL         string
		respBody       string
		expectedString string
	}{
		{
			promQL:         `tidb_server_connections`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"tidb_server_connections","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712738879.406,"0"],[1712738894.406,"0"],[1712738909.406,"0"],[1712738924.406,"0"],[1712738939.406,"0"]]}]}}`,
			expectedString: "tidb_server_connections{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n0 @[1712738879.406]\n0 @[1712738894.406]\n0 @[1712738909.406]\n0 @[1712738924.406]\n0 @[1712738939.406]",
		},
		{
			promQL:         `go_goroutines`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"go_goroutines","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712742184.054,"229"],[1712742199.054,"229"],[1712742214.054,"229"],[1712742229.054,"229"],[1712742244.054,"229"]]}]}}`,
			expectedString: "go_goroutines{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n229 @[1712742184.054]\n229 @[1712742199.054]\n229 @[1712742214.054]\n229 @[1712742229.054]\n229 @[1712742244.054]",
		},
		{
			promQL:         `unknown`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			expectedString: ``,
		},
	}

	f := func(reqBody string) string {
		for _, test := range tests {
			if strings.Contains(reqBody, "query="+test.promQL) {
				return test.respBody
			}
		}
		return ""
	}
	httpHandler.getRespBody.Store(&f)
	for _, test := range tests {
		mr.AddQueryExpr(QueryExpr{
			PromQL: test.promQL,
			Range:  time.Minute,
		})
	}

	waitResultReady(t, mr, ch, len(tests))
	for i, test := range tests {
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		qr := mr.GetQueryResult(uint64(i + 1))
		require.NoError(t, qr.Err, msg)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
	}
	mr.RemoveQueryExpr(1)
	require.Eventually(t, func() bool {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
		qr := mr.GetQueryResult(uint64(1))
		return qr.Value == nil && qr.Err == nil
	}, 3*time.Second, time.Millisecond)
}

func TestBackendLabel(t *testing.T) {
	httpHandler, mr := setupTypicalMetricsReader(t)
	ch := mr.Subscribe("test")

	tests := []struct {
		promQL         string
		respBody       string
		expectedString string
		label          string
	}{
		{
			promQL:         `tidb_server_connections`,
			label:          `job`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"tidb_server_connections","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712738879.406,"0"],[1712738894.406,"0"],[1712738909.406,"0"],[1712738924.406,"0"],[1712738939.406,"0"]]}]}}`,
			expectedString: "tidb_server_connections{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n0 @[1712738879.406]\n0 @[1712738894.406]\n0 @[1712738909.406]\n0 @[1712738924.406]\n0 @[1712738939.406]",
		},
		{
			promQL:         `go_goroutines`,
			label:          `component`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"go_goroutines","instance":"127.0.0.1:10080","component":"tidb"},"values":[[1712742184.054,"229"],[1712742199.054,"229"],[1712742214.054,"229"],[1712742229.054,"229"],[1712742244.054,"229"]]}]}}`,
			expectedString: "go_goroutines{component=\"tidb\", instance=\"127.0.0.1:10080\"} =>\n229 @[1712742184.054]\n229 @[1712742199.054]\n229 @[1712742214.054]\n229 @[1712742229.054]\n229 @[1712742244.054]",
		},
	}

	f := func(reqBody string) string {
		for _, test := range tests {
			if strings.Contains(reqBody, test.promQL) {
				if strings.Contains(reqBody, test.label) {
					return test.respBody
				} else {
					return `{"status":"success","data":{"resultType":"matrix","result":[]}}`
				}
			}
		}
		return ""
	}
	httpHandler.getRespBody.Store(&f)
	for _, test := range tests {
		mr.AddQueryExpr(QueryExpr{
			PromQL:   test.promQL + `{%s="tidb"}`,
			Range:    time.Minute,
			HasLabel: true,
		})
	}

	waitResultReady(t, mr, ch, len(tests))
	for i, test := range tests {
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		qr := mr.GetQueryResult(uint64(i + 1))
		require.NoError(t, qr.Err, msg)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
	}
}

func TestMultiSubscribers(t *testing.T) {
	httpHandler, mr := setupTypicalMetricsReader(t)
	tests := []struct {
		promQL         string
		hasLabel       bool
		respBody       string
		expectedString string
	}{
		{
			promQL:         `tidb_server_connections`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"tidb_server_connections","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712738879.406,"0"],[1712738894.406,"0"],[1712738909.406,"0"],[1712738924.406,"0"],[1712738939.406,"0"]]}]}}`,
			expectedString: "tidb_server_connections{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n0 @[1712738879.406]\n0 @[1712738894.406]\n0 @[1712738909.406]\n0 @[1712738924.406]\n0 @[1712738939.406]",
		},
		{
			promQL:         `go_goroutines`,
			hasLabel:       true,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"go_goroutines","instance":"127.0.0.1:10080","job":"tidb"},"values":[[1712742184.054,"229"],[1712742199.054,"229"],[1712742214.054,"229"],[1712742229.054,"229"],[1712742244.054,"229"]]}]}}`,
			expectedString: "go_goroutines{instance=\"127.0.0.1:10080\", job=\"tidb\"} =>\n229 @[1712742184.054]\n229 @[1712742199.054]\n229 @[1712742214.054]\n229 @[1712742229.054]\n229 @[1712742244.054]",
		},
		{
			promQL:         `unknown`,
			respBody:       `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			expectedString: ``,
		},
	}

	f := func(reqBody string) string {
		for _, test := range tests {
			if strings.Contains(reqBody, test.promQL) {
				return test.respBody
			}
		}
		return ""
	}
	httpHandler.getRespBody.Store(&f)

	for _, test := range tests {
		promQL := test.promQL
		if test.hasLabel {
			promQL += `{%s="tidb"}`
		}
		mr.AddQueryExpr(QueryExpr{
			PromQL:   promQL,
			Range:    time.Minute,
			HasLabel: test.hasLabel,
		})
	}

	// This channel never reads again after Eventually to test block.
	ch := mr.Subscribe("test")
	waitResultReady(t, mr, ch, len(tests))

	var wg waitgroup.WaitGroup
	childCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	for i := 0; i < 10; i++ {
		func(i int) {
			wg.Run(func() {
				id := fmt.Sprintf("test%d", i)
				ch := mr.Subscribe(id)
				defer mr.Unsubscribe(id)
				for childCtx.Err() == nil {
					select {
					case <-ch:
					case <-time.After(500 * time.Millisecond):
						t.Fatal("block for over 500ms")
					case <-childCtx.Done():
						break
					}
					for i, test := range tests {
						msg := fmt.Sprintf("%dth test %s", i, test.promQL)
						qr := mr.GetQueryResult(uint64(i + 1))
						require.NoError(t, qr.Err, msg)
						require.Equal(t, test.expectedString, qr.Value.String(), msg)
					}
				}
			})
		}(i)
	}
	wg.Wait()
	cancel()
}

func TestPromUnavailable(t *testing.T) {
	httpHandler, mr := setupTypicalMetricsReader(t)
	f := func(reqBody string) string {
		return `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"any"},"values":[[1712738879.406,"0"]]}]}}`
	}
	httpHandler.getRespBody.Store(&f)
	id := mr.AddQueryExpr(QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	require.Eventually(t, func() bool {
		qr := mr.GetQueryResult(id)
		return !qr.Empty()
	}, 3*time.Second, 10*time.Millisecond)

	httpHandler.statusCode.Store(http.StatusInternalServerError)
	require.Eventually(t, func() bool {
		// The qr doesn't update anymore.
		qr := mr.GetQueryResult(id)
		time.Sleep(100 * time.Millisecond)
		return qr.UpdateTime == mr.GetQueryResult(id).UpdateTime
	}, 3*time.Second, time.Millisecond)
}

func TestNoPromAddr(t *testing.T) {
	mpf := &mockPromFetcher{
		getPromInfo: func(ctx context.Context) (*infosync.PrometheusInfo, error) {
			return nil, nil
		},
	}
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	mr := NewDefaultMetricsReader(lg, mpf, cfg)
	promInfo, err := mr.getPromAPI(context.Background())
	require.Nil(t, promInfo)
	require.NotNil(t, err)
	require.Equal(t, true, strings.Contains(err.Error(), "no prometheus info found"))

	mr.Start(context.Background())
	t.Cleanup(mr.Close)
	ch := mr.Subscribe("test")
	id := mr.AddQueryExpr(QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	select {
	case <-ch:
		t.Fatal("should not notify")
	case <-time.After(10 * cfg.MetricsInterval):
		qr := mr.GetQueryResult(id)
		require.True(t, qr.Empty())
	}
}

func setupTypicalMetricsReader(t *testing.T) (*mockHttpHandler, MetricsReader) {
	httpHandler := newMockHttpHandler(t)
	port := httpHandler.Start()
	t.Cleanup(httpHandler.Close)
	mpf := newMockPromFetcher(port)
	lg, _ := logger.CreateLoggerForTest(t)
	mr := NewDefaultMetricsReader(lg, mpf, newHealthCheckConfigForTest())
	mr.Start(context.Background())
	t.Cleanup(mr.Close)
	return httpHandler, mr
}

func waitResultReady(t *testing.T, mr MetricsReader, ch <-chan struct{}, resultNum int) {
	require.Eventually(t, func() bool {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
		for i := 0; i < resultNum; i++ {
			qr := mr.GetQueryResult(uint64(i + 1))
			if qr.Value == nil && qr.Err == nil {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
}

func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:          true,
		MetricsInterval: 10 * time.Millisecond,
		MetricsTimeout:  100 * time.Millisecond,
	}
}
