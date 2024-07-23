// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestReadPromMetrics(t *testing.T) {
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

	httpHandler, br := setupTypicalPromReader(t)
	for i, test := range tests {
		key := strconv.Itoa(i)
		br.AddQueryExpr(key, QueryExpr{
			PromQL:   test.promQL,
			Range:    time.Minute,
			HasLabel: test.hasLabel,
		})
		f := func(reqBody string) string {
			return test.respBody
		}
		httpHandler.getRespBody.Store(&f)
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		err := br.ReadMetrics(context.Background())
		require.NoError(t, err, msg)
		qr := br.GetQueryResult(key)
		require.Equal(t, test.expectedType, qr.Value.Type(), msg)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
		br.RemoveQueryExpr(key)
	}
}

func TestMultiExprs(t *testing.T) {
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

	httpHandler, br := setupTypicalPromReader(t)
	f := func(reqBody string) string {
		for _, test := range tests {
			if strings.Contains(reqBody, "query="+test.promQL) {
				return test.respBody
			}
		}
		return ""
	}
	httpHandler.getRespBody.Store(&f)
	for i, test := range tests {
		key := strconv.Itoa(i)
		br.AddQueryExpr(key, QueryExpr{
			PromQL: test.promQL,
			Range:  time.Minute,
		})
	}

	err := br.ReadMetrics(context.Background())
	require.NoError(t, err)
	for i, test := range tests {
		key := strconv.Itoa(i)
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		qr := br.GetQueryResult(key)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
	}

	br.RemoveQueryExpr("0")
	err = br.ReadMetrics(context.Background())
	require.NoError(t, err)
	qr := br.GetQueryResult("0")
	require.True(t, qr.Empty())
}

func TestBackendLabel(t *testing.T) {
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

	httpHandler, br := setupTypicalPromReader(t)
	httpHandler.getRespBody.Store(&f)
	for i, test := range tests {
		key := strconv.Itoa(i)
		br.AddQueryExpr(key, QueryExpr{
			PromQL:   test.promQL + `{%s="tidb"}`,
			Range:    time.Minute,
			HasLabel: true,
		})
	}

	err := br.ReadMetrics(context.Background())
	require.NoError(t, err)
	for i, test := range tests {
		key := strconv.Itoa(i)
		msg := fmt.Sprintf("%dth test %s", i, test.promQL)
		qr := br.GetQueryResult(key)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
	}
}

func TestPromUnavailable(t *testing.T) {
	httpHandler, mr := setupTypicalPromReader(t)
	f := func(reqBody string) string {
		return `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"any"},"values":[[1712738879.406,"0"]]}]}}`
	}
	httpHandler.getRespBody.Store(&f)
	mr.AddQueryExpr("0", QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	err := mr.ReadMetrics(context.Background())
	require.NoError(t, err)
	qr1 := mr.GetQueryResult("0")
	require.False(t, qr1.Empty())

	httpHandler.statusCode.Store(http.StatusInternalServerError)
	err = mr.ReadMetrics(context.Background())
	require.Error(t, err)
}

func TestNoPromAddr(t *testing.T) {
	mpf := &mockPromFetcher{
		getPromInfo: func(ctx context.Context) (*infosync.PrometheusInfo, error) {
			return nil, infosync.ErrNoProm
		},
	}
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	br := NewPromReader(lg, mpf, cfg)
	promInfo, err := br.getPromAPI(context.Background())
	require.Nil(t, promInfo)
	require.ErrorIs(t, err, infosync.ErrNoProm)

	br.AddQueryExpr("0", QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	err = br.ReadMetrics(context.Background())
	require.Error(t, err)
	qr := br.GetQueryResult("0")
	require.True(t, qr.Empty())
}

func setupTypicalPromReader(t *testing.T) (*mockHttpHandler, *PromReader) {
	httpHandler := newMockHttpHandler(t)
	port := httpHandler.Start()
	t.Cleanup(httpHandler.Close)
	mpf := newMockPromFetcher(port)
	lg, _ := logger.CreateLoggerForTest(t)
	pr := NewPromReader(lg, mpf, newHealthCheckConfigForTest())
	return httpHandler, pr
}

func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:          true,
		MetricsInterval: 10 * time.Millisecond,
		MetricsTimeout:  100 * time.Millisecond,
	}
}
