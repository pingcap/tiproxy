// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
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
		msg := fmt.Sprintf("%dth test", i)
		var qr QueryResult
		require.Eventually(t, func() bool {
			<-ch
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
		msg := fmt.Sprintf("%dth test", i)
		qr := mr.GetQueryResult(uint64(i + 1))
		require.NoError(t, qr.Err, msg)
		require.Equal(t, test.expectedString, qr.Value.String(), msg)
	}
	mr.RemoveQueryExpr(1)
	require.Eventually(t, func() bool {
		<-ch
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
		msg := fmt.Sprintf("%dth test", i)
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
				ch := mr.Subscribe(fmt.Sprintf("test%d", i))
				for childCtx.Err() == nil {
					select {
					case <-ch:
					case <-time.After(500 * time.Millisecond):
						t.Fatal("block for over 500ms")
					case <-childCtx.Done():
						return
					}

					for i, test := range tests {
						msg := fmt.Sprintf("%dth test", i)
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
	mpf := newMockPromFetcher(10000)
	lg, _ := logger.CreateLoggerForTest(t)
	mr := NewDefaultMetricsReader(lg, mpf, newHealthCheckConfigForTest())
	mr.Start(context.Background())
	t.Cleanup(mr.Close)
	ch := mr.Subscribe("test")
	id := mr.AddQueryExpr(QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	<-ch
	qr := mr.GetQueryResult(id)
	require.True(t, qr.Empty())
	require.Error(t, qr.Err)
}

func TestNoPromAddr(t *testing.T) {
	mpf := &mockPromFetcher{
		getPromInfo: func(ctx context.Context) (*infosync.PrometheusInfo, error) {
			return nil, nil
		},
	}
	lg, _ := logger.CreateLoggerForTest(t)
	mr := NewDefaultMetricsReader(lg, mpf, newHealthCheckConfigForTest())
	mr.Start(context.Background())
	t.Cleanup(mr.Close)
	ch := mr.Subscribe("test")
	mr.AddQueryExpr(QueryExpr{
		PromQL: "any",
		Range:  time.Minute,
	})
	select {
	case <-ch:
		t.Fatal("should not notify")
	case <-time.After(100 * time.Millisecond):
	}
}

func setupTypicalMetricsReader(t *testing.T) (*mockHttpHandler, MetricsReader) {
	httpHandler := &mockHttpHandler{
		t: t,
	}
	port := httpHandler.Start()
	t.Cleanup(httpHandler.Close)
	mpf := newMockPromFetcher(port)
	lg, _ := logger.CreateLoggerForTest(t)
	mr := NewDefaultMetricsReader(lg, mpf, newHealthCheckConfigForTest())
	mr.Start(context.Background())
	t.Cleanup(mr.Close)
	return httpHandler, mr
}

type mockPromFetcher struct {
	getPromInfo func(ctx context.Context) (*infosync.PrometheusInfo, error)
}

func (mpf *mockPromFetcher) GetPromInfo(ctx context.Context) (*infosync.PrometheusInfo, error) {
	return mpf.getPromInfo(ctx)
}

func newMockPromFetcher(port int) *mockPromFetcher {
	return &mockPromFetcher{
		getPromInfo: func(ctx context.Context) (*infosync.PrometheusInfo, error) {
			return &infosync.PrometheusInfo{
				IP:   "127.0.0.1",
				Port: port,
			}, nil
		},
	}
}

type mockHttpHandler struct {
	getRespBody atomic.Pointer[func(reqBody string) string]
	wg          waitgroup.WaitGroup
	t           *testing.T
	server      *http.Server
}

func (handler *mockHttpHandler) Start() int {
	statusListener, addr := startListener(handler.t, "")
	_, port := parseHostPort(handler.t, addr)
	handler.server = &http.Server{Addr: addr, Handler: handler}
	handler.wg.Run(func() {
		_ = handler.server.Serve(statusListener)
	})
	return int(port)
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	body, err := io.ReadAll(r.Body)
	require.NoError(handler.t, err)
	respBody := (*handler.getRespBody.Load())(string(body))
	require.True(handler.t, len(respBody) > 0, string(body))
	_, err = w.Write([]byte(respBody))
	require.NoError(handler.t, err)
}

func (handler *mockHttpHandler) Close() {
	require.NoError(handler.t, handler.server.Close())
	handler.wg.Wait()
}

func waitResultReady(t *testing.T, mr MetricsReader, ch <-chan struct{}, resultNum int) {
	require.Eventually(t, func() bool {
		<-ch
		for i := 0; i < resultNum; i++ {
			qr := mr.GetQueryResult(uint64(i + 1))
			if qr.Value == nil && qr.Err == nil {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
}

// TODO: move health_check to this package and remove these duplicated functions.
func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:          true,
		MetricsInterval: 10 * time.Millisecond,
		MetricsTimeout:  100 * time.Millisecond,
	}
}

func startListener(t *testing.T, addr string) (net.Listener, string) {
	if len(addr) == 0 {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return listener, listener.Addr().String()
}

func parseHostPort(t *testing.T, addr string) (string, uint) {
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.ParseUint(port, 10, 32)
	require.NoError(t, err)
	return host, uint(p)
}
