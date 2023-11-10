// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

// Test that the metrics are pushed or not pushed with different configurations.
func TestPushMetrics(t *testing.T) {
	proxyAddr := "0.0.0.0:6000"
	labelName := fmt.Sprintf("%s_%s_maxprocs", ModuleProxy, LabelServer)
	bodyCh1, bodyCh2 := make(chan string), make(chan string)
	pgwOK1, pgwOK2 := setupServer(t, bodyCh1), setupServer(t, bodyCh2)
	log, _ := logger.CreateLoggerForTest(t)

	tests := []struct {
		metricsAddr     string
		metricsInterval uint
		pushedCh        chan string
	}{
		{
			metricsAddr:     pgwOK1.URL,
			metricsInterval: 1,
			pushedCh:        bodyCh1,
		},
		{
			metricsAddr:     pgwOK1.URL,
			metricsInterval: 0,
			pushedCh:        nil,
		},
		{
			metricsAddr:     pgwOK2.URL,
			metricsInterval: 1,
			pushedCh:        bodyCh2,
		},
		{
			metricsAddr:     "",
			metricsInterval: 1,
			pushedCh:        nil,
		},
	}
	mm := NewMetricsManager()
	cfgCh := make(chan *config.Config, 1)
	mm.Init(context.Background(), log, proxyAddr, config.Metrics{}, cfgCh)
	for _, tt := range tests {
		cfgCh <- &config.Config{
			Metrics: config.Metrics{
				MetricsAddr:     tt.metricsAddr,
				MetricsInterval: tt.metricsInterval,
			},
		}
		if tt.pushedCh != nil {
			select {
			case body := <-tt.pushedCh:
				require.Contains(t, body, labelName)
			case <-time.After(2 * time.Second):
				t.Fatal("not pushed")
			}
		} else {
			select {
			case <-bodyCh1:
				t.Fatal("pushed 1")
			case <-bodyCh2:
				t.Fatal("pushed 2")
			case <-time.After(2 * time.Second):
			}
		}
	}
	mm.Close()
}

func setupServer(t *testing.T, bodyCh chan string) *httptest.Server {
	hostname, err := os.Hostname()
	require.NoError(t, err)
	expectedPath := fmt.Sprintf("/metrics/job/tiproxy/instance/%s_6000", hostname)
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			bodyCh <- string(body)
			require.Equal(t, expectedPath, r.URL.EscapedPath())
			w.Header().Set("Content-Type", `text/plain; charset=utf-8`)
			w.WriteHeader(http.StatusOK)
		}),
	)
	t.Cleanup(server.Close)
	return server
}
