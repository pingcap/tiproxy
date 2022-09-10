// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/stretchr/testify/require"
)

// Test that the metrics are pushed or not pushed with different configurations.
func TestPushMetrics(t *testing.T) {
	proxyAddr := "0.0.0.0:6000"
	labelName := fmt.Sprintf("%s_%s_connections", ModuleWeirProxy, LabelServer)
	hostname, err := os.Hostname()
	require.NoError(t, err)
	expectedPath := fmt.Sprintf("/metrics/job/proxy/instance/%s_6000", hostname)
	bodyCh := make(chan string)
	pgwOK := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			bodyCh <- string(body)
			require.Equal(t, expectedPath, r.URL.EscapedPath())
			w.Header().Set("Content-Type", `text/plain; charset=utf-8`)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer pgwOK.Close()
	log := logger.CreateLoggerForTest(t)

	tests := []struct {
		metricsAddr     string
		metricsInterval uint
		pushed          bool
	}{
		{
			metricsAddr:     pgwOK.URL,
			metricsInterval: 1,
			pushed:          true,
		},
		{
			metricsAddr:     "",
			metricsInterval: 1,
			pushed:          false,
		},
		{
			metricsAddr:     pgwOK.URL,
			metricsInterval: 0,
			pushed:          false,
		},
	}
	for _, tt := range tests {
		for len(bodyCh) > 0 {
			<-bodyCh
		}
		mm := NewMetricsManager()
		mm.Init(context.Background(), log, tt.metricsAddr, tt.metricsInterval, proxyAddr)
		if tt.pushed {
			select {
			case body := <-bodyCh:
				require.Contains(t, body, labelName)
			case <-time.After(2 * time.Second):
				t.Fatal("not pushed")
			}
		} else {
			select {
			case <-bodyCh:
				t.Fatal("pushed")
			case <-time.After(2 * time.Second):
			}
		}
		mm.Close()
	}
}
