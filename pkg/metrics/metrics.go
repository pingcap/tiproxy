// Copyright 2020 Ipalfish, Inc.
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
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/TiProxy/lib/util/systimemon"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
)

const (
	ModuleProxy = "tiproxy"
)

// metrics labels.
const (
	LabelServer  = "server"
	LabelBalance = "balance"
	LabelSession = "session"
	LabelMonitor = "monitor"
)

// MetricsManager manages metrics.
type MetricsManager struct {
	wg     waitgroup.WaitGroup
	cancel context.CancelFunc
	logger *zap.Logger
}

// NewMetricsManager creates a MetricsManager.
func NewMetricsManager() *MetricsManager {
	return &MetricsManager{}
}

var registerOnce = &sync.Once{}

// Init registers metrics and pushes metrics to prometheus.
func (mm *MetricsManager) Init(ctx context.Context, logger *zap.Logger, metricsAddr string, metricsInterval uint, proxyAddr string) {
	mm.logger = logger
	registerOnce.Do(registerProxyMetrics)
	ctx, mm.cancel = context.WithCancel(ctx)
	mm.setupMonitor(ctx)
	mm.pushMetric(ctx, metricsAddr, time.Duration(metricsInterval)*time.Second, proxyAddr)
}

// Close stops all goroutines.
func (mm *MetricsManager) Close() {
	if mm.cancel != nil {
		mm.cancel()
	}
	mm.wg.Wait()
}

func (mm *MetricsManager) setupMonitor(ctx context.Context) {
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		TimeJumpBackCounter.Inc()
	}
	callBackCount := 0
	successCallBack := func() {
		callBackCount++
		// It is callback by monitor per second, we increase metrics.KeepAliveCounter per 5s.
		if callBackCount >= 5 {
			callBackCount = 0
			KeepAliveCounter.Inc()
		}
	}
	mm.wg.Run(func() {
		systimemon.StartMonitor(ctx, mm.logger, time.Now, systimeErrHandler, successCallBack)
	})
}

// pushMetric pushes metrics in background.
func (mm *MetricsManager) pushMetric(ctx context.Context, addr string, interval time.Duration, proxyAddr string) {
	if interval == time.Duration(0) || len(addr) == 0 {
		mm.logger.Info("disable Prometheus push client")
		return
	}
	mm.logger.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	mm.wg.Run(func() {
		prometheusPushClient(ctx, mm.logger, addr, interval, proxyAddr)
	})
}

// registerProxyMetrics registers metrics.
func registerProxyMetrics() {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	prometheus.MustRegister(collectors.NewGoCollector(collectors.WithGoCollections(collectors.GoRuntimeMetricsCollection | collectors.GoRuntimeMemStatsCollection)))

	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(TimeJumpBackCounter)
	prometheus.MustRegister(KeepAliveCounter)
	prometheus.MustRegister(BackendStatusGauge)
	prometheus.MustRegister(BackendConnGauge)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(QueryDurationHistogram)
	prometheus.MustRegister(GetBackendHistogram)
	prometheus.MustRegister(MigrateCounter)
	prometheus.MustRegister(MigrateDurationHistogram)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(ctx context.Context, logger *zap.Logger, addr string, interval time.Duration, proxyAddr string) {
	job := "tiproxy"
	pusher := push.New(addr, job)
	pusher = pusher.Gatherer(prometheus.DefaultGatherer)
	pusher = pusher.Grouping("instance", instanceName(proxyAddr))
	for ctx.Err() == nil {
		err := pusher.Push()
		if err != nil {
			logger.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}
	}
}

func instanceName(proxyAddr string) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	_, port, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%s", hostname, port)
}

// ReadCounter reads the value from the counter. It is only used for testing.
func ReadCounter(counter prometheus.Counter) (int, error) {
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return 0, err
	}
	return int(metric.Counter.GetValue()), nil
}

// ReadGauge reads the value from the gauge. It is only used for testing.
func ReadGauge(gauge prometheus.Gauge) (int, error) {
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		return 0, err
	}
	return int(metric.Gauge.GetValue()), nil
}
