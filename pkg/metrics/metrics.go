// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/util/systimemon"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	LabelBackend = "backend"
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
func (mm *MetricsManager) Init(ctx context.Context, logger *zap.Logger) {
	mm.logger = logger
	registerOnce.Do(registerProxyMetrics)
	ctx, mm.cancel = context.WithCancel(ctx)
	mm.setupMonitor(ctx)
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

// registerProxyMetrics registers metrics.
func registerProxyMetrics() {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	prometheus.MustRegister(collectors.NewGoCollector(collectors.WithGoCollections(collectors.GoRuntimeMetricsCollection | collectors.GoRuntimeMemStatsCollection)))

	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(CreateConnCounter)
	prometheus.MustRegister(DisConnCounter)
	prometheus.MustRegister(MaxProcsGauge)
	prometheus.MustRegister(ServerEventCounter)
	prometheus.MustRegister(ServerErrCounter)
	prometheus.MustRegister(TimeJumpBackCounter)
	prometheus.MustRegister(KeepAliveCounter)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(QueryDurationHistogram)
	prometheus.MustRegister(BackendStatusGauge)
	prometheus.MustRegister(GetBackendHistogram)
	prometheus.MustRegister(GetBackendCounter)
	prometheus.MustRegister(PingBackendGauge)
	prometheus.MustRegister(BackendConnGauge)
	prometheus.MustRegister(HealthCheckCycleGauge)
	prometheus.MustRegister(MigrateCounter)
	prometheus.MustRegister(MigrateDurationHistogram)
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
func ReadGauge(gauge prometheus.Gauge) (float64, error) {
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		return 0, err
	}
	return metric.Gauge.GetValue(), nil
}
