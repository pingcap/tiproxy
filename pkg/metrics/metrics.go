// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"reflect"
	"runtime"
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
	LabelTraffic = "traffic"
	LabelReplay  = "replay"
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

// Init registers metrics and sets up a monitor.
func (mm *MetricsManager) Init(ctx context.Context, logger *zap.Logger) {
	mm.logger = logger
	mm.registerProxyMetrics()
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
	mm.wg.RunWithRecover(func() {
		systimemon.StartMonitor(ctx, mm.logger, time.Now, systimeErrHandler, successCallBack)
	}, nil, mm.logger)
}

// registerProxyMetrics registers metrics.
func (mm *MetricsManager) registerProxyMetrics() {
	prometheus.DefaultRegisterer.Unregister(collectors.NewGoCollector())
	prometheus.MustRegister(collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler)))
	for _, c := range colls {
		prometheus.MustRegister(c)
	}
}

var colls []prometheus.Collector

func init() {
	colls = []prometheus.Collector{
		ConnGauge,
		CreateConnCounter,
		DisConnCounter,
		MaxProcsGauge,
		OwnerGauge,
		ServerEventCounter,
		ServerErrCounter,
		TimeJumpBackCounter,
		KeepAliveCounter,
		QueryTotalCounter,
		QueryDurationHistogram,
		HandshakeDurationHistogram,
		BackendStatusGauge,
		GetBackendHistogram,
		GetBackendCounter,
		PingBackendGauge,
		BackendConnGauge,
		HealthCheckCycleGauge,
		MigrateCounter,
		MigrateDurationHistogram,
		InboundBytesCounter,
		InboundPacketsCounter,
		OutboundBytesCounter,
		OutboundPacketsCounter,
		CrossLocationBytesCounter,
		ReplayPendingCmdsGauge,
		ReplayWaitTime,
	}
}

func DelBackend(addr string) {
	for _, name := range []string{LblBackend, LblFrom, LblTo} {
		labels := prometheus.Labels{
			name: addr,
		}
		delLabelValues(labels)
	}
}

func delLabelValues(labels prometheus.Labels) {
	for _, c := range colls {
		field := reflect.Indirect(reflect.ValueOf(c)).FieldByName("MetricVec")
		if field.IsValid() {
			vec := field.Interface().(*prometheus.MetricVec)
			vec.DeletePartialMatch(labels)
		}
	}
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

func Collect(coll prometheus.Collector) ([]*dto.Metric, error) {
	results := make([]*dto.Metric, 0)
	ch := make(chan prometheus.Metric)
	go func() {
		coll.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		var metric dto.Metric
		if err := m.Write(&metric); err != nil {
			return nil, err
		}
		results = append(results, &metric)
	}
	return results, nil
}
