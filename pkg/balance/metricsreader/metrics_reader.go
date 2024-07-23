// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	sourceNone int32 = iota
	sourceProm
	sourceBackend
)

// PromInfoFetcher is an interface to fetch the Prometheus info from ETCD.
type PromInfoFetcher interface {
	GetPromInfo(ctx context.Context) (*infosync.PrometheusInfo, error)
}

// TopologyFetcher is an interface to fetch the tidb topology from ETCD.
type TopologyFetcher interface {
	GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error)
}

type MetricsReader interface {
	Start(ctx context.Context, etcdCli *clientv3.Client) error
	AddQueryExpr(key string, queryExpr QueryExpr, queryRule QueryRule)
	RemoveQueryExpr(key string)
	GetQueryResult(key string) QueryResult
	Close()
}

var _ MetricsReader = (*DefaultMetricsReader)(nil)

type DefaultMetricsReader struct {
	source        atomic.Int32
	backendReader *BackendReader
	promReader    *PromReader
	wg            waitgroup.WaitGroup
	cancel        context.CancelFunc
	lg            *zap.Logger
	cfg           *config.HealthCheck
}

func NewDefaultMetricsReader(lg *zap.Logger, promFetcher PromInfoFetcher, backendFetcher TopologyFetcher, httpCli *http.Client,
	cfg *config.HealthCheck, cfgGetter config.ConfigGetter) *DefaultMetricsReader {
	return &DefaultMetricsReader{
		lg:            lg,
		cfg:           cfg,
		promReader:    NewPromReader(lg.Named("prom_reader"), promFetcher, cfg),
		backendReader: NewBackendReader(lg.Named("backend_reader"), cfgGetter, httpCli, backendFetcher, cfg),
	}
}

func (dmr *DefaultMetricsReader) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	if err := dmr.backendReader.Start(ctx, etcdCli); err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	dmr.cancel = cancel
	dmr.wg.RunWithRecover(func() {
		ticker := time.NewTicker(dmr.cfg.MetricsInterval)
		defer ticker.Stop()
		for childCtx.Err() == nil {
			dmr.readMetrics(childCtx)
			select {
			case <-ticker.C:
			case <-childCtx.Done():
				return
			}
		}
	}, nil, dmr.lg)
	return nil
}

// readMetrics reads from Prometheus first. If it fails, fall back to read backends.
func (dmr *DefaultMetricsReader) readMetrics(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	promErr := dmr.promReader.ReadMetrics(ctx)
	if promErr == nil {
		dmr.setSource(sourceProm, nil)
		return
	}

	if ctx.Err() != nil {
		return
	}
	backendErr := dmr.backendReader.ReadMetrics(ctx)
	if backendErr == nil {
		dmr.setSource(sourceBackend, promErr)
		return
	}
	dmr.lg.Info("read metrics failed", zap.NamedError("prometheus", promErr), zap.NamedError("backends", backendErr))
}

func (dmr *DefaultMetricsReader) setSource(source int32, err error) {
	old := dmr.source.Load()
	if old != source {
		dmr.source.Store(source)
		switch source {
		case sourceProm:
			dmr.lg.Info("read metrics from Prometheus")
		case sourceBackend:
			dmr.lg.Info("read Prometheus failed, turn to read backends", zap.Error(err))
		}
	}
}

func (dmr *DefaultMetricsReader) AddQueryExpr(key string, queryExpr QueryExpr, queryRule QueryRule) {
	dmr.promReader.AddQueryExpr(key, queryExpr)
	dmr.backendReader.AddQueryRule(key, queryRule)
}

func (dmr *DefaultMetricsReader) RemoveQueryExpr(key string) {
	dmr.promReader.RemoveQueryExpr(key)
	dmr.backendReader.RemoveQueryRule(key)
}

// GetQueryResult returns an empty result if the key or the result is not found.
func (dmr *DefaultMetricsReader) GetQueryResult(key string) QueryResult {
	switch dmr.source.Load() {
	case sourceProm:
		return dmr.promReader.GetQueryResult(key)
	case sourceBackend:
		return dmr.backendReader.GetQueryResult(key)
	default:
		return QueryResult{}
	}
}

func (dmr *DefaultMetricsReader) Close() {
	if dmr.cancel != nil {
		dmr.cancel()
	}
	dmr.wg.Wait()
	dmr.backendReader.Close()
	dmr.promReader.Close()
}
