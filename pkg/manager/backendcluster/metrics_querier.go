// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"reflect"
	"sync"

	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/prometheus/common/model"
)

var _ metricsreader.MetricsQuerier = (*MetricsQuerier)(nil)

// MetricsQuerier is a thin fan-out and merge view over cluster-scoped metrics readers.
// It does not own any metrics collection lifecycle by itself.
type MetricsQuerier struct {
	manager *Manager
	mu      sync.RWMutex
	exprs   map[string]metricsreader.QueryExpr
	rules   map[string]metricsreader.QueryRule
}

func NewMetricsQuerier(manager *Manager) *MetricsQuerier {
	return &MetricsQuerier{
		manager: manager,
		exprs:   make(map[string]metricsreader.QueryExpr),
		rules:   make(map[string]metricsreader.QueryRule),
	}
}

func (mq *MetricsQuerier) AddQueryExpr(key string, queryExpr metricsreader.QueryExpr, queryRule metricsreader.QueryRule) {
	mq.mu.Lock()
	mq.exprs[key] = queryExpr
	mq.rules[key] = queryRule
	mq.mu.Unlock()

	for _, cluster := range mq.manager.Snapshot() {
		cluster.metrics.AddQueryExpr(key, queryExpr, queryRule)
	}
}

func (mq *MetricsQuerier) RemoveQueryExpr(key string) {
	mq.mu.Lock()
	delete(mq.exprs, key)
	delete(mq.rules, key)
	mq.mu.Unlock()

	for _, cluster := range mq.manager.Snapshot() {
		cluster.metrics.RemoveQueryExpr(key)
	}
}

func (mq *MetricsQuerier) GetQueryResult(key string) metricsreader.QueryResult {
	results := make([]metricsreader.QueryResult, 0, len(mq.manager.Snapshot()))
	for _, cluster := range mq.manager.Snapshot() {
		result := cluster.metrics.GetQueryResult(key)
		if result.Empty() {
			continue
		}
		results = append(results, result)
	}
	return mergeQueryResults(results)
}

func (mq *MetricsQuerier) GetBackendMetrics() []byte {
	return mq.GetBackendMetricsByCluster("")
}

func (mq *MetricsQuerier) GetBackendMetricsByCluster(clusterName string) []byte {
	if clusterName != "" {
		snapshot := mq.manager.Snapshot()
		cluster := snapshot[clusterName]
		if cluster == nil {
			return nil
		}
		return cluster.metrics.GetBackendMetrics()
	}
	if cluster := mq.manager.PrimaryCluster(); cluster != nil {
		return cluster.metrics.GetBackendMetrics()
	}
	return nil
}

func (mq *MetricsQuerier) snapshot() map[string]struct {
	expr metricsreader.QueryExpr
	rule metricsreader.QueryRule
} {
	mq.mu.RLock()
	snapshot := make(map[string]struct {
		expr metricsreader.QueryExpr
		rule metricsreader.QueryRule
	}, len(mq.exprs))
	for key, expr := range mq.exprs {
		snapshot[key] = struct {
			expr metricsreader.QueryExpr
			rule metricsreader.QueryRule
		}{
			expr: expr,
			rule: mq.rules[key],
		}
	}
	mq.mu.RUnlock()
	return snapshot
}

func mergeQueryResults(results []metricsreader.QueryResult) metricsreader.QueryResult {
	if len(results) == 0 {
		return metricsreader.QueryResult{}
	}
	merged := metricsreader.QueryResult{}
	for _, result := range results {
		if merged.UpdateTime.Before(result.UpdateTime) {
			merged.UpdateTime = result.UpdateTime
		}
		if result.Value == nil || reflect.ValueOf(result.Value).IsNil() {
			continue
		}
		switch value := result.Value.(type) {
		case model.Vector:
			vector, _ := merged.Value.(model.Vector)
			vector = append(vector, value...)
			merged.Value = vector
		case model.Matrix:
			matrix, _ := merged.Value.(model.Matrix)
			matrix = append(matrix, value...)
			merged.Value = matrix
		}
	}
	if merged.Value == nil {
		return metricsreader.QueryResult{}
	}
	return merged
}
