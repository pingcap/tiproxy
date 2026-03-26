// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMetricsQuerierQueryRegistry(t *testing.T) {
	mgr := NewManager(zap.NewNop(), nilClusterTLS)
	mq := NewMetricsQuerier(mgr)

	expr := metricsreader.QueryExpr{PromQL: "avg(up)"}
	rule := metricsreader.QueryRule{
		Names:      []string{"up"},
		ResultType: model.ValVector,
	}

	mq.AddQueryExpr("up", expr, rule)

	snapshot := mq.snapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, expr, snapshot["up"].expr)
	require.Equal(t, rule.Names, snapshot["up"].rule.Names)
	require.Equal(t, rule.ResultType, snapshot["up"].rule.ResultType)

	mq.RemoveQueryExpr("up")
	require.Empty(t, mq.snapshot())
}

func TestMergeQueryResults(t *testing.T) {
	ts1 := time.Unix(10, 0)
	ts2 := time.Unix(20, 0)
	vector1 := model.Vector{
		&model.Sample{
			Metric:    model.Metric{model.LabelName("instance"): "tidb-1"},
			Value:     model.SampleValue(1),
			Timestamp: model.Time(1000),
		},
	}
	vector2 := model.Vector{
		&model.Sample{
			Metric:    model.Metric{model.LabelName("instance"): "tidb-2"},
			Value:     model.SampleValue(2),
			Timestamp: model.Time(2000),
		},
	}

	merged := mergeQueryResults([]metricsreader.QueryResult{
		{UpdateTime: ts1, Value: vector1},
		{UpdateTime: ts2, Value: vector2},
	})
	require.Equal(t, ts2, merged.UpdateTime)
	require.Len(t, merged.Value.(model.Vector), 2)
	require.ElementsMatch(t, []string{"tidb-1", "tidb-2"}, []string{
		string(merged.Value.(model.Vector)[0].Metric[model.LabelName("instance")]),
		string(merged.Value.(model.Vector)[1].Metric[model.LabelName("instance")]),
	})
}

func TestMergeQueryResultsMatrix(t *testing.T) {
	ts1 := time.Unix(10, 0)
	ts2 := time.Unix(20, 0)
	matrix1 := model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{model.LabelName("instance"): "tidb-1"},
			Values: []model.SamplePair{{Timestamp: model.Time(1000), Value: model.SampleValue(1)}},
		},
	}
	matrix2 := model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{model.LabelName("instance"): "tidb-2"},
			Values: []model.SamplePair{{Timestamp: model.Time(2000), Value: model.SampleValue(2)}},
		},
	}

	merged := mergeQueryResults([]metricsreader.QueryResult{
		{UpdateTime: ts1, Value: matrix1},
		{UpdateTime: ts2, Value: matrix2},
	})
	require.Equal(t, ts2, merged.UpdateTime)
	require.Len(t, merged.Value.(model.Matrix), 2)
	require.ElementsMatch(t, []string{"tidb-1", "tidb-2"}, []string{
		string(merged.Value.(model.Matrix)[0].Metric[model.LabelName("instance")]),
		string(merged.Value.(model.Matrix)[1].Metric[model.LabelName("instance")]),
	})
}

func TestMetricsQuerierPropagatesQueriesToExistingAndNewClusters(t *testing.T) {
	clusterA := newManagerTestEtcdCluster(t)
	clusterB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { clusterA.close(t) })
	t.Cleanup(func() { clusterB.close(t) })

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zap.NewNop(), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	expr := metricsreader.QueryExpr{PromQL: "sum(up)"}
	rule := metricsreader.QueryRule{
		Names:      []string{"up"},
		ResultType: model.ValVector,
	}
	mgr.MetricsQuerier().AddQueryExpr("up", expr, rule)

	require.Eventually(t, func() bool {
		return clusterHasBackendQueryRule(mgr.Snapshot()["cluster-a"], "up")
	}, 5*time.Second, 100*time.Millisecond)

	nextCfg := cfg.Clone()
	nextCfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: clusterA.addr},
		{Name: "cluster-b", PDAddrs: clusterB.addr},
	}
	cfgGetter.setConfig(nextCfg)
	cfgCh <- nextCfg.Clone()

	require.Eventually(t, func() bool {
		snapshot := mgr.Snapshot()
		return clusterHasBackendQueryRule(snapshot["cluster-a"], "up") &&
			clusterHasBackendQueryRule(snapshot["cluster-b"], "up")
	}, 5*time.Second, 100*time.Millisecond)

	mgr.MetricsQuerier().RemoveQueryExpr("up")
	require.Eventually(t, func() bool {
		snapshot := mgr.Snapshot()
		return !clusterHasBackendQueryRule(snapshot["cluster-a"], "up") &&
			!clusterHasBackendQueryRule(snapshot["cluster-b"], "up")
	}, 5*time.Second, 100*time.Millisecond)
}

func clusterHasBackendQueryRule(cluster *Cluster, key string) bool {
	if cluster == nil || cluster.metrics == nil {
		return false
	}
	metricsValue := reflect.ValueOf(cluster.metrics).Elem()
	backendReaderValue := metricsValue.FieldByName("backendReader")
	if !backendReaderValue.IsValid() || backendReaderValue.IsNil() {
		return false
	}
	queryRulesValue := backendReaderValue.Elem().FieldByName("queryRules")
	if !queryRulesValue.IsValid() || queryRulesValue.Len() == 0 {
		return false
	}
	return queryRulesValue.MapIndex(reflect.ValueOf(key)).IsValid()
}
