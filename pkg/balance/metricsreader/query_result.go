// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

const (
	LabelNameInstance = "instance"
	LabelNameCluster  = "tiproxy_cluster"
)

// QueryExpr is used for querying Prometheus.
type QueryExpr struct {
	PromQL   string
	Range    time.Duration
	HasLabel bool
}

func (qe QueryExpr) PromRange(curTime time.Time) promv1.Range {
	if qe.Range == 0 {
		return promv1.Range{}
	}
	return promv1.Range{Start: curTime.Add(-qe.Range), End: curTime, Step: 15 * time.Second}
}

// QueryRule is used for processing the result from backend HTTP metrics API.
// It's too complicated for the BackendReader to parse PromQL, so let the caller to define the process function.
type QueryRule struct {
	// Names are the metric names that to be extracted from backend metrics.
	Names []string
	// Retention is the retention time of the metrics. Older metrics are expired.
	Retention time.Duration
	// Metric2Value defines the process from backend metrics (at a timepoint) to a float value.
	// E.g. division or aggregation.
	Metric2Value func(mfs map[string]*dto.MetricFamily) model.SampleValue
	// Range2Value defines the process from a time range of values to a float value.
	// E.g. irate[30s] or increase[1m].
	Range2Value func(pairs []model.SamplePair) model.SampleValue
	// ResultType indicates returning a vector or matrix as the final result.
	ResultType model.ValueType
}

type QueryResult struct {
	Value      model.Value
	UpdateTime time.Time
}

func (qr QueryResult) Empty() bool {
	if qr.Value == nil || reflect.ValueOf(qr.Value).IsNil() {
		return true
	}
	switch qr.Value.Type() {
	case model.ValNone:
		return true
	case model.ValMatrix:
		matrix := qr.Value.(model.Matrix)
		return len(matrix) == 0
	case model.ValVector:
		vector := qr.Value.(model.Vector)
		return len(vector) == 0
	default:
		// We don't need other value types.
		return false
	}
}

// GetSamplePair4Backend returns metric of a backend from a matrix.
func (qr QueryResult) GetSamplePair4Backend(backend policy.BackendCtx) []model.SamplePair {
	if qr.Value == nil || reflect.ValueOf(qr.Value).IsNil() {
		return nil
	}
	if qr.Value.Type() != model.ValMatrix {
		return nil
	}
	matrix := qr.Value.(model.Matrix)
	labelValue := getLabel4Backend(backend)
	clusterValue := getLabel4Cluster(backend)
	for _, m := range matrix {
		label, ok := m.Metric[LabelNameInstance]
		if !ok || labelValue != string(label) {
			continue
		}
		if !matchClusterLabel(m.Metric, clusterValue) {
			continue
		}
		return m.Values
	}
	return nil
}

// GetSample4Backend returns metric of a backend from a vector.
func (qr QueryResult) GetSample4Backend(backend policy.BackendCtx) *model.Sample {
	if qr.Value == nil || reflect.ValueOf(qr.Value).IsNil() {
		return nil
	}
	if qr.Value.Type() != model.ValVector {
		return nil
	}
	vector := qr.Value.(model.Vector)
	labelValue := getLabel4Backend(backend)
	clusterValue := getLabel4Cluster(backend)
	for _, m := range vector {
		label, ok := m.Metric[LabelNameInstance]
		if !ok || labelValue != string(label) {
			continue
		}
		if !matchClusterLabel(m.Metric, clusterValue) {
			continue
		}
		return m
	}
	return nil
}

func matchClusterLabel(metric model.Metric, clusterValue string) bool {
	if label, ok := metric[LabelNameCluster]; ok {
		return clusterValue == string(label)
	}
	return true
}

func getLabel4Backend(backend policy.BackendCtx) string {
	addr := backend.Addr()
	if isOperatorDeployed(addr) {
		// In operator deployment, the label value of `instance` is the pod name.
		return addr[:strings.Index(addr, ".")]
	}
	// In tiup deployment, the label value of `instance` is hostname:statusPort.
	backendInfo := backend.GetBackendInfo()
	return net.JoinHostPort(backendInfo.IP, strconv.Itoa(int(backendInfo.StatusPort)))
}

func getLabel4Cluster(backend policy.BackendCtx) string {
	return normalizeClusterMetricLabel(backend.GetBackendInfo().ClusterName)
}

// addr is the address of the backend status port.
func getLabel4Addr(addr string) string {
	if isOperatorDeployed(addr) {
		// In operator deployment, the label value of `instance` is the pod name.
		return addr[:strings.Index(addr, ".")]
	}
	// In tiup deployment, the label value of `instance` is hostname:statusPort.
	return addr
}

func normalizeClusterMetricLabel(clusterName string) string {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return config.DefaultBackendClusterName
	}
	return clusterName
}

func attachClusterLabel(qr QueryResult, clusterName string) QueryResult {
	clusterValue := model.LabelValue(normalizeClusterMetricLabel(clusterName))
	if qr.Value == nil || reflect.ValueOf(qr.Value).IsNil() {
		return qr
	}
	switch value := qr.Value.(type) {
	case model.Vector:
		for _, sample := range value {
			if sample.Metric == nil {
				sample.Metric = model.Metric{}
			}
			sample.Metric[LabelNameCluster] = clusterValue
		}
	case model.Matrix:
		for _, stream := range value {
			if stream.Metric == nil {
				stream.Metric = model.Metric{}
			}
			stream.Metric[LabelNameCluster] = clusterValue
		}
	}
	return qr
}

func isOperatorDeployed(addr string) bool {
	// (.+-tidb-[0-9]+).*peer.*.svc.*")
	for _, str := range []string{"-tidb-", ".", "peer", ".svc"} {
		idx := strings.Index(addr, str)
		if idx < 0 {
			return false
		}
		addr = addr[idx+len(str):]
	}
	return true
}
