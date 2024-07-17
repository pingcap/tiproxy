// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

const (
	LabelNameInstance = "instance"
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
	Err        error
	UpdateTime monotime.Time
}

func (qr QueryResult) Empty() bool {
	if qr.Value == nil {
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
	if qr.Value == nil {
		return nil
	}
	if qr.Value.Type() != model.ValMatrix {
		return nil
	}
	matrix := qr.Value.(model.Matrix)
	labelValue := getLabel4Backend(backend)
	for _, m := range matrix {
		if label, ok := m.Metric[LabelNameInstance]; ok {
			if labelValue == (string)(label) {
				return m.Values
			}
		}
	}
	return nil
}

// GetSample4Backend returns metric of a backend from a vector.
func (qr QueryResult) GetSample4Backend(backend policy.BackendCtx) *model.Sample {
	if qr.Value == nil {
		return nil
	}
	if qr.Value.Type() != model.ValVector {
		return nil
	}
	vector := qr.Value.(model.Vector)
	labelValue := getLabel4Backend(backend)
	for _, m := range vector {
		if label, ok := m.Metric[LabelNameInstance]; ok {
			if labelValue == (string)(label) {
				return m
			}
		}
	}
	return nil
}

func getLabel4Backend(backend policy.BackendCtx) string {
	addr := backend.Addr()
	if strings.Contains(addr, ".svc:") {
		// In operator deployment, the label value of `instance` is the pod name.
		return addr[:strings.Index(addr, ".")]
	}
	// In tiup deployment, the label value of `instance` is hostname:statusPort.
	backendInfo := backend.GetBackendInfo()
	return net.JoinHostPort(backendInfo.IP, strconv.Itoa(int(backendInfo.StatusPort)))
}
