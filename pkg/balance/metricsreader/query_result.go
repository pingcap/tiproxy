// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"time"

	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/common/model"
)

const (
	LabelNameInstance = "instance"
)

type QueryExpr struct {
	PromQL   string
	Range    time.Duration
	HasLabel bool
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

// GetMetric4Backend returns metric of a backend.
func (qr QueryResult) GetMetric4Backend(backend policy.BackendCtx) []model.SamplePair {
	if qr.Value == nil {
		return nil
	}
	if qr.Value.Type() != model.ValMatrix {
		return nil
	}
	matrix := qr.Value.(model.Matrix)
	for _, m := range matrix {
		if label, ok := m.Metric[LabelNameInstance]; ok {
			// FIXME: this is incorrect.
			if (string)(label) == backend.Addr() {
				return m.Values
			}
		}
	}
	return nil
}
