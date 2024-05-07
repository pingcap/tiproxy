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
	addr := backend.Addr()
	var labelValue string
	if strings.Contains(addr, ".svc:") {
		// In operator deployment, the label value of `instance` is the pod name.
		labelValue = addr[:strings.Index(addr, ".")]
	} else {
		// In tiup deployment, the label value of `instance` is hostname:statusPort.
		backendInfo := backend.GetBackendInfo()
		labelValue = net.JoinHostPort(backendInfo.IP, strconv.Itoa(int(backendInfo.StatusPort)))
	}
	for _, m := range matrix {
		if label, ok := m.Metric[LabelNameInstance]; ok {
			if labelValue == (string)(label) {
				return m.Values
			}
		}
	}
	return nil
}
