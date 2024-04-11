// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import "github.com/prometheus/common/model"

type QueryResult struct {
	Value model.Value
	Err   error
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
