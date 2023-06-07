// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"time"

	"github.com/pingcap/TiProxy/pkg/metrics"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
)

func addCmdMetrics(cmd pnet.Command, addr string, startTime time.Time) {
	label := cmd.String()
	metrics.QueryTotalCounter.WithLabelValues(addr, label).Inc()

	// The duration labels are different with TiDB: Labels in TiDB are statement types.
	// However, the proxy is not aware of the statement types, so we use command types instead.
	cost := time.Since(startTime)
	metrics.QueryDurationHistogram.WithLabelValues(addr, label).Observe(cost.Seconds())
}

func readCmdCounter(cmd pnet.Command, addr string) (int, error) {
	label := cmd.String()
	return metrics.ReadCounter(metrics.QueryTotalCounter.WithLabelValues(addr, label))
}

func addGetBackendMetrics(duration time.Duration, succeed bool) {
	metrics.GetBackendHistogram.Observe(duration.Seconds())
	lbl := "succeed"
	if !succeed {
		lbl = "fail"
	}
	metrics.GetBackendCounter.WithLabelValues(lbl).Inc()
}
