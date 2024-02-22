// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"sync"
	"time"

	"github.com/pingcap/tiproxy/pkg/metrics"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/client_golang/prometheus"
)

type mcPerCmd struct {
	counter  prometheus.Counter
	observer prometheus.Observer
}

type mcPerBackend struct {
	// The duration labels are different with TiDB: Labels in TiDB are statement types.
	// However, the proxy is not aware of the statement types, so we use command types instead.
	cmds       [pnet.ComEnd]mcPerCmd
	inBytes    prometheus.Counter
	inPackets  prometheus.Counter
	outBytes   prometheus.Counter
	outPackets prometheus.Counter
}

type cmdMetricsCache struct {
	sync.Mutex
	backendMetrics map[string]*mcPerBackend
}

func newCmdMetricsCache() cmdMetricsCache {
	return cmdMetricsCache{
		backendMetrics: make(map[string]*mcPerBackend),
	}
}

var cache = newCmdMetricsCache()

func addCmdMetrics(cmd pnet.Command, addr string, startTime monotime.Time, inBytes, inPackets, outBytes, outPackets uint64) {
	cache.Lock()
	defer cache.Unlock()

	// Updating traffic per IO costs too much CPU, so update it per command.
	backendMetrics, ok := cache.backendMetrics[addr]
	if !ok {
		backendMetrics = &mcPerBackend{
			cmds:       [pnet.ComEnd]mcPerCmd{},
			inBytes:    metrics.InboundBytesCounter.WithLabelValues(addr),
			inPackets:  metrics.InboundPacketsCounter.WithLabelValues(addr),
			outBytes:   metrics.OutboundBytesCounter.WithLabelValues(addr),
			outPackets: metrics.OutboundPacketsCounter.WithLabelValues(addr),
		}
		cache.backendMetrics[addr] = backendMetrics
	}
	backendMetrics.inBytes.Add(float64(inBytes))
	backendMetrics.inPackets.Add(float64(inPackets))
	backendMetrics.outBytes.Add(float64(outBytes))
	backendMetrics.outPackets.Add(float64(outPackets))

	// query metrics
	mc := &backendMetrics.cmds[cmd]
	if mc.counter == nil {
		label := cmd.String()
		mc.counter = metrics.QueryTotalCounter.WithLabelValues(addr, label)
		mc.observer = metrics.QueryDurationHistogram.WithLabelValues(addr, label)
	}
	mc.counter.Inc()
	cost := monotime.Since(startTime)
	mc.observer.Observe(cost.Seconds())
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
