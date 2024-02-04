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

type cmdMetricsCache struct {
	sync.Mutex
	counter   map[string][pnet.ComEnd]prometheus.Counter  // addr -> [cmd]counter
	histogram map[string][pnet.ComEnd]prometheus.Observer // addr -> [cmd]observer
}

func newCmdMetricsCache() cmdMetricsCache {
	return cmdMetricsCache{
		counter:   make(map[string][pnet.ComEnd]prometheus.Counter),
		histogram: make(map[string][pnet.ComEnd]prometheus.Observer),
	}
}

var cache = newCmdMetricsCache()

func addCmdMetrics(cmd pnet.Command, addr string, startTime monotime.Time) {
	cache.Lock()
	defer cache.Unlock()

	addrCounter, ok := cache.counter[addr]
	if !ok {
		addrCounter = [pnet.ComEnd]prometheus.Counter{}
		cache.counter[addr] = addrCounter
	}
	counter := addrCounter[cmd]
	if counter == nil {
		counter = metrics.QueryTotalCounter.WithLabelValues(addr, cmd.String())
		addrCounter[cmd] = counter
	}
	counter.Inc()

	// The duration labels are different with TiDB: Labels in TiDB are statement types.
	// However, the proxy is not aware of the statement types, so we use command types instead.
	cost := monotime.Since(startTime)
	addrHist, ok := cache.histogram[addr]
	if !ok {
		addrHist = [pnet.ComEnd]prometheus.Observer{}
		cache.histogram[addr] = addrHist
	}
	hist := addrHist[cmd]
	if hist == nil {
		hist = metrics.QueryDurationHistogram.WithLabelValues(addr, cmd.String())
		addrHist[cmd] = hist
	}
	hist.Observe(cost.Seconds())
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
