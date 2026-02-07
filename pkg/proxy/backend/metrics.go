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
	counter             prometheus.Counter
	observer            prometheus.Observer
	interactionObserver prometheus.Observer
}

type mcPerBackend struct {
	// The duration labels are different with TiDB: Labels in TiDB are statement types.
	// However, the proxy is not aware of the statement types, so we use command types instead.
	cmds              [pnet.ComEnd]mcPerCmd
	inBytes           prometheus.Counter
	inPackets         prometheus.Counter
	outBytes          prometheus.Counter
	outPackets        prometheus.Counter
	handshakeDuration prometheus.Observer
	lastSeen          monotime.Time
}

type cmdMetricsCache struct {
	sync.Mutex
	backendMetrics map[string]*mcPerBackend
	lastGCTime     monotime.Time
}

func newCmdMetricsCache() cmdMetricsCache {
	return cmdMetricsCache{
		backendMetrics: make(map[string]*mcPerBackend),
	}
}

var cache = newCmdMetricsCache()

func addCmdMetrics(cmd pnet.Command, addr string, startTime monotime.Time) {
	if cmd >= pnet.ComEnd {
		return
	}
	now := monotime.Now()
	cache.Lock()
	defer cache.Unlock()
	backendMetrics := ensureBackendMetrics(addr, now)
	mc := &backendMetrics.cmds[cmd]
	if mc.counter == nil {
		label := cmd.String()
		mc.counter = metrics.QueryTotalCounter.WithLabelValues(addr, label)
		mc.observer = metrics.QueryDurationHistogram.WithLabelValues(addr, label)
	}
	mc.counter.Inc()
	cost := monotime.Since(startTime)
	mc.observer.Observe(cost.Seconds())
	cache.maybeRunGC(now)
}

func addCmdInteractionMetrics(cmd pnet.Command, addr string, duration time.Duration) {
	if cmd >= pnet.ComEnd {
		return
	}
	now := monotime.Now()
	cache.Lock()
	defer cache.Unlock()
	backendMetrics := ensureBackendMetrics(addr, now)
	mc := &backendMetrics.cmds[cmd]
	if mc.interactionObserver == nil {
		label := cmd.String()
		mc.interactionObserver = metrics.QueryInteractionDurationHistogram.WithLabelValues(addr, label)
	}
	mc.interactionObserver.Observe(duration.Seconds())
	cache.maybeRunGC(now)
}

func addTraffic(addr string, inBytes, inPackets, outBytes, outPackets uint64) {
	now := monotime.Now()
	cache.Lock()
	defer cache.Unlock()
	// Updating traffic per IO costs too much CPU, so update it per command.
	backendMetrics := ensureBackendMetrics(addr, now)
	backendMetrics.inBytes.Add(float64(inBytes))
	backendMetrics.inPackets.Add(float64(inPackets))
	backendMetrics.outBytes.Add(float64(outBytes))
	backendMetrics.outPackets.Add(float64(outPackets))
	cache.maybeRunGC(now)
}

func ensureBackendMetrics(addr string, now monotime.Time) *mcPerBackend {
	backendMetrics, ok := cache.backendMetrics[addr]
	if !ok {
		backendMetrics = &mcPerBackend{
			cmds:              [pnet.ComEnd]mcPerCmd{},
			inBytes:           metrics.InboundBytesCounter.WithLabelValues(addr),
			inPackets:         metrics.InboundPacketsCounter.WithLabelValues(addr),
			outBytes:          metrics.OutboundBytesCounter.WithLabelValues(addr),
			outPackets:        metrics.OutboundPacketsCounter.WithLabelValues(addr),
			handshakeDuration: metrics.HandshakeDurationHistogram.WithLabelValues(addr),
		}
		cache.backendMetrics[addr] = backendMetrics
	}
	backendMetrics.lastSeen = now
	return backendMetrics
}

func (cache *cmdMetricsCache) maybeRunGC(now monotime.Time) {
	ttl := metrics.BackendMetricsGCIdleTTL()
	interval := metrics.BackendMetricsGCInterval()
	if ttl <= 0 || interval <= 0 {
		return
	}
	if cache.lastGCTime > 0 && now.Before(cache.lastGCTime.Add(interval)) {
		return
	}
	cache.lastGCTime = now
	expireBefore := now.Sub(ttl)
	for addr, backendMetrics := range cache.backendMetrics {
		if backendMetrics.lastSeen.After(expireBefore) {
			continue
		}
		deleteBackendMetricLabels(addr)
		delete(cache.backendMetrics, addr)
	}
}

func deleteBackendMetricLabels(addr string) {
	metrics.InboundBytesCounter.DeleteLabelValues(addr)
	metrics.InboundPacketsCounter.DeleteLabelValues(addr)
	metrics.OutboundBytesCounter.DeleteLabelValues(addr)
	metrics.OutboundPacketsCounter.DeleteLabelValues(addr)
	metrics.HandshakeDurationHistogram.DeleteLabelValues(addr)
	for cmd := pnet.Command(0); cmd < pnet.ComEnd; cmd++ {
		label := cmd.String()
		metrics.QueryTotalCounter.DeleteLabelValues(addr, label)
		metrics.QueryDurationHistogram.DeleteLabelValues(addr, label)
		metrics.QueryInteractionDurationHistogram.DeleteLabelValues(addr, label)
	}
}

// Only used for testing, no need to optimize.
func readCmdCounter(cmd pnet.Command, addr string) (int, error) {
	label := cmd.String()
	return metrics.ReadCounter(metrics.QueryTotalCounter.WithLabelValues(addr, label))
}

// Only used for testing, no need to optimize.
func readCmdInteractionCounter(cmd pnet.Command, addr string) (uint64, error) {
	label := cmd.String()
	return metrics.ReadHistogramSampleCount(metrics.QueryInteractionDurationHistogram.WithLabelValues(addr, label))
}

// Only used for testing, no need to optimize.
func readTraffic(addr string) (inBytes, inPackets, outBytes, outPackets int, err error) {
	if inBytes, err = metrics.ReadCounter(metrics.InboundBytesCounter.WithLabelValues(addr)); err != nil {
		return
	}
	if inPackets, err = metrics.ReadCounter(metrics.InboundPacketsCounter.WithLabelValues(addr)); err != nil {
		return
	}
	if outBytes, err = metrics.ReadCounter(metrics.OutboundBytesCounter.WithLabelValues(addr)); err != nil {
		return
	}
	outPackets, err = metrics.ReadCounter(metrics.OutboundPacketsCounter.WithLabelValues(addr))
	return
}

func addHandshakeMetrics(addr string, duration time.Duration) {
	now := monotime.Now()
	cache.Lock()
	defer cache.Unlock()
	backendMetrics := ensureBackendMetrics(addr, now)
	backendMetrics.handshakeDuration.Observe(duration.Seconds())
	cache.maybeRunGC(now)
}

func addGetBackendMetrics(duration time.Duration, succeed bool) {
	metrics.GetBackendHistogram.Observe(duration.Seconds())
	lbl := "succeed"
	if !succeed {
		lbl = "fail"
	}
	metrics.GetBackendCounter.WithLabelValues(lbl).Inc()
}
