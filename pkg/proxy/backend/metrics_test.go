// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"sync"
	"testing"

	"github.com/pingcap/tiproxy/pkg/metrics"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkAddCmdMetrics(b *testing.B) {
	cmd := pnet.ComQuery
	addr := "127.0.0.1:4000"
	startTime := monotime.Now()
	for i := 0; i < b.N; i++ {
		addCmdMetrics(cmd, addr, startTime)
	}
}

func BenchmarkAddCmdMetricsArrayPointer(b *testing.B) {
	cmd := pnet.ComQuery
	addr := "127.0.0.1:4000"
	var lock sync.Mutex
	counter := make(map[string]*[pnet.ComEnd]prometheus.Counter)
	for i := 0; i < b.N; i++ {
		lock.Lock()

		addrCounter, ok := counter[addr]
		if !ok {
			addrCounter = &[pnet.ComEnd]prometheus.Counter{}
			counter[addr] = addrCounter
		}
		counter := addrCounter[cmd]
		if counter == nil {
			counter = metrics.QueryTotalCounter.WithLabelValues(addr, cmd.String())
			addrCounter[cmd] = counter
		}
		counter.Inc()

		lock.Unlock()
	}
}

func BenchmarkAddCmdMetricsArray(b *testing.B) {
	cmd := pnet.ComQuery
	addr := "127.0.0.1:4000"
	var lock sync.Mutex
	counter := make(map[string][pnet.ComEnd]prometheus.Counter)
	for i := 0; i < b.N; i++ {
		lock.Lock()

		addrCounter, ok := counter[addr]
		if !ok {
			addrCounter = [pnet.ComEnd]prometheus.Counter{}
			counter[addr] = addrCounter
		}
		counter := addrCounter[cmd]
		if counter == nil {
			counter = metrics.QueryTotalCounter.WithLabelValues(addr, cmd.String())
			addrCounter[cmd] = counter
		}
		counter.Inc()

		lock.Unlock()
	}
}

func BenchmarkAddCmdMetricsMap(b *testing.B) {
	cmd := pnet.ComQuery
	addr := "127.0.0.1:4000"
	var lock sync.Mutex
	counter := make(map[string]map[pnet.Command]prometheus.Counter)
	for i := 0; i < b.N; i++ {
		lock.Lock()

		addrCounter, ok := counter[addr]
		if !ok {
			addrCounter = make(map[pnet.Command]prometheus.Counter)
			counter[addr] = addrCounter
		}
		counter, ok := addrCounter[cmd]
		if !ok {
			counter = metrics.QueryTotalCounter.WithLabelValues(addr, cmd.String())
			addrCounter[cmd] = counter
		}
		counter.Inc()

		lock.Unlock()
	}
}

func BenchmarkAddCmdMetricsSimple(b *testing.B) {
	for i := 0; i < b.N; i++ {
		metrics.QueryTotalCounter.WithLabelValues("127.0.0.1:4000", pnet.ComQuery.String()).Inc()
	}
}
