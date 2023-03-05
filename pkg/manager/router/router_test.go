// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"container/list"
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/stretchr/testify/require"
)

type mockRedirectableConn struct {
	sync.Mutex
	t        *testing.T
	connID   uint64
	from, to string
	status   BackendStatus
	receiver ConnEventReceiver
}

func (conn *mockRedirectableConn) SetEventReceiver(receiver ConnEventReceiver) {
	conn.Lock()
	conn.receiver = receiver
	conn.Unlock()
}

func (conn *mockRedirectableConn) Redirect(addr string) {
	conn.Lock()
	require.Len(conn.t, conn.to, 0)
	conn.to = addr
	conn.Unlock()
}

func (conn *mockRedirectableConn) GetRedirectingAddr() string {
	conn.Lock()
	defer conn.Unlock()
	return conn.to
}

func (conn *mockRedirectableConn) NotifyBackendStatus(status BackendStatus) {
	conn.Lock()
	conn.status = status
	conn.Unlock()
}

func (conn *mockRedirectableConn) ConnectionID() uint64 {
	return conn.connID
}

func (conn *mockRedirectableConn) getAddr() (string, string) {
	conn.Lock()
	defer conn.Unlock()
	return conn.from, conn.to
}

func (conn *mockRedirectableConn) redirectSucceed() {
	conn.Lock()
	require.Greater(conn.t, len(conn.to), 0)
	conn.from = conn.to
	conn.to = ""
	conn.Unlock()
}

func (conn *mockRedirectableConn) redirectFail() {
	conn.Lock()
	require.Greater(conn.t, len(conn.to), 0)
	conn.to = ""
	conn.Unlock()
}

type routerTester struct {
	t         *testing.T
	router    *ScoreBasedRouter
	connID    uint64
	conns     map[uint64]*mockRedirectableConn
	backendID int
}

func newRouterTester(t *testing.T) *routerTester {
	router := &ScoreBasedRouter{
		logger:   logger.CreateLoggerForTest(t),
		backends: list.New(),
	}
	t.Cleanup(router.Close)
	return &routerTester{
		t:      t,
		router: router,
		conns:  make(map[uint64]*mockRedirectableConn),
	}
}

func (tester *routerTester) createConn() *mockRedirectableConn {
	tester.connID++
	return &mockRedirectableConn{
		t:      tester.t,
		connID: tester.connID,
	}
}

func (tester *routerTester) addBackends(num int) {
	backends := make(map[string]BackendStatus)
	for i := 0; i < num; i++ {
		tester.backendID++
		addr := strconv.Itoa(tester.backendID)
		backends[addr] = StatusHealthy
		metrics.BackendConnGauge.WithLabelValues(addr).Set(0)
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) killBackends(num int) {
	backends := make(map[string]BackendStatus)
	indexes := rand.Perm(tester.router.backends.Len())
	for _, index := range indexes {
		if len(backends) >= num {
			break
		}
		// set the ith backend as unhealthy
		backend := tester.getBackendByIndex(index)
		if backend.status == StatusCannotConnect {
			continue
		}
		backends[backend.addr] = StatusCannotConnect
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) updateBackendStatusByAddr(addr string, status BackendStatus) {
	backends := map[string]BackendStatus{
		addr: status,
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) getBackendByIndex(index int) *backendWrapper {
	be := tester.router.backends.Front()
	for i := 0; be != nil && i < index; be, i = be.Next(), i+1 {
	}
	require.NotNil(tester.t, be)
	return be.Value.(*backendWrapper)
}

func (tester *routerTester) checkBackendOrder() {
	score := math.MaxInt
	for be := tester.router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value.(*backendWrapper)
		// Empty unhealthy backends should be removed.
		if backend.status == StatusCannotConnect {
			require.NotEqual(tester.t, 0, backend.connList.Len())
		}
		curScore := backend.score()
		require.GreaterOrEqual(tester.t, score, curScore)
		score = curScore
	}
}

func (tester *routerTester) simpleRoute(conn RedirectableConn) string {
	selector := tester.router.GetBackendSelector()
	addr, err := selector.Next()
	require.NoError(tester.t, err)
	if len(addr) > 0 {
		err := selector.Succeed(conn)
		require.NoError(tester.t, err)
	}
	return addr
}

func (tester *routerTester) addConnections(num int) {
	for i := 0; i < num; i++ {
		conn := tester.createConn()
		addr := tester.simpleRoute(conn)
		require.True(tester.t, len(addr) > 0)
		conn.from = addr
		tester.conns[conn.connID] = conn
	}
	tester.checkBackendOrder()
}

func (tester *routerTester) closeConnections(num int, redirecting bool) {
	conns := make(map[uint64]*mockRedirectableConn, num)
	for id, conn := range tester.conns {
		if redirecting {
			if len(conn.GetRedirectingAddr()) == 0 {
				continue
			}
		} else {
			if len(conn.GetRedirectingAddr()) > 0 {
				continue
			}
		}
		conns[id] = conn
		if len(conns) >= num {
			break
		}
	}
	for _, conn := range conns {
		err := tester.router.OnConnClosed(conn.from, conn)
		require.NoError(tester.t, err)
		delete(tester.conns, conn.connID)
	}
	tester.checkBackendOrder()
}

func (tester *routerTester) rebalance(num int) {
	tester.router.rebalance(num)
	tester.checkBackendOrder()
}

func (tester *routerTester) redirectFinish(num int, succeed bool) {
	i := 0
	for _, conn := range tester.conns {
		if len(conn.GetRedirectingAddr()) == 0 {
			continue
		}

		from, to := conn.from, conn.to
		prevCount, err := readMigrateCounter(from, to, succeed)
		require.NoError(tester.t, err)
		if succeed {
			err = tester.router.OnRedirectSucceed(from, to, conn)
			require.NoError(tester.t, err)
			conn.redirectSucceed()
		} else {
			err = tester.router.OnRedirectFail(from, to, conn)
			require.NoError(tester.t, err)
			conn.redirectFail()
		}
		curCount, err := readMigrateCounter(from, to, succeed)
		require.NoError(tester.t, err)
		require.Equal(tester.t, prevCount+1, curCount)

		i++
		if i >= num {
			break
		}
	}
	tester.checkBackendOrder()
}

func (tester *routerTester) checkBalanced() {
	maxNum, minNum := 0, math.MaxInt
	for be := tester.router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value.(*backendWrapper)
		// Empty unhealthy backends should be removed.
		require.Equal(tester.t, StatusHealthy, backend.status)
		curScore := backend.score()
		if curScore > maxNum {
			maxNum = curScore
		}
		if curScore < minNum {
			minNum = curScore
		}
	}
	ratio := float64(maxNum) / float64(minNum+1)
	require.LessOrEqual(tester.t, ratio, rebalanceMaxScoreRatio)
}

func (tester *routerTester) checkRedirectingNum(num int) {
	redirectingNum := 0
	for _, conn := range tester.conns {
		if len(conn.GetRedirectingAddr()) > 0 {
			redirectingNum++
		}
	}
	require.Equal(tester.t, num, redirectingNum)
}

func (tester *routerTester) checkBackendNum(num int) {
	require.Equal(tester.t, num, tester.router.backends.Len())
}

func (tester *routerTester) checkBackendConnMetrics() {
	for be := tester.router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value.(*backendWrapper)
		val, err := readBackendConnMetrics(backend.addr)
		require.NoError(tester.t, err)
		require.Equal(tester.t, backend.connList.Len(), val)
	}
}

func (tester *routerTester) clear() {
	tester.conns = make(map[uint64]*mockRedirectableConn)
	tester.router.backends = list.New()
}

// Test that the backends are always ordered by scores.
func TestBackendScore(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	tester.killBackends(2)
	tester.addConnections(100)
	tester.checkBackendConnMetrics()
	// 90 not redirecting
	tester.closeConnections(10, false)
	tester.checkBackendConnMetrics()
	// make sure rebalance will work
	tester.addBackends(3)
	// 40 not redirecting, 50 redirecting
	tester.rebalance(50)
	tester.checkRedirectingNum(50)
	// 40 not redirecting, 40 redirecting
	tester.closeConnections(10, true)
	tester.checkRedirectingNum(40)
	// 50 not redirecting, 30 redirecting
	tester.redirectFinish(10, true)
	tester.checkRedirectingNum(30)
	// 60 not redirecting, 20 redirecting
	tester.redirectFinish(10, false)
	tester.checkRedirectingNum(20)
	// 50 not redirecting, 20 redirecting
	tester.closeConnections(10, false)
	tester.checkRedirectingNum(20)
}

// Test that the connections are always balanced after rebalance and routing.
func TestConnBalanced(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)

	// balanced after routing
	tester.addConnections(100)
	tester.checkBalanced()

	tests := []func(){
		func() {
			// balanced after scale in
			tester.killBackends(1)
		},
		func() {
			// balanced after scale out
			tester.addBackends(1)
		},
		func() {
			// balanced after closing connections
			tester.closeConnections(10, false)
		},
	}

	for _, tt := range tests {
		tt()
		tester.rebalance(100)
		tester.redirectFinish(100, true)
		tester.checkBalanced()
		tester.checkBackendConnMetrics()
	}
}

// Test that routing fails when there's no healthy backends.
func TestNoBackends(t *testing.T) {
	tester := newRouterTester(t)
	conn := tester.createConn()
	addr := tester.simpleRoute(conn)
	require.True(t, len(addr) == 0)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	addr = tester.simpleRoute(conn)
	require.True(t, len(addr) == 0)
}

// Test that the backends returned by the BackendSelector are complete and different.
func TestSelectorReturnOrder(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	selector := tester.router.GetBackendSelector()
	for i := 0; i < 3; i++ {
		addrs := make(map[string]struct{}, 3)
		for j := 0; j < 3; j++ {
			addr, err := selector.Next()
			require.NoError(t, err)
			addrs[addr] = struct{}{}
		}
		// All 3 addresses are different.
		require.Equal(t, 3, len(addrs))
		addr, err := selector.Next()
		require.NoError(t, err)
		require.True(t, len(addr) == 0)
		selector.Reset()
	}

	tester.killBackends(1)
	for i := 0; i < 2; i++ {
		addr, err := selector.Next()
		require.NoError(t, err)
		require.True(t, len(addr) > 0)
	}
	addr, err := selector.Next()
	require.NoError(t, err)
	require.True(t, len(addr) == 0)
	selector.Reset()

	tester.addBackends(1)
	for i := 0; i < 3; i++ {
		addr, err := selector.Next()
		require.NoError(t, err)
		require.True(t, len(addr) > 0)
	}
	addr, err = selector.Next()
	require.NoError(t, err)
	require.True(t, len(addr) == 0)
}

// Test that the backends are balanced during rolling restart.
func TestRollingRestart(t *testing.T) {
	tester := newRouterTester(t)
	backendNum := 3
	tester.addBackends(backendNum)
	tester.addConnections(100)
	tester.checkBalanced()

	backendAddrs := make([]string, 0, backendNum)
	for i := 0; i < backendNum; i++ {
		backendAddrs = append(backendAddrs, tester.getBackendByIndex(i).addr)
	}

	for i := 0; i < backendNum+1; i++ {
		if i > 0 {
			tester.updateBackendStatusByAddr(backendAddrs[i-1], StatusHealthy)
			tester.rebalance(100)
			tester.redirectFinish(100, true)
			tester.checkBalanced()
		}
		if i < backendNum {
			tester.updateBackendStatusByAddr(backendAddrs[i], StatusCannotConnect)
			tester.rebalance(100)
			tester.redirectFinish(100, true)
			tester.checkBalanced()
		}
	}
}

// Test the corner cases of rebalance.
func TestRebalanceCornerCase(t *testing.T) {
	tester := newRouterTester(t)
	tests := []func(){
		func() {
			// Balancer won't work when there's no backend.
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Balancer won't work when there's only one backend.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Router should have already balanced it.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.addBackends(1)
			tester.addConnections(10)
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Balancer won't work when all the backends are unhealthy.
			tester.addBackends(2)
			tester.addConnections(20)
			tester.killBackends(2)
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
		func() {
			// The parameter limits the redirecting num.
			tester.addBackends(2)
			tester.addConnections(50)
			tester.killBackends(1)
			tester.rebalance(5)
			tester.checkRedirectingNum(5)
		},
		func() {
			// All the connections are redirected to the new healthy one and the unhealthy backends are removed.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(10)
			tester.checkBackendNum(1)
			backend := tester.getBackendByIndex(0)
			require.Len(t, backend.connMap, 10)
		},
		func() {
			// Connections won't be redirected again before redirection finishes.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(10)
			backend := tester.getBackendByIndex(0)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(10)
			require.Len(t, backend.connMap, 10)
		},
		func() {
			// After redirection fails, the connections are moved back to the unhealthy backends.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkBackendNum(1)
			tester.redirectFinish(10, false)
			tester.checkBackendNum(2)
		},
		func() {
			// It won't rebalance when there's no connection.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.closeConnections(10, false)
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
		func() {
			// It won't rebalance when there's only 1 connection.
			tester.addBackends(1)
			tester.addConnections(1)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// It won't rebalance when only 2 connections are on 3 backends.
			tester.addBackends(2)
			tester.addConnections(2)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Connections will be redirected again immediately after failure.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.redirectFinish(10, false)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(0)
		},
	}

	for _, test := range tests {
		test()
		tester.clear()
	}
}

// Test all kinds of events occur concurrently.
func TestConcurrency(t *testing.T) {
	// Router.observer doesn't work because the etcd is always empty.
	// We create other goroutines to change backends easily.
	etcd := createEtcdServer(t, "127.0.0.1:0")
	client := createEtcdClient(t, etcd)
	healthCheckConfig := newHealthCheckConfigForTest()
	fetcher := NewPDFetcher(client, logger.CreateLoggerForTest(t), healthCheckConfig)
	router, err := NewScoreBasedRouter(logger.CreateLoggerForTest(t), nil, fetcher, healthCheckConfig)
	require.NoError(t, err)

	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Create 3 backends.
	backends := map[string]BackendStatus{
		"0": StatusHealthy,
		"1": StatusHealthy,
		"2": StatusHealthy,
	}
	router.OnBackendChanged(backends, nil)
	for addr, status := range backends {
		func(addr string, status BackendStatus) {
			wg.Run(func() {
				for {
					waitTime := rand.Intn(50) + 30
					select {
					case <-time.After(time.Duration(waitTime) * time.Millisecond):
					case <-ctx.Done():
						return
					}
					if status == StatusHealthy {
						status = StatusCannotConnect
					} else {
						status = StatusHealthy
					}
					router.OnBackendChanged(map[string]BackendStatus{
						addr: status,
					}, nil)
				}
			})
		}(addr, status)
	}

	// Create 20 connections.
	for i := 0; i < 20; i++ {
		func(connID uint64) {
			wg.Run(func() {
				var conn *mockRedirectableConn
				for {
					waitTime := rand.Intn(20) + 10
					select {
					case <-time.After(time.Duration(waitTime) * time.Millisecond):
					case <-ctx.Done():
						return
					}

					if conn == nil {
						// not connected, connect
						conn = &mockRedirectableConn{
							t:      t,
							connID: connID,
						}
						selector := router.GetBackendSelector()
						addr, err := selector.Next()
						require.NoError(t, err)
						if len(addr) == 0 {
							conn = nil
							continue
						}
						err = selector.Succeed(conn)
						require.NoError(t, err)
						conn.from = addr
					} else if len(conn.GetRedirectingAddr()) > 0 {
						// redirecting, 70% success, 20% fail, 10% close
						i := rand.Intn(10)
						from, to := conn.getAddr()
						var err error
						if i < 1 {
							err = router.OnConnClosed(from, conn)
							conn = nil
						} else if i < 3 {
							conn.redirectFail()
							err = router.OnRedirectFail(from, to, conn)
						} else {
							conn.redirectSucceed()
							err = router.OnRedirectSucceed(from, to, conn)
						}
						require.NoError(t, err)
					} else {
						// not redirecting, 20% close
						i := rand.Intn(10)
						if i < 2 {
							// The balancer may happen to redirect it concurrently - that's exactly what may happen.
							from, _ := conn.getAddr()
							err := router.OnConnClosed(from, conn)
							require.NoError(t, err)
							conn = nil
						}
					}
				}
			})
		}(uint64(i))
	}
	wg.Wait()
	cancel()
	router.Close()
}

// Test that the backends are refreshed immediately after it's empty.
func TestRefresh(t *testing.T) {
	backends := make([]string, 0)
	var m sync.Mutex
	fetcher := NewExternalFetcher(func() ([]string, error) {
		m.Lock()
		defer m.Unlock()
		return backends, nil
	})
	// Create a router with a very long health check interval.
	lg := logger.CreateLoggerForTest(t)
	rt := &ScoreBasedRouter{
		logger:   lg,
		backends: list.New(),
	}
	cfg := config.NewDefaultHealthCheckConfig()
	cfg.Interval = time.Minute
	observer, err := StartBackendObserver(lg, rt, nil, cfg, fetcher)
	require.NoError(t, err)
	rt.Lock()
	rt.observer = observer
	rt.Unlock()
	defer rt.Close()
	// The initial backends are empty.
	selector := rt.GetBackendSelector()
	addr, err := selector.Next()
	require.NoError(t, err)
	require.True(t, len(addr) == 0)
	// Create a new backend and add to the list.
	server := newBackendServer(t)
	m.Lock()
	backends = append(backends, server.sqlAddr)
	m.Unlock()
	defer server.close()
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		addr, err = selector.Next()
		require.NoError(t, err)
		return len(addr) > 0
	}, 3*time.Second, 100*time.Millisecond)
}

func TestObserveError(t *testing.T) {
	backends := make([]string, 0)
	var observeError error
	var m sync.Mutex
	fetcher := NewExternalFetcher(func() ([]string, error) {
		m.Lock()
		defer m.Unlock()
		return backends, observeError
	})
	// Create a router with a very short health check interval.
	lg := logger.CreateLoggerForTest(t)
	rt := &ScoreBasedRouter{
		logger:   lg,
		backends: list.New(),
	}
	observer, err := StartBackendObserver(lg, rt, nil, newHealthCheckConfigForTest(), fetcher)
	require.NoError(t, err)
	rt.Lock()
	rt.observer = observer
	rt.Unlock()
	defer rt.Close()
	// No backends and no error.
	selector := rt.GetBackendSelector()
	addr, err := selector.Next()
	require.NoError(t, err)
	require.True(t, len(addr) == 0)
	// Create a new backend and add to the list.
	server := newBackendServer(t)
	m.Lock()
	backends = append(backends, server.sqlAddr)
	m.Unlock()
	defer server.close()
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		selector.Reset()
		addr, err = selector.Next()
		require.NoError(t, err)
		return len(addr) > 0
	}, 3*time.Second, 100*time.Millisecond)
	// Mock an observe error.
	m.Lock()
	observeError = errors.New("mock observe error")
	m.Unlock()
	require.Eventually(t, func() bool {
		selector.Reset()
		addr, err = selector.Next()
		return len(addr) == 0 && err != nil
	}, 3*time.Second, 100*time.Millisecond)
	// Clear the observe error.
	m.Lock()
	observeError = nil
	m.Unlock()
	require.Eventually(t, func() bool {
		selector.Reset()
		addr, err = selector.Next()
		return len(addr) > 0 && err == nil
	}, 3*time.Second, 100*time.Millisecond)
}

func TestDisableHealthCheck(t *testing.T) {
	backends := []string{"127.0.0.1:4000"}
	var m sync.Mutex
	fetcher := NewExternalFetcher(func() ([]string, error) {
		m.Lock()
		defer m.Unlock()
		return backends, nil
	})
	// Create a router with a very short health check interval.
	lg := logger.CreateLoggerForTest(t)
	rt, err := NewScoreBasedRouter(lg, nil, fetcher, &config.HealthCheck{Enable: false})
	require.NoError(t, err)
	defer rt.Close()
	// No backends and no error.
	selector := rt.GetBackendSelector()
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		addr, err := selector.Next()
		require.NoError(t, err)
		return addr == "127.0.0.1:4000"
	}, 3*time.Second, 100*time.Millisecond)
	// Replace the backend.
	m.Lock()
	backends[0] = "127.0.0.1:5000"
	m.Unlock()
	require.Eventually(t, func() bool {
		addr, err := selector.Next()
		require.NoError(t, err)
		return addr == "127.0.0.1:5000"
	}, 3*time.Second, 100*time.Millisecond)
}

func TestSetBackendStatus(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	for _, conn := range tester.conns {
		require.Equal(t, StatusCannotConnect, conn.status)
	}
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).addr, StatusHealthy)
	for _, conn := range tester.conns {
		require.Equal(t, StatusHealthy, conn.status)
	}
}
