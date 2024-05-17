// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/stretchr/testify/require"
)

type mockRedirectableConn struct {
	sync.Mutex
	t        *testing.T
	kv       map[any]any
	connID   uint64
	from     BackendInst
	to       BackendInst
	receiver ConnEventReceiver
}

func newMockRedirectableConn(t *testing.T, id uint64) *mockRedirectableConn {
	return &mockRedirectableConn{
		t:      t,
		connID: id,
		kv:     make(map[any]any),
	}
}

func (conn *mockRedirectableConn) SetEventReceiver(receiver ConnEventReceiver) {
	conn.Lock()
	conn.receiver = receiver
	conn.Unlock()
}

func (conn *mockRedirectableConn) SetValue(k, v any) {
	conn.Lock()
	conn.kv[k] = v
	conn.Unlock()
}

func (conn *mockRedirectableConn) Value(k any) any {
	conn.Lock()
	v := conn.kv[k]
	conn.Unlock()
	return v
}

func (conn *mockRedirectableConn) Redirect(inst BackendInst) bool {
	conn.Lock()
	require.Nil(conn.t, conn.to)
	require.True(conn.t, inst.Healthy())
	conn.to = inst
	conn.Unlock()
	return true
}

func (conn *mockRedirectableConn) GetRedirectingAddr() string {
	conn.Lock()
	defer conn.Unlock()
	if conn.to == nil {
		return ""
	}
	return conn.to.Addr()
}

func (conn *mockRedirectableConn) ConnectionID() uint64 {
	return conn.connID
}

func (conn *mockRedirectableConn) SaveSession() bool {
	return true
}

func (conn *mockRedirectableConn) getAddr() (string, string) {
	conn.Lock()
	defer conn.Unlock()
	var to string
	if conn.to != nil && !reflect.ValueOf(conn.to).IsNil() {
		to = conn.to.Addr()
	}
	return conn.from.Addr(), to
}

func (conn *mockRedirectableConn) redirectSucceed() {
	conn.Lock()
	require.True(conn.t, conn.to != nil && !reflect.ValueOf(conn.to).IsNil())
	conn.from = conn.to
	conn.to = nil
	conn.Unlock()
}

func (conn *mockRedirectableConn) redirectFail() {
	conn.Lock()
	require.True(conn.t, conn.to != nil && !reflect.ValueOf(conn.to).IsNil())
	conn.to = nil
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
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg, false)
	t.Cleanup(router.Close)
	return &routerTester{
		t:      t,
		router: router,
		conns:  make(map[uint64]*mockRedirectableConn),
	}
}

func (tester *routerTester) createConn() *mockRedirectableConn {
	tester.connID++
	return newMockRedirectableConn(tester.t, tester.connID)
}

func (tester *routerTester) addBackends(num int) {
	backends := make(map[string]*BackendHealth)
	for i := 0; i < num; i++ {
		tester.backendID++
		addr := strconv.Itoa(tester.backendID)
		backends[addr] = &BackendHealth{
			Status: StatusHealthy,
		}
		metrics.BackendConnGauge.WithLabelValues(addr).Set(0)
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) killBackends(num int) {
	backends := make(map[string]*BackendHealth)
	indexes := rand.Perm(tester.router.backends.Len())
	for _, index := range indexes {
		if len(backends) >= num {
			break
		}
		// set the ith backend as unhealthy
		backend := tester.getBackendByIndex(index)
		if backend.Status() == StatusCannotConnect {
			continue
		}
		backends[backend.addr] = &BackendHealth{
			Status: StatusCannotConnect,
		}
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) updateBackendStatusByAddr(addr string, status BackendStatus) {
	backends := map[string]*BackendHealth{
		addr: {
			Status: status,
		},
	}
	tester.router.OnBackendChanged(backends, nil)
	tester.checkBackendOrder()
}

func (tester *routerTester) getBackendByIndex(index int) *backendWrapper {
	be := tester.router.backends.Front()
	for i := 0; be != nil && i < index; be, i = be.Next(), i+1 {
	}
	require.NotNil(tester.t, be)
	return be.Value
}

func (tester *routerTester) checkBackendOrder() {
	score := math.MaxInt
	for be := tester.router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value
		// Empty unhealthy backends should be removed.
		if backend.Status() == StatusCannotConnect {
			require.True(tester.t, backend.connList.Len() > 0 || backend.connScore > 0)
		}
		curScore := backend.score()
		require.GreaterOrEqual(tester.t, score, curScore)
		score = curScore
	}
}

func (tester *routerTester) simpleRoute(conn RedirectableConn) BackendInst {
	selector := tester.router.GetBackendSelector()
	backend, err := selector.Next()
	if err != ErrNoBackend {
		require.NoError(tester.t, err)
		selector.Finish(conn, true)
	}
	return backend
}

func (tester *routerTester) addConnections(num int) {
	for i := 0; i < num; i++ {
		conn := tester.createConn()
		backend := tester.simpleRoute(conn)
		require.False(tester.t, backend == nil || reflect.ValueOf(backend).IsNil())
		conn.from = backend
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
		err := tester.router.OnConnClosed(conn.from.Addr(), conn)
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
		prevCount, err := readMigrateCounter(from.Addr(), to.Addr(), succeed)
		require.NoError(tester.t, err)
		if succeed {
			err = tester.router.OnRedirectSucceed(from.Addr(), to.Addr(), conn)
			require.NoError(tester.t, err)
			conn.redirectSucceed()
		} else {
			err = tester.router.OnRedirectFail(from.Addr(), to.Addr(), conn)
			require.NoError(tester.t, err)
			conn.redirectFail()
		}
		curCount, err := readMigrateCounter(from.Addr(), to.Addr(), succeed)
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
		backend := be.Value
		// Empty unhealthy backends should be removed.
		require.Equal(tester.t, StatusHealthy, backend.Status())
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
		backend := be.Value
		val, err := readBackendConnMetrics(backend.addr)
		require.NoError(tester.t, err)
		require.Equal(tester.t, backend.connList.Len(), val)
	}
}

func (tester *routerTester) clear() {
	tester.conns = make(map[uint64]*mockRedirectableConn)
	tester.router.backends.Init()
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
	backend := tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	backend = tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
}

// Test that the backends returned by the BackendSelector are complete and different.
func TestSelectorReturnOrder(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	selector := tester.router.GetBackendSelector()
	for i := 0; i < 3; i++ {
		addrs := make(map[string]struct{}, 3)
		for j := 0; j < 3; j++ {
			backend, err := selector.Next()
			require.NoError(t, err)
			addrs[backend.Addr()] = struct{}{}
		}
		// All 3 addresses are different.
		require.Equal(t, 3, len(addrs))
	}

	tester.killBackends(1)
	for i := 0; i < 2; i++ {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err := selector.Next()
	require.NoError(t, err)

	tester.addBackends(1)
	for i := 0; i < 3; i++ {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err = selector.Next()
	require.NoError(t, err)
}

// Test that the backends are balanced even when routing are concurrent.
func TestRouteConcurrently(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	addrs := make(map[string]int, 3)
	selectors := make([]BackendSelector, 0, 30)
	// All the clients are calling Next() but not yet Finish().
	for i := 0; i < 30; i++ {
		selector := tester.router.GetBackendSelector()
		backend, err := selector.Next()
		require.NoError(t, err)
		addrs[backend.Addr()]++
		selectors = append(selectors, selector)
	}
	require.Equal(t, 3, len(addrs))
	for _, num := range addrs {
		require.Equal(t, 10, num)
	}
	for i := 0; i < 3; i++ {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 10, backend.connScore)
	}
	for _, selector := range selectors {
		selector.Finish(nil, false)
	}
	for i := 0; i < 3; i++ {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 0, backend.connScore)
	}
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
			tester.checkBackendNum(2)
			backend := tester.getBackendByIndex(1)
			require.Equal(t, 10, backend.connScore)
			tester.redirectFinish(10, true)
			tester.checkBackendNum(1)
		},
		func() {
			// Connections won't be redirected again before redirection finishes.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkRedirectingNum(10)
			backend := tester.getBackendByIndex(0)
			require.Equal(t, 10, backend.connScore)
			require.Equal(t, 0, backend.connList.Len())
			backend = tester.getBackendByIndex(1)
			require.Equal(t, 0, backend.connScore)
			require.Equal(t, 10, backend.connList.Len())
		},
		func() {
			// After redirection fails, the connections are moved back to the unhealthy backends.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(10)
			tester.checkBackendNum(2)
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
	// Disable health check so that it just reads BackendFetcher.
	healthCheckConfig := &config.HealthCheck{
		Enable:   false,
		Interval: 10 * time.Millisecond,
	}
	fetcher := newMockBackendFetcher()
	lg, _ := logger.CreateLoggerForTest(t)
	hc := NewDefaultHealthCheck(nil, healthCheckConfig, lg)
	router := NewScoreBasedRouter(lg, false)
	err := router.Init(fetcher, hc, healthCheckConfig)
	require.NoError(t, err)

	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Create 3 backends and change their status randomly.
	fetcher.setBackend("0", &BackendInfo{})
	fetcher.setBackend("1", &BackendInfo{})
	fetcher.setBackend("2", &BackendInfo{})
	wg.Run(func() {
		for {
			waitTime := rand.Intn(20) + 10
			select {
			case <-time.After(time.Duration(waitTime) * time.Millisecond):
			case <-ctx.Done():
				return
			}
			idx := rand.Intn(3)
			addr := strconv.Itoa(idx)
			backends, err := fetcher.GetBackendList(context.Background(), false)
			require.NoError(t, err)
			if _, ok := backends[addr]; ok {
				fetcher.removeBackend(addr)
			} else {
				fetcher.setBackend(addr, &BackendInfo{})
			}
		}
	})

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
						conn = newMockRedirectableConn(t, connID)
						selector := router.GetBackendSelector()
						backend, err := selector.Next()
						if err == ErrNoBackend {
							conn = nil
							continue
						}
						require.NoError(t, err)
						selector.Finish(conn, true)
						conn.from = backend
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
	fetcher := newMockBackendFetcher()
	hc := newMockHealthCheck()
	lg, _ := logger.CreateLoggerForTest(t)
	// Create a router with a very long health check interval.
	rt := NewScoreBasedRouter(lg, false)
	cfg := config.NewDefaultHealthCheckConfig()
	cfg.Interval = time.Minute
	observer := StartBackendObserver(lg, rt, cfg, fetcher, hc)
	rt.Lock()
	rt.observer = observer
	rt.Unlock()
	defer rt.Close()
	// The initial backends are empty.
	selector := rt.GetBackendSelector()
	_, err := selector.Next()
	require.Equal(t, ErrNoBackend, err)
	// Create a new backend and add to the list.
	fetcher.setBackend("127.0.0.1:4000", &BackendInfo{})
	hc.setBackend("127.0.0.1:4000", &BackendHealth{Status: StatusHealthy})
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		_, err := selector.Next()
		return err == nil
	}, 3*time.Second, 100*time.Millisecond)
}

func TestObserveError(t *testing.T) {
	backends := make([]string, 0, 1)
	var observeError error
	var m sync.Mutex
	fetcher := NewExternalFetcher(func() ([]string, error) {
		m.Lock()
		defer m.Unlock()
		return backends, observeError
	})
	hc := newMockHealthCheck()
	// Create a router with a very short health check interval.
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg, false)
	err := rt.Init(fetcher, hc, newHealthCheckConfigForTest())
	require.NoError(t, err)
	defer rt.Close()
	// No backends.
	selector := rt.GetBackendSelector()
	_, err = selector.Next()
	require.Equal(t, ErrNoBackend, err)
	// Create a new backend and add to the list.
	hc.setBackend("127.0.0.1:4000", &BackendHealth{Status: StatusHealthy})
	m.Lock()
	backends = append(backends, "127.0.0.1:4000")
	m.Unlock()
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		_, err := selector.Next()
		return err == nil
	}, 3*time.Second, 100*time.Millisecond)
	// Mock an observe error.
	m.Lock()
	observeError = errors.New("mock observe error")
	m.Unlock()
	require.Eventually(t, func() bool {
		_, err = selector.Next()
		return err != nil && err != ErrNoBackend
	}, 3*time.Second, 100*time.Millisecond)
	// Clear the observe error.
	m.Lock()
	observeError = nil
	m.Unlock()
	require.Eventually(t, func() bool {
		_, err = selector.Next()
		return err == nil
	}, 3*time.Second, 100*time.Millisecond)
}

func TestDisableHealthCheck(t *testing.T) {
	fetcher := newMockBackendFetcher()
	fetcher.setBackend("127.0.0.1:4000", nil)
	healtchCheckConfig := &config.HealthCheck{Enable: false}
	// Create a router with a very short health check interval.
	lg, _ := logger.CreateLoggerForTest(t)
	hc := NewDefaultHealthCheck(nil, healtchCheckConfig, lg)
	rt := NewScoreBasedRouter(lg, false)
	err := rt.Init(fetcher, hc, healtchCheckConfig)
	require.NoError(t, err)
	defer rt.Close()
	// No backends and no error.
	selector := rt.GetBackendSelector()
	// The backends are refreshed very soon.
	require.Eventually(t, func() bool {
		backend, err := selector.Next()
		require.NoError(t, err)
		return backend.Addr() == "127.0.0.1:4000"
	}, 3*time.Second, 100*time.Millisecond)
	// Replace the backend.
	fetcher.setBackend("127.0.0.1:5000", nil)
	require.Eventually(t, func() bool {
		backend, err := selector.Next()
		require.NoError(t, err)
		return backend.Addr() == "127.0.0.1:5000"
	}, 3*time.Second, 100*time.Millisecond)
}

func TestSetBackendStatus(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	for _, conn := range tester.conns {
		require.False(t, conn.from.Healthy())
	}
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).addr, StatusHealthy)
	for _, conn := range tester.conns {
		require.True(t, conn.from.Healthy())
	}
}

func TestGetServerVersion(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg, false)
	t.Cleanup(rt.Close)
	backends := map[string]*BackendHealth{
		"0": {
			Status:        StatusHealthy,
			ServerVersion: "1.0",
		},
		"1": {
			Status:        StatusHealthy,
			ServerVersion: "2.0",
		},
	}
	rt.OnBackendChanged(backends, nil)
	version := rt.ServerVersion()
	require.True(t, version == "1.0" || version == "2.0")
}

func TestBackendHealthy(t *testing.T) {
	// Make the connection redirect.
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(1)
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance(1)

	// The target backend becomes unhealthy during redirection.
	conn := tester.conns[1]
	require.True(t, conn.to.Healthy())
	tester.killBackends(1)
	require.False(t, conn.to.Healthy())
	tester.redirectFinish(1, false)
}

func TestCloseRedirectingConns(t *testing.T) {
	// Make the connection redirect.
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(1)
	require.Equal(t, 1, tester.getBackendByIndex(0).connScore)
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance(1)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 1, tester.getBackendByIndex(1).connScore)
	// Close the connection.
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).Addr(), StatusHealthy)
	tester.closeConnections(1, true)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(1).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(0).connList.Len())
	require.Equal(t, 0, tester.getBackendByIndex(1).connList.Len())
}
