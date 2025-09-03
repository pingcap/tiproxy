// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var simpleBpCreator = func(_ *zap.Logger) policy.BalancePolicy {
	return policy.NewSimpleBalancePolicy()
}

type routerTester struct {
	t         *testing.T
	router    *ScoreBasedRouter
	connID    uint64
	backendID int
	backends  map[string]*observer.BackendHealth
	conns     map[uint64]*mockRedirectableConn
}

func newRouterTester(t *testing.T, bp policy.BalancePolicy) *routerTester {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	router.bpCreator = func(_ *zap.Logger) policy.BalancePolicy {
		if bp == nil {
			bp = policy.NewSimpleBalancePolicy()
			bp.Init(nil)
		}
		return bp
	}
	t.Cleanup(router.Close)
	return &routerTester{
		t:        t,
		router:   router,
		backends: make(map[string]*observer.BackendHealth),
		conns:    make(map[uint64]*mockRedirectableConn),
	}
}

func (tester *routerTester) createConn() *mockRedirectableConn {
	tester.connID++
	return newMockRedirectableConn(tester.t, tester.connID)
}

func (tester *routerTester) notifyHealth() {
	result := observer.NewHealthResult(tester.backends, nil)
	tester.router.updateBackendHealth(result)
}

func (tester *routerTester) addBackends(num int) {
	for range num {
		tester.backendID++
		addr := strconv.Itoa(tester.backendID)
		tester.backends[addr] = &observer.BackendHealth{
			Healthy:            true,
			SupportRedirection: true,
		}
		metrics.BackendConnGauge.WithLabelValues(addr).Set(0)
	}
	tester.notifyHealth()
}

func (tester *routerTester) killBackends(num int) {
	killed := 0
	for _, health := range tester.backends {
		if killed >= num {
			break
		}
		if !health.Healthy {
			continue
		}
		health.Healthy = false
		killed++
	}
	tester.notifyHealth()
}

func (tester *routerTester) removeBackends(num int) {
	backends := make([]string, 0, num)
	for addr := range tester.backends {
		if len(backends) >= num {
			break
		}
		backends = append(backends, addr)
	}
	for _, addr := range backends {
		delete(tester.backends, addr)
	}
	tester.notifyHealth()
}

func (tester *routerTester) updateBackendStatusByAddr(addr string, healthy bool) {
	health, ok := tester.backends[addr]
	if ok {
		health.Healthy = healthy
	} else {
		tester.backends[addr] = &observer.BackendHealth{
			SupportRedirection: true,
			Healthy:            healthy,
		}
	}
	tester.notifyHealth()
}

func (tester *routerTester) updateBackendLocalityByAddr(addr string, local bool) {
	health, ok := tester.backends[addr]
	require.True(tester.t, ok)
	health.Local = local
	tester.notifyHealth()
}

func (tester *routerTester) getBackendByIndex(index int) *backendWrapper {
	addr := strconv.Itoa(index + 1)
	backend := tester.router.backends[addr]
	require.NotNil(tester.t, backend)
	return backend
}

func (tester *routerTester) simpleRoute(conn RedirectableConn) BackendInst {
	selector := tester.router.GetBackendSelector(ClientInfo{})
	backend, err := selector.Next()
	if err != ErrNoBackend {
		require.NoError(tester.t, err)
		selector.Finish(conn, true)
	}
	return backend
}

func (tester *routerTester) addConnections(num int) {
	for range num {
		conn := tester.createConn()
		backend := tester.simpleRoute(conn)
		require.False(tester.t, backend == nil || reflect.ValueOf(backend).IsNil())
		conn.from = backend
		tester.conns[conn.connID] = conn
	}
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
		err := tester.router.groups[0].OnConnClosed(conn.from.Addr(), conn.GetRedirectingAddr(), conn)
		require.NoError(tester.t, err)
		delete(tester.conns, conn.connID)
	}
}

func (tester *routerTester) rebalance(num int) {
	for range num {
		if len(tester.router.groups) > 0 {
			tester.router.groups[0].lastRedirectTime = time.Time{}
		}
		tester.router.rebalance(context.Background())
	}
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
			err = tester.router.groups[0].OnRedirectSucceed(from.Addr(), to.Addr(), conn)
			require.NoError(tester.t, err)
			conn.redirectSucceed()
		} else {
			err = tester.router.groups[0].OnRedirectFail(from.Addr(), to.Addr(), conn)
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
}

func (tester *routerTester) checkBalanced() {
	maxNum, minNum := 0, math.MaxInt
	for _, backend := range tester.router.backends {
		if !backend.Healthy() {
			continue
		}
		curScore := backend.connScore
		if curScore > maxNum {
			maxNum = curScore
		}
		if curScore < minNum {
			minNum = curScore
		}
	}
	ratio := float64(maxNum) / float64(minNum+1)
	require.LessOrEqual(tester.t, ratio, policy.ConnBalancedRatio)
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
	require.Equal(tester.t, num, len(tester.router.backends))
	require.Equal(tester.t, num, len(tester.router.groups[0].backends))
}

func (tester *routerTester) checkBackendConnMetrics() {
	for addr, backend := range tester.router.backends {
		val, err := readBackendConnMetrics(addr)
		require.NoError(tester.t, err)
		require.Equal(tester.t, backend.ConnCount(), val)
	}
	require.EqualValues(tester.t, len(tester.conns), tester.router.ConnCount())
}

func (tester *routerTester) clear() {
	tester.backendID = 0
	tester.connID = 0
	tester.conns = make(map[uint64]*mockRedirectableConn)
	tester.router.groups = nil
	tester.router.backends = make(map[string]*backendWrapper)
	tester.backends = make(map[string]*observer.BackendHealth)
}

func TestRebalance(t *testing.T) {
	tester := newRouterTester(t, nil)
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
	tester := newRouterTester(t, nil)
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
	tester := newRouterTester(t, nil)
	conn := tester.createConn()
	backend := tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
	require.Equal(t, 0, tester.router.HealthyBackendCount())
	tester.addBackends(1)
	require.Equal(t, 1, tester.router.HealthyBackendCount())
	tester.addConnections(10)
	tester.killBackends(1)
	backend = tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
	require.Equal(t, 0, tester.router.HealthyBackendCount())
}

// Test that the backends returned by the BackendSelector are complete and different.
func TestSelectorReturnOrder(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	selector := tester.router.GetBackendSelector(ClientInfo{})
	for range 3 {
		addrs := make(map[string]struct{}, 3)
		for range 3 {
			backend, err := selector.Next()
			require.NoError(t, err)
			addrs[backend.Addr()] = struct{}{}
		}
		// All 3 addresses are different.
		require.Equal(t, 3, len(addrs))
	}

	tester.killBackends(1)
	for range 2 {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err := selector.Next()
	require.NoError(t, err)

	tester.addBackends(1)
	for range 3 {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err = selector.Next()
	require.NoError(t, err)
}

// Test that the backends are balanced even when routing are concurrent.
func TestRouteConcurrently(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	addrs := make(map[string]int, 3)
	selectors := make([]BackendSelector, 0, 30)
	// All the clients are calling Next() but not yet Finish().
	for range 30 {
		selector := tester.router.GetBackendSelector(ClientInfo{})
		backend, err := selector.Next()
		require.NoError(t, err)
		addrs[backend.Addr()]++
		selectors = append(selectors, selector)
	}
	require.Equal(t, 3, len(addrs))
	for _, num := range addrs {
		require.Equal(t, 10, num)
	}
	for i := range 3 {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 10, backend.connScore)
	}
	for _, selector := range selectors {
		selector.Finish(nil, false)
	}
	for i := range 3 {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 0, backend.connScore)
	}
}

// Test that the backends are balanced during rolling restart.
func TestRollingRestart(t *testing.T) {
	tester := newRouterTester(t, nil)
	backendNum := 3
	tester.addBackends(backendNum)
	tester.addConnections(100)
	tester.checkBalanced()

	backendAddrs := make([]string, 0, backendNum)
	for i := range backendNum {
		backendAddrs = append(backendAddrs, tester.getBackendByIndex(i).addr)
	}

	for i := 0; i < backendNum+1; i++ {
		if i > 0 {
			tester.updateBackendStatusByAddr(backendAddrs[i-1], true)
			tester.rebalance(100)
			tester.redirectFinish(100, true)
			tester.checkBalanced()
		}
		if i < backendNum {
			tester.updateBackendStatusByAddr(backendAddrs[i], false)
			tester.rebalance(100)
			tester.redirectFinish(100, true)
			tester.checkBalanced()
		}
	}
}

// Test the corner cases of rebalance.
func TestRebalanceCornerCase(t *testing.T) {
	tester := newRouterTester(t, nil)
	tests := []func(){
		func() {
			// Balancer won't work when there's no backend.
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Balancer won't work when there's only one backend.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Router should have already balanced it.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.addBackends(1)
			tester.addConnections(10)
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// Balancer won't work when all the backends are unhealthy.
			tester.addBackends(2)
			tester.addConnections(20)
			tester.killBackends(2)
			tester.rebalance(1)
			tester.checkRedirectingNum(0)
		},
		func() {
			// The parameter limits the redirecting num.
			tester.addBackends(2)
			tester.addConnections(50)
			tester.killBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(10)
		},
		func() {
			// All the connections are redirected to the new healthy one and the unhealthy backends are removed.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(10)
			tester.checkBackendNum(2)
			backend := tester.getBackendByIndex(1)
			require.Equal(t, 10, backend.connScore)
			tester.redirectFinish(10, true)
		},
		func() {
			// Connections won't be redirected again before redirection finishes.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkRedirectingNum(10)
			backend := tester.getBackendByIndex(0)
			require.Equal(t, 0, backend.connScore)
			require.Equal(t, 10, backend.connList.Len())
			backend = tester.getBackendByIndex(1)
			require.Equal(t, 10, backend.connScore)
			require.Equal(t, 0, backend.connList.Len())
		},
		func() {
			// After redirection fails, the connections are moved back to the unhealthy backends.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(1)
			tester.checkBackendNum(2)
			tester.redirectFinish(10, false)
			tester.checkBackendNum(2)
		},
		func() {
			// It won't rebalance when there's no connection.
			tester.addBackends(1)
			tester.addConnections(10)
			tester.closeConnections(10, false)
			tester.rebalance(1)
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
			tester.rebalance(1)
			tester.redirectFinish(10, false)
			tester.killBackends(1)
			tester.addBackends(1)
			tester.rebalance(1)
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
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	cfgGetter := newMockConfigGetter(&config.Config{})
	router.Init(context.Background(), bo, simpleBpCreator, cfgGetter, make(<-chan *config.Config))
	t.Cleanup(bo.Close)
	t.Cleanup(router.Close)

	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Create 3 backends and change their status randomly.
	bo.addBackend("0", nil)
	bo.addBackend("1", nil)
	bo.addBackend("2", nil)
	bo.notify(nil)
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
			bo.toggleBackendHealth(addr)
			bo.notify(nil)
		}
	})

	// Create 20 connections.
	for i := range 20 {
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
						selector := router.GetBackendSelector(ClientInfo{})
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
							err = router.groups[0].OnConnClosed(from, conn.GetRedirectingAddr(), conn)
							conn = nil
						} else if i < 3 {
							conn.redirectFail()
							err = router.groups[0].OnRedirectFail(from, to, conn)
						} else {
							conn.redirectSucceed()
							err = router.groups[0].OnRedirectSucceed(from, to, conn)
						}
						require.NoError(t, err)
					} else {
						// not redirecting, 20% close
						i := rand.Intn(10)
						if i < 2 {
							// The balancer may happen to redirect it concurrently - that's exactly what may happen.
							from, _ := conn.getAddr()
							err := router.groups[0].OnConnClosed(from, conn.GetRedirectingAddr(), conn)
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
}

// Test that the backends are refreshed immediately after it's empty.
func TestRefresh(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	cfgGetter := newMockConfigGetter(&config.Config{})
	rt.Init(context.Background(), bo, simpleBpCreator, cfgGetter, make(<-chan *config.Config))
	t.Cleanup(bo.Close)
	t.Cleanup(rt.Close)
	// The initial backends are empty.
	selector := rt.GetBackendSelector(ClientInfo{})
	_, err := selector.Next()
	require.Equal(t, ErrNoBackend, err)
	// Refresh is called internally and there comes a new one.
	bo.notify(nil)
	require.Eventually(t, func() bool {
		_, err = selector.Next()
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestObserveError(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	cfgGetter := newMockConfigGetter(&config.Config{})
	rt.Init(context.Background(), bo, simpleBpCreator, cfgGetter, make(<-chan *config.Config))
	t.Cleanup(bo.Close)
	t.Cleanup(rt.Close)
	// Mock an observe error.
	bo.notify(errors.New("mock observe error"))
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector(ClientInfo{})
		_, err := selector.Next()
		return err != nil && err != ErrNoBackend
	}, 3*time.Second, 10*time.Millisecond)
	// Clear the observe error.
	bo.addBackend("0", nil)
	bo.notify(nil)
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector(ClientInfo{})
		_, err := selector.Next()
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSetBackendStatus(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	for _, conn := range tester.conns {
		require.False(t, conn.from.Healthy())
	}
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).addr, true)
	for _, conn := range tester.conns {
		require.True(t, conn.from.Healthy())
	}
}

func TestGetServerVersion(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	rt.bpCreator = simpleBpCreator
	t.Cleanup(rt.Close)
	backends := map[string]*observer.BackendHealth{
		"0": {
			Healthy:       true,
			ServerVersion: "1.0",
		},
		"1": {
			Healthy:       true,
			ServerVersion: "2.0",
		},
	}
	result := observer.NewHealthResult(backends, nil)
	rt.updateBackendHealth(result)
	version := rt.ServerVersion()
	require.True(t, version == "1.0" || version == "2.0")
}

func TestBackendHealthy(t *testing.T) {
	// Make the connection redirect.
	tester := newRouterTester(t, nil)
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
	tester := newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(2)
	require.Equal(t, 2, tester.getBackendByIndex(0).connScore)
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance(2)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 2, tester.getBackendByIndex(1).connScore)
	// Close the connection.
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).Addr(), true)
	tester.closeConnections(2, true)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(1).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(0).connList.Len())
	require.Equal(t, 0, tester.getBackendByIndex(1).connList.Len())
	// The results are received (after getting the lock) after the connections are closed.
	tester.redirectFinish(1, true)
	tester.redirectFinish(1, false)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(1).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(0).connList.Len())
	require.Equal(t, 0, tester.getBackendByIndex(1).connList.Len())
}

func TestUpdateBackendHealth(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	// Test locality of some backends are changed.
	tester.updateBackendLocalityByAddr(tester.getBackendByIndex(0).Addr(), false)
	tester.updateBackendLocalityByAddr(tester.getBackendByIndex(1).Addr(), true)
	require.Equal(t, false, tester.router.backends[tester.getBackendByIndex(0).Addr()].Local())
	require.Equal(t, true, tester.router.backends[tester.getBackendByIndex(1).Addr()].Local())
	// Test some backends are not in the list anymore.
	tester.removeBackends(1)
	tester.checkBackendNum(2)
	// Test some backends failed.
	tester.killBackends(1)
	tester.checkBackendNum(1)
	tester.addBackends(2)
	tester.checkBackendNum(3)
	// The backend won't be removed when there are connections on it.
	tester.addConnections(90)
	tester.killBackends(1)
	tester.checkBackendNum(3)
}

func TestWatchConfig(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	cfgCh := make(chan *config.Config)
	cfg := &config.Config{
		Labels: map[string]string{"k1": "v1"},
	}
	cfgGetter := newMockConfigGetter(cfg)
	p := &mockBalancePolicy{}
	bpCreator := func(_ *zap.Logger) policy.BalancePolicy {
		p.Init(cfg)
		return p
	}
	bo := newMockBackendObserver()
	router.Init(context.Background(), bo, bpCreator, cfgGetter, cfgCh)
	t.Cleanup(router.Close)

	bo.addBackend("0", nil)
	bo.notify(nil)
	require.Eventually(t, func() bool {
		return p.getConfig() != nil && p.getConfig().Labels["k1"] == "v1"
	}, 30*time.Second, 10*time.Millisecond)
	cfgCh <- &config.Config{
		Labels: map[string]string{"k1": "v2"},
	}
	require.Eventually(t, func() bool {
		return p.getConfig().Labels["k1"] == "v2"
	}, 3*time.Second, 10*time.Millisecond)
}

func TestControlSpeed(t *testing.T) {
	tests := []struct {
		balanceCount     float64
		rounds           int
		interval         time.Duration
		expectedCountMin int
		expectedCountMax int
	}{
		{
			balanceCount:     0,
			rounds:           2,
			interval:         time.Second,
			expectedCountMin: 0,
			expectedCountMax: 0,
		},
		{
			balanceCount:     1,
			rounds:           1,
			interval:         10 * time.Millisecond,
			expectedCountMin: 1,
			expectedCountMax: 2,
		},
		{
			balanceCount:     1,
			rounds:           100,
			interval:         100 * time.Millisecond,
			expectedCountMin: 9,
			expectedCountMax: 11,
		},
		{
			balanceCount:     10,
			rounds:           10,
			interval:         100 * time.Millisecond,
			expectedCountMin: 9,
			expectedCountMax: 20,
		},
		{
			balanceCount:     100,
			rounds:           10,
			expectedCountMin: 10,
			expectedCountMax: 10,
		},
		{
			balanceCount:     1000,
			rounds:           1,
			expectedCountMin: 10,
			expectedCountMax: 10,
		},
		{
			balanceCount:     1000,
			rounds:           10,
			expectedCountMin: 100,
			expectedCountMax: 100,
		},
		{
			balanceCount:     1.1,
			rounds:           1000,
			interval:         10 * time.Millisecond,
			expectedCountMin: 10,
			expectedCountMax: 20,
		},
		{
			balanceCount:     0.9,
			rounds:           1000,
			interval:         10 * time.Millisecond,
			expectedCountMin: 5,
			expectedCountMax: 20,
		},
		{
			balanceCount:     0.5,
			rounds:           1000,
			interval:         10 * time.Millisecond,
			expectedCountMin: 3,
			expectedCountMax: 7,
		},
		{
			balanceCount:     0.1,
			rounds:           1000,
			interval:         10 * time.Millisecond,
			expectedCountMin: 1,
			expectedCountMax: 2,
		},
	}

	bp := &mockBalancePolicy{}
	tester := newRouterTester(t, bp)
	tester.addBackends(2)
	bp.backendToRoute = func(bc []policy.BackendCtx) policy.BackendCtx {
		return tester.getBackendByIndex(0)
	}
	total := 2000
	tester.addConnections(total)
	for i, test := range tests {
		bp.backendsToBalance = func(bc []policy.BackendCtx) (from policy.BackendCtx, to policy.BackendCtx, balanceCount float64, reason string, logFields []zapcore.Field) {
			return tester.getBackendByIndex(0), tester.getBackendByIndex(1), test.balanceCount, "conn", nil
		}
		tester.router.groups[0].lastRedirectTime = time.Time{}
		require.Equal(t, total, tester.getBackendByIndex(0).connScore, "case %d", i)
		for j := 0; j < test.rounds; j++ {
			tester.router.rebalance(context.Background())
			tester.router.groups[0].lastRedirectTime = tester.router.groups[0].lastRedirectTime.Add(-test.interval)
		}
		redirectingNum := total - tester.getBackendByIndex(0).connScore
		// Define a bound because the test may be slow.
		require.LessOrEqual(t, test.expectedCountMin, redirectingNum, "case %d", i)
		require.GreaterOrEqual(t, test.expectedCountMax, redirectingNum, "case %d", i)
		tester.redirectFinish(redirectingNum, false)
	}
}

func TestRedirectFail(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(1)
	tester.conns[1].closing = true
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance(1)
	// If the connection refuses to redirect, the connScore should not change.
	require.Equal(t, 1, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(1).connScore)

	tester = newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(2)
	tester.conns[1].closing = true
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance(1)
	// Even if the first connection refuses to redirect, the second one should be redirected.
	require.Equal(t, 1, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 1, tester.getBackendByIndex(1).connScore)
}

func TestSkipRedirection(t *testing.T) {
	tester := newRouterTester(t, nil)
	backends := map[string]*observer.BackendHealth{
		"0": {
			Healthy:            true,
			SupportRedirection: false,
		},
		"1": {
			Healthy:            true,
			SupportRedirection: true,
		},
	}
	result := observer.NewHealthResult(backends, nil)
	tester.router.updateBackendHealth(result)
	require.False(t, tester.router.supportRedirection)

	tester.addConnections(10)
	require.Equal(t, 5, tester.getBackendByIndex(0).connScore)
	backends["0"].Healthy = false
	tester.router.updateBackendHealth(result)
	tester.rebalance(1)
	require.Equal(t, 5, tester.getBackendByIndex(0).connScore)

	backends["0"].SupportRedirection = true
	tester.router.updateBackendHealth(result)
	require.True(t, tester.router.supportRedirection)
	tester.rebalance(1)
	require.NotEqual(t, 5, tester.getBackendByIndex(0).connScore)
}

func TestGroupBackends(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	cfgCh := make(chan *config.Config)
	cfg := &config.Config{
		Balance: config.Balance{
			RoutingRule: MatchClientCIDRStr,
		},
	}
	cfgGetter := newMockConfigGetter(cfg)
	p := &mockBalancePolicy{}
	bpCreator := func(_ *zap.Logger) policy.BalancePolicy {
		p.Init(cfg)
		return p
	}
	bo := newMockBackendObserver()
	router.Init(context.Background(), bo, bpCreator, cfgGetter, cfgCh)
	t.Cleanup(bo.Close)
	t.Cleanup(router.Close)

	tests := []struct {
		addr         string
		labels       map[string]string
		groupCount   int
		backendCount int
		cidrs        []string
	}{
		{
			addr:         "0",
			labels:       nil,
			groupCount:   0,
			backendCount: 1,
			cidrs:        nil,
		},
		{
			addr:         "1",
			labels:       map[string]string{"cidr": "1.1.1.1/32"},
			groupCount:   1,
			backendCount: 2,
			cidrs:        []string{"1.1.1.1/32"},
		},
		{
			addr:         "2",
			labels:       map[string]string{"cidr": "1.1.1.1/32 , "},
			groupCount:   1,
			backendCount: 3,
			cidrs:        []string{"1.1.1.1/32"},
		},
		{
			addr:         "3",
			labels:       map[string]string{"cidr": "1.1.2.1/32, 1.1.3.1/32"},
			groupCount:   2,
			backendCount: 4,
			cidrs:        []string{"1.1.2.1/32", "1.1.3.1/32"},
		},
		{
			addr:         "4",
			labels:       map[string]string{"cidr": "1.1.2.1/32,, 1.1.3.1/32 "},
			groupCount:   2,
			backendCount: 5,
			cidrs:        []string{"1.1.2.1/32", "1.1.3.1/32"},
		},
		{
			addr:         "0",
			labels:       map[string]string{"cidr": " 1.1.1.1/32 "},
			groupCount:   2,
			backendCount: 5,
			cidrs:        []string{"1.1.1.1/32"},
		},
	}

	for i, test := range tests {
		bo.addBackend(test.addr, test.labels)
		bo.notify(nil)
		require.Eventually(t, func() bool {
			router.Lock()
			defer router.Unlock()
			if len(router.groups) != test.groupCount {
				return false
			}
			if len(router.backends) != test.backendCount {
				return false
			}
			group := router.backends[test.addr].group
			if test.cidrs == nil {
				return group == nil
			}
			return slices.Equal(test.cidrs, group.values)
		}, 3*time.Second, 10*time.Millisecond, "test %d", i)
	}
}
