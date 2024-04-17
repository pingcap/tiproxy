// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/stretchr/testify/require"
)

type routerTester struct {
	t         *testing.T
	router    *ScoreBasedRouter
	connID    uint64
	backendID int
	backends  map[string]*observer.BackendHealth
	conns     map[uint64]*mockRedirectableConn
}

func newRouterTester(t *testing.T) *routerTester {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
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
	for i := 0; i < num; i++ {
		tester.backendID++
		addr := strconv.Itoa(tester.backendID)
		tester.backends[addr] = &observer.BackendHealth{
			Status: observer.StatusHealthy,
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
		if health.Status == observer.StatusCannotConnect {
			continue
		}
		health.Status = observer.StatusCannotConnect
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

func (tester *routerTester) updateBackendStatusByAddr(addr string, status observer.BackendStatus) {
	health, ok := tester.backends[addr]
	if ok {
		health.Status = status
	} else {
		tester.backends[addr] = &observer.BackendHealth{
			Status: status,
		}
	}
	tester.notifyHealth()
}

func (tester *routerTester) getBackendByIndex(index int) *backendWrapper {
	addr := strconv.Itoa(index + 1)
	backend := tester.router.backends[addr]
	require.NotNil(tester.t, backend)
	return backend
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
}

func (tester *routerTester) rebalance() {
	tester.router.rebalance()
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
}

func (tester *routerTester) checkBackendConnMetrics() {
	for addr, backend := range tester.router.backends {
		val, err := readBackendConnMetrics(addr)
		require.NoError(tester.t, err)
		require.Equal(tester.t, backend.ConnCount(), val)
	}
}

func (tester *routerTester) clear() {
	tester.conns = make(map[uint64]*mockRedirectableConn)
	tester.router.backends = make(map[string]*backendWrapper)
	tester.backends = make(map[string]*observer.BackendHealth)
}

func TestRouteWithOneFactor(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	factor := &mockFactor{}
	tester.router.factors = []Factor{factor}

	// return no backends
	factor.route = func(backends []*backendWrapper) []*backendWrapper {
		return nil
	}
	selector := tester.router.GetBackendSelector()
	_, err := selector.Next()
	require.ErrorIs(t, err, ErrNoBackend)

	// return one backend
	var addr string
	factor.route = func(backends []*backendWrapper) []*backendWrapper {
		addr = backends[0].addr
		return backends[:1]
	}
	selector = tester.router.GetBackendSelector()
	backend, err := selector.Next()
	require.NoError(t, err)
	require.NotNil(t, backend)
	require.Equal(t, addr, backend.Addr())

	// return multiple backends
	factor.route = func(backends []*backendWrapper) []*backendWrapper {
		return backends
	}
	selector = tester.router.GetBackendSelector()
	insts := make(map[string]BackendInst)
	for i := 0; i < 3; i++ {
		backend, err = selector.Next()
		require.NoError(t, err)
		require.NotNil(t, backend)
		_, ok := insts[backend.Addr()]
		require.False(t, ok)
		insts[backend.Addr()] = backend
	}
}

func TestMultipleFactors(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	factor1, factor2 := &mockFactor{}, &mockFactor{}
	tester.router.factors = []Factor{factor1, factor2}
	returnEmpty := func(backends []*backendWrapper) []*backendWrapper {
		return nil
	}
	var addr string
	returnOne := func(backends []*backendWrapper) []*backendWrapper {
		addr = backends[0].addr
		return backends[:1]
	}
	returnAll := func(backends []*backendWrapper) []*backendWrapper {
		return backends
	}

	expectEmpty := func() {
		selector := tester.router.GetBackendSelector()
		_, err := selector.Next()
		require.ErrorIs(t, err, ErrNoBackend)
	}
	expectOne := func() {
		selector := tester.router.GetBackendSelector()
		backend, err := selector.Next()
		require.NoError(t, err)
		require.NotNil(t, backend)
		require.Equal(t, addr, backend.Addr())
	}
	expectAll := func() {
		selector := tester.router.GetBackendSelector()
		insts := make(map[string]BackendInst)
		for i := 0; i < 3; i++ {
			backend, err := selector.Next()
			require.NoError(t, err)
			require.NotNil(t, backend)
			_, ok := insts[backend.Addr()]
			require.False(t, ok)
			insts[backend.Addr()] = backend
		}
	}
	// empty + any
	factor1.route = returnEmpty
	expectEmpty()

	// 1 backend + any
	factor1.route = returnOne
	expectOne()

	// 3 backends + empty
	factor1.route = returnAll
	factor2.route = returnEmpty
	expectEmpty()

	// 3 backends + 1 backend
	factor2.route = returnOne
	expectOne()

	// 3 backends + 3 backends
	factor2.route = returnAll
	expectAll()
}

func TestBalance(t *testing.T) {

}

// Test that the backends returned by the BackendSelector are complete and different.
func TestSelectorReturnOrder(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
	tester.router.factors = []Factor{
		&mockFactor{
			route: func(backends []*backendWrapper) []*backendWrapper {
				return backends
			},
		},
	}
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
	tester.router.createFactors()
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

// Test all kinds of events occur concurrently.
func TestConcurrency(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	router.Init(context.Background(), bo)
	t.Cleanup(bo.Close)
	t.Cleanup(router.Close)

	var wg waitgroup.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Create 3 backends and change their status randomly.
	bo.addBackend("0")
	bo.addBackend("1")
	bo.addBackend("2")
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
}

// Test that the backends are refreshed immediately after it's empty.
func TestRefresh(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	rt.Init(context.Background(), bo)
	t.Cleanup(bo.Close)
	t.Cleanup(rt.Close)
	// The initial backends are empty.
	selector := rt.GetBackendSelector()
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
	rt.Init(context.Background(), bo)
	t.Cleanup(rt.Close)
	t.Cleanup(bo.Close)
	// Mock an observe error.
	bo.notify(errors.New("mock observe error"))
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector()
		_, err := selector.Next()
		return err != nil && err != ErrNoBackend
	}, 3*time.Second, 10*time.Millisecond)
	// Clear the observe error.
	bo.addBackend("0")
	bo.notify(nil)
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector()
		_, err := selector.Next()
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSetBackendStatus(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	for _, conn := range tester.conns {
		require.False(t, conn.from.Healthy())
	}
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).addr, observer.StatusHealthy)
	for _, conn := range tester.conns {
		require.True(t, conn.from.Healthy())
	}
}

func TestGetServerVersion(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	t.Cleanup(rt.Close)
	backends := map[string]*observer.BackendHealth{
		"0": {
			Status:        observer.StatusHealthy,
			ServerVersion: "1.0",
		},
		"1": {
			Status:        observer.StatusHealthy,
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
	tester := newRouterTester(t)
	tester.addBackends(1)
	tester.addConnections(1)
	tester.killBackends(1)
	tester.addBackends(1)
	tester.rebalance()

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
	tester.rebalance()
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 1, tester.getBackendByIndex(1).connScore)
	// Close the connection.
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).Addr(), observer.StatusHealthy)
	tester.closeConnections(1, true)
	require.Equal(t, 0, tester.getBackendByIndex(0).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(1).connScore)
	require.Equal(t, 0, tester.getBackendByIndex(0).connList.Len())
	require.Equal(t, 0, tester.getBackendByIndex(1).connList.Len())
}

func TestUpdateBackendHealth(t *testing.T) {
	tester := newRouterTester(t)
	tester.addBackends(3)
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
