// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"net"
	"reflect"
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

const (
	_routerKey = "__tiproxy_router"
)

var _ Router = &ScoreBasedRouter{}

// ScoreBasedRouter is an implementation of Router interface.
// It routes a connection based on score.
type ScoreBasedRouter struct {
	sync.Mutex
	logger     *zap.Logger
	policy     policy.BalancePolicy
	observer   observer.BackendObserver
	healthCh   <-chan observer.HealthResult
	cfgCh      <-chan *config.Config
	cancelFunc context.CancelFunc
	wg         waitgroup.WaitGroup
	// A list of *backendWrapper. The backends are in descending order of scores.
	backends     map[string]*backendWrapper
	observeError error
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
	// To limit the speed of redirection.
	lastRedirectTime time.Time
	// The backend supports redirection only when they have signing certs.
	supportRedirection bool
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:   logger,
		backends: make(map[string]*backendWrapper),
	}
}

func (r *ScoreBasedRouter) Init(ctx context.Context, ob observer.BackendObserver, balancePolicy policy.BalancePolicy, cfg *config.Config, cfgCh <-chan *config.Config) {
	r.observer = ob
	r.healthCh = r.observer.Subscribe("score_based_router")
	r.policy = balancePolicy
	balancePolicy.Init(cfg)
	childCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	r.cfgCh = cfgCh
	// Failing to rebalance backends may cause even more serious problems than TiProxy reboot, so we don't recover panics.
	r.wg.Run(func() {
		r.rebalanceLoop(childCtx)
	})
}

// GetBackendSelector implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) GetBackendSelector() BackendSelector {
	return BackendSelector{
		routeOnce: router.routeOnce,
		onCreate:  router.onCreateConn,
	}
}

func (router *ScoreBasedRouter) HealthyBackendCount() int {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return 0
	}

	count := 0
	for _, backend := range router.backends {
		if backend.Healthy() {
			count++
		}
	}
	return count
}

func (router *ScoreBasedRouter) getConnWrapper(conn RedirectableConn) *glist.Element[*connWrapper] {
	return conn.Value(_routerKey).(*glist.Element[*connWrapper])
}

func (router *ScoreBasedRouter) setConnWrapper(conn RedirectableConn, ce *glist.Element[*connWrapper]) {
	conn.SetValue(_routerKey, ce)
}

func (router *ScoreBasedRouter) routeOnce(excluded []BackendInst) (BackendInst, error) {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return nil, router.observeError
	}

	backends := make([]policy.BackendCtx, 0, len(router.backends))
	for _, backend := range router.backends {
		if !backend.Healthy() {
			continue
		}
		// Exclude the backends that are already tried.
		found := false
		for _, e := range excluded {
			if backend.Addr() == e.Addr() {
				found = true
				break
			}
		}
		if found {
			continue
		}
		backends = append(backends, backend)
	}

	idlestBackend := router.policy.BackendToRoute(backends)
	if idlestBackend == nil || reflect.ValueOf(idlestBackend).IsNil() {
		// No available backends, maybe the health check result is outdated during rolling restart.
		// Refresh the backends asynchronously in this case.
		if router.observer != nil {
			router.observer.Refresh()
		}
		return nil, ErrNoBackend
	}
	backend := idlestBackend.(*backendWrapper)
	backend.connScore++
	return backend, nil
}

func (router *ScoreBasedRouter) onCreateConn(backendInst BackendInst, conn RedirectableConn, succeed bool) {
	router.Lock()
	defer router.Unlock()
	backend := router.ensureBackend(backendInst.Addr())
	if succeed {
		connWrapper := &connWrapper{
			RedirectableConn: conn,
			phase:            phaseNotRedirected,
		}
		router.addConn(backend, connWrapper)
		conn.SetEventReceiver(router)
	} else {
		backend.connScore--
	}
}

func (router *ScoreBasedRouter) removeConn(backend *backendWrapper, ce *glist.Element[*connWrapper]) {
	backend.connList.Remove(ce)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
	router.removeBackendIfEmpty(backend)
}

func (router *ScoreBasedRouter) addConn(backend *backendWrapper, conn *connWrapper) {
	ce := backend.connList.PushBack(conn)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
	router.setConnWrapper(conn, ce)
}

// RefreshBackend implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) RefreshBackend() {
	router.observer.Refresh()
}

// RedirectConnections implements Router.RedirectConnections interface.
// It redirects all connections compulsively. It's only used for testing.
func (router *ScoreBasedRouter) RedirectConnections() error {
	router.Lock()
	defer router.Unlock()
	for _, backend := range router.backends {
		for ce := backend.connList.Front(); ce != nil; ce = ce.Next() {
			// This is only for test, so we allow it to reconnect to the same backend.
			connWrapper := ce.Value
			if connWrapper.phase != phaseRedirectNotify {
				connWrapper.phase = phaseRedirectNotify
				connWrapper.redirectReason = "test"
				// Ignore the results.
				if connWrapper.Redirect(backend) {
					metrics.PendingMigrateGuage.WithLabelValues(backend.addr, backend.addr, connWrapper.redirectReason).Inc()
				}
			}
		}
	}
	return nil
}

func (router *ScoreBasedRouter) ensureBackend(addr string) *backendWrapper {
	backend, ok := router.backends[addr]
	if ok {
		return backend
	}
	// The backend should always exist if it will be needed. Add a warning and add it back.
	router.logger.Warn("backend is not found in the router", zap.String("backend_addr", addr), zap.Stack("stack"))
	ip, _, _ := net.SplitHostPort(addr)
	backend = newBackendWrapper(addr, observer.BackendHealth{
		BackendInfo: observer.BackendInfo{
			IP:         ip,
			StatusPort: 10080, // impossible anyway
		},
		SupportRedirection: true,
		Healthy:            false,
	}, router.logger)
	router.backends[addr] = backend
	return backend
}

// OnRedirectSucceed implements ConnEventReceiver.OnRedirectSucceed interface.
func (router *ScoreBasedRouter) OnRedirectSucceed(from, to string, conn RedirectableConn) error {
	router.onRedirectFinished(from, to, conn, true)
	return nil
}

// OnRedirectFail implements ConnEventReceiver.OnRedirectFail interface.
func (router *ScoreBasedRouter) OnRedirectFail(from, to string, conn RedirectableConn) error {
	router.onRedirectFinished(from, to, conn, false)
	return nil
}

func (router *ScoreBasedRouter) onRedirectFinished(from, to string, conn RedirectableConn, succeed bool) {
	router.Lock()
	defer router.Unlock()
	fromBackend := router.ensureBackend(from)
	toBackend := router.ensureBackend(to)
	connWrapper := router.getConnWrapper(conn).Value
	addMigrateMetrics(from, to, connWrapper.redirectReason, succeed, connWrapper.lastRedirect)
	// The connection may be closed when this function is waiting for the lock.
	if connWrapper.phase == phaseClosed {
		return
	}

	if succeed {
		router.removeConn(fromBackend, router.getConnWrapper(conn))
		router.addConn(toBackend, connWrapper)
		connWrapper.phase = phaseRedirectEnd
	} else {
		fromBackend.connScore++
		toBackend.connScore--
		router.removeBackendIfEmpty(toBackend)
		connWrapper.phase = phaseRedirectFail
	}
}

// OnConnClosed implements ConnEventReceiver.OnConnClosed interface.
func (router *ScoreBasedRouter) OnConnClosed(addr, redirectingAddr string, conn RedirectableConn) error {
	router.Lock()
	defer router.Unlock()
	backend := router.ensureBackend(addr)
	connWrapper := router.getConnWrapper(conn)
	// If this connection has not redirected yet, decrease the score of the target backend.
	if redirectingAddr != "" {
		redirectingBackend := router.ensureBackend(redirectingAddr)
		redirectingBackend.connScore--
		router.removeBackendIfEmpty(redirectingBackend)
		metrics.PendingMigrateGuage.WithLabelValues(addr, redirectingAddr, connWrapper.Value.redirectReason).Dec()
	} else {
		backend.connScore--
	}
	router.removeConn(backend, connWrapper)
	connWrapper.Value.phase = phaseClosed
	return nil
}

func (router *ScoreBasedRouter) updateBackendHealth(healthResults observer.HealthResult) {
	router.Lock()
	defer router.Unlock()
	router.observeError = healthResults.Error()
	if router.observeError != nil {
		return
	}

	// `backends` contain all the backends, not only the updated ones.
	backends := healthResults.Backends()
	// If some backends are removed from the list, add them to `backends`.
	for addr, backend := range router.backends {
		if _, ok := backends[addr]; !ok {
			backends[addr] = &observer.BackendHealth{
				BackendInfo:        backend.GetBackendInfo(),
				SupportRedirection: backend.SupportRedirection(),
				Healthy:            false,
				PingErr:            errors.New("removed from backend list"),
			}
		}
	}
	var serverVersion string
	supportRedirection := true
	for addr, health := range backends {
		backend, ok := router.backends[addr]
		if !ok && health.Healthy {
			router.backends[addr] = newBackendWrapper(addr, *health, router.logger)
			serverVersion = health.ServerVersion
		} else if ok {
			backend.setHealth(*health)
			router.removeBackendIfEmpty(backend)
			if health.Healthy {
				serverVersion = health.ServerVersion
			}
		}
		supportRedirection = health.SupportRedirection && supportRedirection
	}
	if len(serverVersion) > 0 {
		router.serverVersion = serverVersion
	}
	if router.supportRedirection != supportRedirection {
		router.logger.Info("updated supporting redirection", zap.Bool("support", supportRedirection))
		router.supportRedirection = supportRedirection
	}
}

func (router *ScoreBasedRouter) rebalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(rebalanceInterval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case healthResults := <-router.healthCh:
			router.updateBackendHealth(healthResults)
		case cfg := <-router.cfgCh:
			router.policy.SetConfig(cfg)
		case <-ticker.C:
			router.rebalance(ctx)
		}
	}
}

// Rebalance every a short time and migrate only a few connections in each round so that:
// - The lock is not held too long
// - The connections are migrated to different backends
func (router *ScoreBasedRouter) rebalance(ctx context.Context) {
	router.Lock()
	defer router.Unlock()

	if !router.supportRedirection {
		return
	}
	if len(router.backends) <= 1 {
		return
	}
	backends := make([]policy.BackendCtx, 0, len(router.backends))
	for _, backend := range router.backends {
		backends = append(backends, backend)
	}

	busiestBackend, idlestBackend, balanceCount, reason, logFields := router.policy.BackendsToBalance(backends)
	if balanceCount == 0 {
		return
	}
	fromBackend, toBackend := busiestBackend.(*backendWrapper), idlestBackend.(*backendWrapper)

	// Control the speed of migration.
	curTime := time.Now()
	migrationInterval := time.Duration(float64(time.Second) / balanceCount)
	count := 0
	if migrationInterval < rebalanceInterval*2 {
		// If we need to migrate multiple connections in each round, calculate the connection count for each round.
		count = int((rebalanceInterval-1)/migrationInterval) + 1
	} else {
		// If we need to wait for multiple rounds to migrate a connection, calculate the interval for each connection.
		if curTime.Sub(router.lastRedirectTime) >= migrationInterval {
			count = 1
		} else {
			return
		}
	}
	// Migrate balanceCount connections.
	for i := 0; i < count && ctx.Err() == nil; i++ {
		var ce *glist.Element[*connWrapper]
		for ele := fromBackend.connList.Front(); ele != nil; ele = ele.Next() {
			conn := ele.Value
			switch conn.phase {
			case phaseRedirectNotify:
				// A connection cannot be redirected again when it has not finished redirecting.
				continue
			case phaseRedirectFail:
				// If it failed recently, it will probably fail this time.
				if conn.lastRedirect.Add(redirectFailMinInterval).After(curTime) {
					continue
				}
			}
			ce = ele
			break
		}
		if ce == nil {
			break
		}
		router.redirectConn(ce.Value, fromBackend, toBackend, reason, logFields, curTime)
		router.lastRedirectTime = curTime
	}
}

func (router *ScoreBasedRouter) redirectConn(conn *connWrapper, fromBackend *backendWrapper, toBackend *backendWrapper,
	reason string, logFields []zap.Field, curTime time.Time) {
	// Skip the connection if it's closing.
	if conn.Redirect(toBackend) {
		fields := []zap.Field{
			zap.Uint64("connID", conn.ConnectionID()),
			zap.String("from", fromBackend.addr),
			zap.String("to", toBackend.addr),
		}
		fields = append(fields, logFields...)
		router.logger.Debug("begin redirect connection", fields...)
		fromBackend.connScore--
		router.removeBackendIfEmpty(fromBackend)
		toBackend.connScore++
		conn.phase = phaseRedirectNotify
		conn.redirectReason = reason
		metrics.PendingMigrateGuage.WithLabelValues(fromBackend.addr, toBackend.addr, reason).Inc()
	} else {
		// Avoid it to be redirected again immediately.
		conn.phase = phaseRedirectFail
	}
	conn.lastRedirect = curTime
}

func (router *ScoreBasedRouter) removeBackendIfEmpty(backend *backendWrapper) bool {
	// If connList.Len() == 0, there won't be any outgoing connections.
	// And if also connScore == 0, there won't be any incoming connections.
	if !backend.Healthy() && backend.connList.Len() == 0 && backend.connScore <= 0 {
		delete(router.backends, backend.addr)
		return true
	}
	return false
}

func (router *ScoreBasedRouter) ConnCount() int {
	router.Lock()
	defer router.Unlock()
	j := 0
	for _, backend := range router.backends {
		j += backend.connList.Len()
	}
	return j
}

func (router *ScoreBasedRouter) ServerVersion() string {
	router.Lock()
	version := router.serverVersion
	router.Unlock()
	return version
}

// Close implements Router.Close interface.
func (router *ScoreBasedRouter) Close() {
	if router.cancelFunc != nil {
		router.cancelFunc()
		router.cancelFunc = nil
	}
	router.wg.Wait()
	if router.observer != nil {
		router.observer.Unsubscribe("score_based_router")
	}
	// Router only refers to RedirectableConn, it doesn't manage RedirectableConn.
}
