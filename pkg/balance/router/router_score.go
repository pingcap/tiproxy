// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"sort"
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
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
	factors    []Factor
	observer   observer.BackendObserver
	healthCh   <-chan observer.HealthResult
	cancelFunc context.CancelFunc
	wg         waitgroup.WaitGroup
	// A list of *backendWrapper. The backends are in descending order of scores.
	backends     map[string]*backendWrapper
	observeError error
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:   logger,
		backends: make(map[string]*backendWrapper),
	}
}

func (r *ScoreBasedRouter) Init(ctx context.Context, ob observer.BackendObserver) {
	r.observer = ob
	r.healthCh = r.observer.Subscribe("score_based_router")
	r.createFactors()
	childCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	// Failing to rebalance backends may cause even more serious problems than TiProxy reboot, so we don't recover panics.
	r.wg.Run(func() {
		r.rebalanceLoop(childCtx)
	})
}

func (r *ScoreBasedRouter) createFactors() {
	r.factors = []Factor{
		NewFactorHealth(),
		NewFactorConnCount(),
	}
}

// GetBackendSelector implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) GetBackendSelector() BackendSelector {
	return BackendSelector{
		routeOnce: router.routeOnce,
		onCreate:  router.onCreateConn,
	}
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

	backends := make([]*backendWrapper, 0, len(router.backends))
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
		backend.clearScore()
		backends = append(backends, backend)
	}

	if len(backends) == 0 {
		// No available backends, maybe the health check result is outdated during rolling restart.
		// Refresh the backends asynchronously in this case.
		if router.observer != nil {
			router.observer.Refresh()
		}
		return nil, ErrNoBackend
	}

	if len(backends) == 1 {
		backends[0].connScore++
		return backends[0], nil
	}

	for _, factor := range router.factors {
		factor.UpdateScore(backends)
	}
	bestBackend := backends[0]
	minScore := bestBackend.score()
	for i := 1; i < len(backends); i++ {
		score := backends[i].score()
		if score < minScore {
			minScore = score
			bestBackend = backends[i]
		}
	}
	bestBackend.connScore++
	return bestBackend, nil
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
				// Ignore the results.
				_ = connWrapper.Redirect(backend)
				connWrapper.redirectingBackend = backend
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
	backend = newBackendWrapper(addr, observer.BackendHealth{
		Status: observer.StatusCannotConnect,
	})
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
	connWrapper.redirectingBackend = nil
	addMigrateMetrics(from, to, succeed, connWrapper.lastRedirect)
}

// OnConnClosed implements ConnEventReceiver.OnConnClosed interface.
func (router *ScoreBasedRouter) OnConnClosed(addr string, conn RedirectableConn) error {
	router.Lock()
	defer router.Unlock()
	backend := router.ensureBackend(addr)
	connWrapper := router.getConnWrapper(conn)
	redirectingBackend := connWrapper.Value.redirectingBackend
	// If this connection is redirecting, decrease the score of the target backend.
	if redirectingBackend != nil {
		redirectingBackend.connScore--
		connWrapper.Value.redirectingBackend = nil
		router.removeBackendIfEmpty(redirectingBackend)
	} else {
		backend.connScore--
	}
	router.removeConn(backend, connWrapper)
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
	for addr := range router.backends {
		if _, ok := backends[addr]; !ok {
			backends[addr] = &observer.BackendHealth{
				Status:  observer.StatusCannotConnect,
				PingErr: errors.New("removed from backend list"),
			}
		}
	}
	var serverVersion string
	for addr, health := range backends {
		backend, ok := router.backends[addr]
		if !ok && health.Status != observer.StatusCannotConnect {
			router.backends[addr] = newBackendWrapper(addr, *health)
			serverVersion = health.ServerVersion
		} else if ok {
			if !backend.Equals(*health) {
				backend.setHealth(*health)
				router.removeBackendIfEmpty(backend)
				if health.Status != observer.StatusCannotConnect {
					serverVersion = health.ServerVersion
				}
			}
		}
	}
	if len(serverVersion) > 0 {
		router.serverVersion = serverVersion
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
		case <-ticker.C:
			router.rebalance()
		}
	}
}

func (router *ScoreBasedRouter) rebalance() {
	curTime := monotime.Now()
	router.Lock()
	defer router.Unlock()

	backends := make([]*backendWrapper, 0, len(router.backends))
	for _, backend := range router.backends {
		backends = append(backends, backend)
		backend.clearScore()
	}
	if len(backends) <= 1 {
		return
	}

	totalBitNum := 0
	for _, factor := range router.factors {
		factor.UpdateScore(backends)
		totalBitNum += factor.ScoreBitNum()
	}
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].score() < backends[j].score()
	})
	idlest := backends[0]
	if !idlest.Healthy() {
		return
	}
	var busiest *backendWrapper
	// Skip the backends without connections.
	for i := len(backends) - 1; i >= 0; i-- {
		if backends[i].connScore > 0 {
			busiest = backends[i]
			break
		}
	}
	if busiest == idlest {
		return
	}

	fromScore, toScore := busiest.score(), idlest.score()
	var factor Factor
	var balanceCount int
	for _, factor = range router.factors {
		bitNum := factor.ScoreBitNum()
		score1 := fromScore << (64 - totalBitNum) >> (64 - bitNum)
		score2 := toScore << (64 - totalBitNum) >> (64 - bitNum)
		if score1 > score2 {
			balanceCount = factor.BalanceCount(busiest, idlest)
			if balanceCount > 0 {
				break
			}
		}
		totalBitNum -= bitNum
	}

	// Migrate connCount connections.
	for i := 0; i < balanceCount; i++ {
		var ce *glist.Element[*connWrapper]
		for ele := busiest.connList.Front(); ele != nil; ele = ele.Next() {
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

		// Migrate the connection.
		router.redirectConn(ce.Value, busiest, idlest, factor, curTime)
	}
}

func (router *ScoreBasedRouter) redirectConn(conn *connWrapper, fromBackend *backendWrapper, toBackend *backendWrapper,
	factor Factor, curTime monotime.Time) {
	router.logger.Debug("begin redirect connection", zap.Uint64("connID", conn.ConnectionID()),
		zap.String("from", fromBackend.addr), zap.String("to", toBackend.addr),
		zap.String("factor", factor.Name()), zap.Uint64("from_score", fromBackend.score()),
		zap.Uint64("to_score", toBackend.score()))
	fromBackend.connScore--
	router.removeBackendIfEmpty(fromBackend)
	toBackend.connScore++
	conn.phase = phaseRedirectNotify
	conn.lastRedirect = curTime
	conn.Redirect(toBackend)
	conn.redirectingBackend = toBackend
}

func (router *ScoreBasedRouter) removeBackendIfEmpty(backend *backendWrapper) bool {
	// If connList.Len() == 0, there won't be any outgoing connections.
	// And if also connScore == 0, there won't be any incoming connections.
	if backend.Status() == observer.StatusCannotConnect && backend.connList.Len() == 0 && backend.connScore <= 0 {
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
