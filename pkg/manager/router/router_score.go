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
	"context"
	"net/http"
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
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
	observer   *BackendObserver
	cancelFunc context.CancelFunc
	wg         waitgroup.WaitGroup
	// A list of *backendWrapper. The backends are in descending order of scores.
	backends     *glist.List[*backendWrapper]
	observeError error
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:   logger,
		backends: glist.New[*backendWrapper](),
	}
}

func (r *ScoreBasedRouter) Init(httpCli *http.Client, fetcher BackendFetcher, cfg *config.HealthCheck) error {
	cfg.Check()
	observer, err := StartBackendObserver(r.logger.Named("observer"), r, httpCli, cfg, fetcher)
	if err != nil {
		return err
	}
	r.observer = observer
	childCtx, cancelFunc := context.WithCancel(context.Background())
	r.cancelFunc = cancelFunc
	r.wg.Run(func() {
		r.rebalanceLoop(childCtx)
	})
	return nil
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

func (router *ScoreBasedRouter) routeOnce(excluded []string) (string, error) {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return "", router.observeError
	}
	for be := router.backends.Back(); be != nil; be = be.Prev() {
		backend := be.Value
		// These backends may be recycled, so we should not connect to them again.
		switch backend.status {
		case StatusCannotConnect, StatusSchemaOutdated:
			continue
		}
		found := false
		for _, ex := range excluded {
			if ex == backend.addr {
				found = true
				break
			}
		}
		if !found {
			backend.connScore++
			router.adjustBackendList(be)
			return backend.addr, nil
		}
	}
	// No available backends, maybe the health check result is outdated during rolling restart.
	// Refresh the backends asynchronously in this case.
	if router.observer != nil {
		router.observer.Refresh()
	}
	return "", nil
}

func (router *ScoreBasedRouter) onCreateConn(addr string, conn RedirectableConn, succeed bool) {
	router.Lock()
	defer router.Unlock()
	be := router.ensureBackend(addr, true)
	backend := be.Value
	if succeed {
		connWrapper := &connWrapper{
			RedirectableConn: conn,
			phase:            phaseNotRedirected,
		}
		router.addConn(be, connWrapper)
		conn.SetEventReceiver(router)
	} else {
		backend.connScore--
		router.adjustBackendList(be)
	}
}

func (router *ScoreBasedRouter) removeConn(be *glist.Element[*backendWrapper], ce *glist.Element[*connWrapper]) {
	backend := be.Value
	backend.connList.Remove(ce)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
	router.adjustBackendList(be)
}

func (router *ScoreBasedRouter) addConn(be *glist.Element[*backendWrapper], conn *connWrapper) {
	backend := be.Value
	ce := backend.connList.PushBack(conn)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
	router.setConnWrapper(conn, ce)
	conn.NotifyBackendStatus(backend.status)
	router.adjustBackendList(be)
}

// adjustBackendList moves `be` after the score of `be` changes to keep the list ordered.
func (router *ScoreBasedRouter) adjustBackendList(be *glist.Element[*backendWrapper]) {
	if router.removeBackendIfEmpty(be) {
		return
	}

	backend := be.Value
	curScore := backend.score()
	var mark *glist.Element[*backendWrapper]
	for ele := be.Prev(); ele != nil; ele = ele.Prev() {
		b := ele.Value
		if b.score() >= curScore {
			break
		}
		mark = ele
	}
	if mark != nil {
		router.backends.MoveBefore(be, mark)
		return
	}
	for ele := be.Next(); ele != nil; ele = ele.Next() {
		b := ele.Value
		if b.score() <= curScore {
			break
		}
		mark = ele
	}
	if mark != nil {
		router.backends.MoveAfter(be, mark)
	}
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
	for be := router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value
		for ce := backend.connList.Front(); ce != nil; ce = ce.Next() {
			// This is only for test, so we allow it to reconnect to the same backend.
			connWrapper := ce.Value
			if connWrapper.phase != phaseRedirectNotify {
				connWrapper.phase = phaseRedirectNotify
				// we dont care the results
				_ = connWrapper.Redirect(backend.addr)
			}
		}
	}
	return nil
}

// forward is a hint to speed up searching.
func (router *ScoreBasedRouter) lookupBackend(addr string, forward bool) *glist.Element[*backendWrapper] {
	if forward {
		for be := router.backends.Front(); be != nil; be = be.Next() {
			backend := be.Value
			if backend.addr == addr {
				return be
			}
		}
	} else {
		for be := router.backends.Back(); be != nil; be = be.Prev() {
			backend := be.Value
			if backend.addr == addr {
				return be
			}
		}
	}
	return nil
}

func (router *ScoreBasedRouter) ensureBackend(addr string, forward bool) *glist.Element[*backendWrapper] {
	be := router.lookupBackend(addr, forward)
	if be == nil {
		// The backend should always exist if it will be needed. Add a warning and add it back.
		router.logger.Warn("backend is not found in the router", zap.String("backend_addr", addr), zap.Stack("stack"))
		be = router.backends.PushFront(&backendWrapper{
			backendHealth: &backendHealth{
				status: StatusCannotConnect,
			},
			addr:     addr,
			connList: glist.New[*connWrapper](),
		})
		router.adjustBackendList(be)
	}
	return be
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
	fromBe := router.ensureBackend(from, true)
	toBe := router.ensureBackend(to, false)
	connWrapper := router.getConnWrapper(conn).Value
	if succeed {
		router.removeConn(fromBe, router.getConnWrapper(conn))
		router.addConn(toBe, connWrapper)
		connWrapper.phase = phaseRedirectEnd
	} else {
		fromBe.Value.connScore++
		router.adjustBackendList(fromBe)
		toBe.Value.connScore--
		router.adjustBackendList(toBe)
		connWrapper.phase = phaseRedirectFail
	}
	addMigrateMetrics(from, to, succeed, connWrapper.lastRedirect)
}

// OnConnClosed implements ConnEventReceiver.OnConnClosed interface.
func (router *ScoreBasedRouter) OnConnClosed(addr string, conn RedirectableConn) error {
	router.Lock()
	defer router.Unlock()
	// OnConnClosed is always called after processing ongoing redirect events,
	// so the addr passed in is the right backend.
	be := router.ensureBackend(addr, true)
	be.Value.connScore--
	router.removeConn(be, router.getConnWrapper(conn))
	return nil
}

// OnBackendChanged implements BackendEventReceiver.OnBackendChanged interface.
func (router *ScoreBasedRouter) OnBackendChanged(backends map[string]*backendHealth, err error) {
	router.Lock()
	defer router.Unlock()
	router.observeError = err
	for addr, health := range backends {
		be := router.lookupBackend(addr, true)
		if be == nil && health.status != StatusCannotConnect {
			router.logger.Info("update backend", zap.String("backend_addr", addr),
				zap.String("prev", "none"), zap.String("cur", health.String()))
			be = router.backends.PushBack(&backendWrapper{
				backendHealth: health,
				addr:          addr,
				connList:      glist.New[*connWrapper](),
			})
			router.adjustBackendList(be)
		} else if be != nil {
			backend := be.Value
			router.logger.Info("update backend", zap.String("backend_addr", addr),
				zap.String("prev", backend.String()), zap.String("cur", health.String()))
			backend.backendHealth = health
			router.adjustBackendList(be)
			for ele := backend.connList.Front(); ele != nil; ele = ele.Next() {
				conn := ele.Value
				conn.NotifyBackendStatus(health.status)
			}
		}
	}
	if len(backends) > 0 {
		router.updateServerVersion()
	}
}

func (router *ScoreBasedRouter) rebalanceLoop(ctx context.Context) {
	for {
		router.rebalance(rebalanceConnsPerLoop)
		select {
		case <-ctx.Done():
			return
		case <-time.After(rebalanceInterval):
		}
	}
}

func (router *ScoreBasedRouter) rebalance(maxNum int) {
	curTime := time.Now()
	router.Lock()
	defer router.Unlock()
	for i := 0; i < maxNum; i++ {
		var busiestEle *glist.Element[*backendWrapper]
		for be := router.backends.Front(); be != nil; be = be.Next() {
			backend := be.Value
			if backend.connList.Len() > 0 {
				busiestEle = be
				break
			}
		}
		if busiestEle == nil {
			break
		}
		busiestBackend := busiestEle.Value
		idlestEle := router.backends.Back()
		idlestBackend := idlestEle.Value
		if float64(busiestBackend.score())/float64(idlestBackend.score()+1) < rebalanceMaxScoreRatio {
			break
		}
		var ce *glist.Element[*connWrapper]
		for ele := busiestBackend.connList.Front(); ele != nil; ele = ele.Next() {
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
		conn := ce.Value
		router.logger.Info("begin redirect connection", zap.Uint64("connID", conn.ConnectionID()),
			zap.String("from", busiestBackend.addr), zap.String("to", idlestBackend.addr),
			zap.Int("from_score", busiestBackend.score()), zap.Int("to_score", idlestBackend.score()))
		busiestBackend.connScore--
		router.adjustBackendList(busiestEle)
		idlestBackend.connScore++
		router.adjustBackendList(idlestEle)
		conn.phase = phaseRedirectNotify
		conn.lastRedirect = curTime
		conn.Redirect(idlestBackend.addr)
	}
}

func (router *ScoreBasedRouter) removeBackendIfEmpty(be *glist.Element[*backendWrapper]) bool {
	backend := be.Value
	// If connList.Len() == 0, there won't be any outgoing connections.
	// And if also connScore == 0, there won't be any incoming connections.
	if backend.status == StatusCannotConnect && backend.connList.Len() == 0 && backend.connScore <= 0 {
		router.backends.Remove(be)
		return true
	}
	return false
}

func (router *ScoreBasedRouter) ConnCount() int {
	router.Lock()
	defer router.Unlock()
	j := 0
	for be := router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value
		j += backend.connList.Len()
	}
	return j
}

// It's called within a lock.
func (router *ScoreBasedRouter) updateServerVersion() {
	for be := router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value
		if backend.backendHealth.status != StatusCannotConnect && len(backend.serverVersion) > 0 {
			router.serverVersion = backend.serverVersion
			return
		}
	}
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
	if router.observer != nil {
		router.observer.Close()
		router.observer = nil
	}
	router.wg.Wait()
	// Router only refers to RedirectableConn, it doesn't manage RedirectableConn.
}
