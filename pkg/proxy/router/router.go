package router

import (
	"container/list"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	ErrNoInstanceToSelect = errors.New("no instances to route")
)

const (
	phaseNotRedirected int = iota
	phaseRedirectNotify
	phaseRedirectEnd
	phaseRedirectFail
)

const (
	rebalanceInterval     = 10 * time.Millisecond
	rebalanceConnsPerLoop = 10
	rebalancePctThreshold = 0.1
)

type BackendWrapper struct {
	BackendInfo
	connList *list.List
	connMap  map[uint64]*list.Element
}

type ConnWrapper struct {
	driver.RedirectableConn
	phase int
}

type RedirectRequest struct {
	from   string
	connID uint64
}

type RandomRouter struct {
	observer   *BackendObserver
	cancelFunc context.CancelFunc
	mu         struct {
		sync.Mutex
		backends map[string]*BackendWrapper
	}
}

func NewRandomRouter(cfg *config.BackendNamespace) (*RandomRouter, error) {
	router := &RandomRouter{}
	observer, err := NewBackendObserver(router)
	if err != nil {
		return nil, err
	}
	router.observer = observer
	err = router.initConnections(cfg.Instances)
	childCtx, cancelFunc := context.WithCancel(context.Background())
	go router.rebalanceLoop(childCtx)
	router.cancelFunc = cancelFunc
	return router, err
}

func (router *RandomRouter) initConnections(addrs []string) error {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.mu.backends = make(map[string]*BackendWrapper)
	if router.observer == nil {
		logutil.BgLogger().Info("pd addr is not configured, use static backend instances instead.")
		if len(addrs) == 0 {
			return ErrNoInstanceToSelect
		}
		for _, addr := range addrs {
			router.mu.backends[addr] = &BackendWrapper{
				BackendInfo: BackendInfo{
					status: StatusHealthy,
				},
				connList: list.New(),
				connMap:  make(map[uint64]*list.Element),
			}
		}
	}
	return nil
}

func (router *RandomRouter) Route(conn driver.RedirectableConn) (string, error) {
	addr, err := router.getLeastConnBackend()
	if err != nil {
		return addr, err
	}
	router.addConn(addr, conn)
	conn.SetEventReceiver(router)
	return addr, nil
}

func (router *RandomRouter) getLeastConnBackend() (string, error) {
	leastConnNum := math.MaxInt
	var leastConnAddr string
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, backend := range router.mu.backends {
		if backend.status == StatusHealthy {
			num := len(backend.connMap)
			if num < leastConnNum {
				leastConnNum = num
				leastConnAddr = addr
			}
		}
	}
	if len(leastConnAddr) == 0 {
		return leastConnAddr, ErrNoInstanceToSelect
	}
	return leastConnAddr, nil
}

func (router *RandomRouter) addConn(addr string, conn driver.RedirectableConn) {
	connID := conn.ConnectionID()
	router.mu.Lock()
	defer router.mu.Unlock()
	backend, ok := router.mu.backends[addr]
	if !ok {
		// The backend may be just down and moved away.
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
		return
	}
	connWrapper := &ConnWrapper{
		RedirectableConn: conn,
		phase:            phaseNotRedirected,
	}
	e := backend.connList.PushBack(connWrapper)
	backend.connMap[connID] = e
}

func (router *RandomRouter) RedirectConnections() error {
	requests := make([]*RedirectRequest, 0)
	router.mu.Lock()
	for addr, backend := range router.mu.backends {
		for e := backend.connList.Front(); e != nil; e = e.Next() {
			requests = append(requests, &RedirectRequest{
				from:   addr,
				connID: e.Value.(*ConnWrapper).ConnectionID(),
			})
		}
	}
	router.mu.Unlock()
	for _, request := range requests {
		to, err := router.getLeastConnBackend()
		if err != nil {
			logutil.BgLogger().Warn("redirecting encounters an error", zap.Error(err))
			break
		}
		// This is only for test, so we allow it to reconnect to the same backend.
		router.notifyRedirect(request.from, to, request.connID)
	}
	return nil
}

func (router *RandomRouter) notifyRedirect(from, to string, connID uint64) {
	router.mu.Lock()
	defer router.mu.Unlock()
	originalBackend, ok := router.mu.backends[from]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", from))
		return
	}
	newBackend, ok := router.mu.backends[to]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", to))
		return
	}
	e, ok := originalBackend.connMap[connID]
	if !ok {
		// It may have been redirected or closed. It's fine, we can redirect it at next time if necessary.
		return
	}
	if newBackend.status != StatusHealthy {
		return
	}
	connWrapper := e.Value.(*ConnWrapper)
	if connWrapper.phase == phaseRedirectNotify {
		// We do not send concurrent signals.
		return
	}
	originalBackend.connList.Remove(e)
	delete(originalBackend.connMap, connID)
	router.removeBackendIfEmpty(from, originalBackend)
	connWrapper.phase = phaseRedirectNotify
	e = newBackend.connList.PushBack(connWrapper)
	newBackend.connMap[connID] = e
	connWrapper.Redirect(to)
}

func (router *RandomRouter) OnRedirectSucceed(from, to string, conn driver.RedirectableConn) {
	connID := conn.ConnectionID()
	router.mu.Lock()
	defer router.mu.Unlock()
	newBackend, ok := router.mu.backends[to]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", to))
		return
	}
	e, ok := newBackend.connMap[connID]
	if !ok {
		// Impossible here.
		return
	}
	connWrapper := e.Value.(*ConnWrapper)
	if connWrapper.phase != phaseRedirectNotify {
		// Impossible here.
		return
	}
	connWrapper.phase = phaseRedirectEnd
}

func (router *RandomRouter) OnRedirectFail(from, to string, conn driver.RedirectableConn) {
	connID := conn.ConnectionID()
	router.mu.Lock()
	defer router.mu.Unlock()
	newBackend, ok := router.mu.backends[to]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", to))
		return
	}
	e, ok := newBackend.connMap[connID]
	if !ok {
		// Impossible here.
		return
	}
	connWrapper := e.Value.(*ConnWrapper)
	if connWrapper.phase != phaseRedirectNotify {
		// Impossible here.
		return
	}
	connWrapper.phase = phaseRedirectFail
	newBackend.connList.Remove(e)
	delete(newBackend.connMap, connID)
	originalBackend, ok := router.mu.backends[from]
	if !ok {
		// The backend may have been removed already, then the connection is discarded from the router.
		return
	}
	e = originalBackend.connList.PushBack(connWrapper)
	originalBackend.connMap[connID] = e
}

func (router *RandomRouter) OnConnClosed(addr string, conn driver.RedirectableConn) {
	connID := conn.ConnectionID()
	router.mu.Lock()
	defer router.mu.Unlock()
	backend, ok := router.mu.backends[addr]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
		return
	}
	e, ok := backend.connMap[connID]
	if !ok {
		// Impossible here.
		return
	}
	backend.connList.Remove(e)
	delete(backend.connMap, connID)
	router.removeBackendIfEmpty(addr, backend)
}

func (router *RandomRouter) OnBackendChanged(removed, added map[string]*BackendInfo) {
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, info := range removed {
		logutil.BgLogger().Info("remove backend", zap.String("url", addr), zap.String("status", info.status.String()))
		backend, ok := router.mu.backends[addr]
		if !ok {
			logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
			continue
		}
		backend.BackendInfo = *info
		router.removeBackendIfEmpty(addr, backend)
	}
	for addr, info := range added {
		logutil.BgLogger().Info("add backend", zap.String("url", addr))
		backend, ok := router.mu.backends[addr]
		if !ok {
			router.mu.backends[addr] = &BackendWrapper{
				BackendInfo: *info,
				connList:    list.New(),
				connMap:     make(map[uint64]*list.Element),
			}
		} else {
			backend.BackendInfo = *info
		}
	}
}

func (router *RandomRouter) rebalanceLoop(ctx context.Context) {
	for {
		requests := router.getStrayConns(rebalanceConnsPerLoop)
		// If there are any homeless connections this loop, we move unbalanced connections
		// in the next loop to simplify the logic.
		if len(requests) == 0 {
			requests = router.getUnbalancedConns(rebalanceConnsPerLoop)
		}
		if ctx.Err() != nil {
			break
		}
		if len(requests) > 0 {
			router.redirectConns(requests)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(rebalanceInterval):
		}
	}
}

func (router *RandomRouter) getStrayConns(maxNum int) []*RedirectRequest {
	requests := make([]*RedirectRequest, 0, maxNum)
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, backend := range router.mu.backends {
		if backend.status != StatusHealthy {
			for e := backend.connList.Front(); e != nil && len(requests) < maxNum; e = e.Next() {
				id := e.Value.(*ConnWrapper).ConnectionID()
				requests = append(requests, &RedirectRequest{
					from:   addr,
					connID: id,
				})
			}
		}
		if len(requests) >= maxNum {
			break
		}
	}
	return requests
}

func (router *RandomRouter) getUnbalancedConns(maxNum int) []*RedirectRequest {
	var (
		totalConns, totalBackends, maxConnNum int
		maxConnAddr                           string
	)
	requests := make([]*RedirectRequest, 0, maxNum)
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, backend := range router.mu.backends {
		if backend.status == StatusHealthy {
			totalBackends++
			connNum := len(backend.connMap)
			totalConns += connNum
			if connNum > maxConnNum {
				maxConnNum = connNum
				maxConnAddr = addr
			}
		}
	}
	if totalBackends <= 1 || totalConns <= 1 {
		return requests
	}
	avgConnNum := float64(totalConns) / float64(totalBackends)
	threshold := int(avgConnNum*(1+rebalancePctThreshold)) + 1
	// We only rebalance one overloaded backend at one time.
	if maxConnNum > threshold {
		if maxConnNum-threshold < maxNum {
			maxNum = maxConnNum - threshold
		}
		backend := router.mu.backends[maxConnAddr]
		for e := backend.connList.Front(); e != nil && len(requests) < maxNum; e = e.Next() {
			id := e.Value.(*ConnWrapper).ConnectionID()
			requests = append(requests, &RedirectRequest{
				from:   maxConnAddr,
				connID: id,
			})
		}
	}
	return requests
}

func (router *RandomRouter) redirectConns(requests []*RedirectRequest) {
	for _, request := range requests {
		to, err := router.getLeastConnBackend()
		if err != nil {
			// All instances are down. We don't log here because the logs will be too many.
			break
		}
		if request.from == to {
			continue
		}
		router.notifyRedirect(request.from, to, request.connID)
	}
}

func (router *RandomRouter) removeBackendIfEmpty(addr string, backend *BackendWrapper) {
	if (backend.status == StatusServerDown || backend.status == StatusCannotConnect) && backend.connList.Len() == 0 {
		delete(router.mu.backends, addr)
	}
}

func (router *RandomRouter) Close() {
	if router.cancelFunc != nil {
		router.cancelFunc()
		router.cancelFunc = nil
	}
	if router.observer != nil {
		router.observer.Close()
		router.observer = nil
	}
	// Router only refers to RedirectableConn, it doesn't manage RedirectableConn.
}
