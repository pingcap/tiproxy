package router

import (
	"errors"
	"math"
	"sync"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	ErrNoInstanceToSelect = errors.New("no instances to route")
)

type ConnList struct {
	//sync.Mutex
	backendInfo *BackendInfo
	movingIn    map[uint64]driver.RedirectableConn
	active      map[uint64]driver.RedirectableConn
	movingOut   map[uint64]driver.RedirectableConn
}

type RandomRouter struct {
	observer *BackendObserver
	mu       struct {
		sync.Mutex
		conns map[string]*ConnList
	}
}

func NewRandomRouter(cfg *config.BackendNamespace) (*RandomRouter, error) {
	router := &RandomRouter{}
	observer, err := NewBackendObserver(router.onBackendChanged)
	if err != nil {
		return nil, err
	}
	router.observer = observer
	err = router.initConnections(cfg.Instances)
	return router, err
}

func (router *RandomRouter) initConnections(addrs []string) error {
	router.mu.Lock()
	defer router.mu.Unlock()
	router.mu.conns = make(map[string]*ConnList)
	if router.observer == nil {
		if len(addrs) == 0 {
			return ErrNoInstanceToSelect
		}
		for _, addr := range addrs {
			router.mu.conns[addr] = &ConnList{
				backendInfo: &BackendInfo{
					status: StatusHealthy,
				},
				movingIn:  make(map[uint64]driver.RedirectableConn),
				active:    make(map[uint64]driver.RedirectableConn),
				movingOut: make(map[uint64]driver.RedirectableConn),
			}
		}
	}
	return nil
}

func (router *RandomRouter) Route() (string, error) {
	leastConnNum := math.MaxInt
	var leastConnAddr string
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, connList := range router.mu.conns {
		if connList.backendInfo.status == StatusHealthy {
			num := len(connList.movingIn) + len(connList.active)
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

// ConnCount returns the overall connections on backend servers.
// The result may be inaccurate during connection migration.
func (router *RandomRouter) ConnCount() int {
	connNum := 0
	router.mu.Lock()
	defer router.mu.Unlock()
	for _, connList := range router.mu.conns {
		connNum += len(connList.active) + len(connList.movingOut)
	}
	return connNum
}

func (router *RandomRouter) AddConn(addr string, conn driver.RedirectableConn) {
	router.mu.Lock()
	defer router.mu.Unlock()
	conns, ok := router.mu.conns[addr]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
	}
	// TODO: check health
	conns.active[conn.ConnectionID()] = conn
}

func (router *RandomRouter) RedirectConnections() error {
	router.mu.Lock()
	defer router.mu.Unlock()
	var err error
	for addr, connList := range router.mu.conns {
		for _, conn := range connList.active {
			err1 := conn.Redirect(addr)
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	return err
}

func (router *RandomRouter) BeginRedirect(from, to string, conn driver.RedirectableConn) {
	router.mu.Lock()
	defer router.mu.Unlock()
	originalConns, ok := router.mu.conns[from]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", from))
		return
	}
	connID := conn.ConnectionID()
	if _, ok = originalConns.movingOut[connID]; ok {
		return
	}
	delete(originalConns.active, connID)
	delete(originalConns.movingIn, connID)
	originalConns.movingOut[connID] = conn

	newConns, ok := router.mu.conns[to]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", to))
		return
	}
	newConns.movingIn[connID] = conn
}

func (router *RandomRouter) FinishRedirect(from, to string, conn driver.RedirectableConn) {
	router.mu.Lock()
	defer router.mu.Unlock()
	originalConns, ok := router.mu.conns[from]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", from))
		return
	}
	connID := conn.ConnectionID()
	delete(originalConns.movingOut, connID)
	newConns, ok := router.mu.conns[to]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", to))
		return
	}
	delete(newConns.movingIn, connID)
	newConns.active[connID] = conn
}

func (router *RandomRouter) CloseConn(addr string, conn driver.RedirectableConn) {
	router.mu.Lock()
	defer router.mu.Unlock()
	originalConns, ok := router.mu.conns[addr]
	if !ok {
		logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
		return
	}
	connID := conn.ConnectionID()
	delete(originalConns.active, connID)
}

func (router *RandomRouter) onBackendChanged(removed, added map[string]*BackendInfo) {
	router.mu.Lock()
	defer router.mu.Unlock()
	for addr, info := range removed {
		logutil.BgLogger().Info("remove backend", zap.String("url", addr), zap.String("status", info.status.String()))
		conns, ok := router.mu.conns[addr]
		if !ok {
			logutil.BgLogger().Error("no such address in backend info", zap.String("addr", addr))
			continue
		}
		conns.backendInfo = info
	}
	for addr, info := range added {
		logutil.BgLogger().Info("add backend", zap.String("url", addr))
		conns, ok := router.mu.conns[addr]
		if !ok {
			router.mu.conns[addr] = &ConnList{
				backendInfo: info,
				movingIn:    make(map[uint64]driver.RedirectableConn),
				active:      make(map[uint64]driver.RedirectableConn),
				movingOut:   make(map[uint64]driver.RedirectableConn),
			}
		} else {
			conns.backendInfo = info
		}
	}
	// TODO: rebalance
}

func (router *RandomRouter) Close() {
	router.observer.Close()
	// Router only refers to RedirectableConn, it doesn't manage RedirectableConn.
}
