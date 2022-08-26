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
	"errors"
	"sync"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Router interface {
	Route(RedirectableConn) (string, error)
	RedirectConnections() error
	Close()
}

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
	rebalanceInterval      = 10 * time.Millisecond
	rebalanceConnsPerLoop  = 10
	rebalanceMaxScoreRatio = 1.1
)

type ConnEventReceiver interface {
	OnRedirectSucceed(from, to string, conn RedirectableConn)
	OnRedirectFail(from, to string, conn RedirectableConn)
	OnConnClosed(addr string, conn RedirectableConn)
}

type RedirectableConn interface {
	SetEventReceiver(receiver ConnEventReceiver)
	Redirect(addr string)
	ConnectionID() uint64
}

type BackendWrapper struct {
	status BackendStatus
	addr   string
	// A list of *ConnWrapper and is ordered by the connecting or redirecting time.
	connList *list.List
	connMap  map[uint64]*list.Element
}

func (b *BackendWrapper) score() int {
	return b.status.ToScore() + b.connList.Len()
}

type ConnWrapper struct {
	RedirectableConn
	phase int
}

type RandomRouter struct {
	sync.Mutex
	observer   *BackendObserver
	cancelFunc context.CancelFunc
	// A list of *BackendWrapper and ordered by the score of the backends.
	backends *list.List
}

func NewRandomRouter(cfg *config.BackendNamespace, client *clientv3.Client) (*RandomRouter, error) {
	router := &RandomRouter{
		backends: list.New(),
	}
	router.Lock()
	defer router.Unlock()
	observer, err := StartBackendObserver(router, client, newDefaultHealthCheckConfig(), cfg.Instances)
	if err != nil {
		return nil, err
	}
	router.observer = observer
	childCtx, cancelFunc := context.WithCancel(context.Background())
	router.cancelFunc = cancelFunc
	go router.rebalanceLoop(childCtx)
	return router, err
}

func (router *RandomRouter) Route(conn RedirectableConn) (string, error) {
	router.Lock()
	defer router.Unlock()
	be := router.backends.Back()
	if be == nil {
		return "", ErrNoInstanceToSelect
	}
	backend := be.Value.(*BackendWrapper)
	switch backend.status {
	case StatusCannotConnect, StatusSchemaOutdated:
		return "", ErrNoInstanceToSelect
	}
	connWrapper := &ConnWrapper{
		RedirectableConn: conn,
		phase:            phaseNotRedirected,
	}
	router.addConn(be, connWrapper)
	conn.SetEventReceiver(router)
	return backend.addr, nil
}

func (router *RandomRouter) removeConn(be *list.Element, ce *list.Element) {
	backend := be.Value.(*BackendWrapper)
	conn := ce.Value.(*ConnWrapper)
	backend.connList.Remove(ce)
	delete(backend.connMap, conn.ConnectionID())
	router.adjustBackendList(be)
}

func (router *RandomRouter) addConn(be *list.Element, conn *ConnWrapper) {
	backend := be.Value.(*BackendWrapper)
	ce := backend.connList.PushBack(conn)
	backend.connMap[conn.ConnectionID()] = ce
	router.adjustBackendList(be)
}

func (router *RandomRouter) adjustBackendList(be *list.Element) {
	backend := be.Value.(*BackendWrapper)
	curScore := backend.score()
	var mark *list.Element
	for ele := be.Prev(); ele != nil; ele = ele.Prev() {
		b := ele.Value.(*BackendWrapper)
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
		b := ele.Value.(*BackendWrapper)
		if b.score() <= curScore {
			break
		}
		mark = ele
	}
	if mark != nil {
		router.backends.MoveAfter(be, mark)
	}
}

func (router *RandomRouter) RedirectConnections() error {
	router.Lock()
	defer router.Unlock()
	for be := router.backends.Front(); be != nil; be = be.Next() {
		backend := be.Value.(*BackendWrapper)
		for ce := backend.connList.Front(); ce != nil; ce = ce.Next() {
			// This is only for test, so we allow it to reconnect to the same backend.
			connWrapper := ce.Value.(*ConnWrapper)
			if connWrapper.phase != phaseRedirectNotify {
				connWrapper.phase = phaseRedirectNotify
				connWrapper.Redirect(backend.addr)
			}
		}
	}
	return nil
}

func (router *RandomRouter) lookupBackend(addr string, forward bool) *list.Element {
	if forward {
		for be := router.backends.Front(); be != nil; be = be.Next() {
			backend := be.Value.(*BackendWrapper)
			if backend.addr == addr {
				return be
			}
		}
	} else {
		for be := router.backends.Back(); be != nil; be = be.Prev() {
			backend := be.Value.(*BackendWrapper)
			if backend.addr == addr {
				return be
			}
		}
	}
	return nil
}

func (router *RandomRouter) OnRedirectSucceed(from, to string, conn RedirectableConn) {
	router.Lock()
	defer router.Unlock()
	be := router.lookupBackend(to, false)
	if be == nil {
		// impossible here
		return
	}
	toBackend := be.Value.(*BackendWrapper)
	e, ok := toBackend.connMap[conn.ConnectionID()]
	if !ok {
		// impossible here
		return
	}
	connWrapper := e.Value.(*ConnWrapper)
	connWrapper.phase = phaseRedirectEnd
}

func (router *RandomRouter) OnRedirectFail(from, to string, conn RedirectableConn) {
	router.Lock()
	defer router.Unlock()
	be := router.lookupBackend(to, false)
	if be == nil {
		// impossible here
		return
	}
	toBackend := be.Value.(*BackendWrapper)
	ce, ok := toBackend.connMap[conn.ConnectionID()]
	if !ok {
		// impossible here
		return
	}
	router.removeConn(be, ce)

	be = router.lookupBackend(from, true)
	// If the backend has already been removed, the connection is discarded from the router.
	if be == nil {
		return
	}
	connWrapper := ce.Value.(*ConnWrapper)
	connWrapper.phase = phaseRedirectFail
	router.addConn(be, connWrapper)
}

func (router *RandomRouter) OnConnClosed(addr string, conn RedirectableConn) {
	connID := conn.ConnectionID()
	router.Lock()
	defer router.Unlock()
	be := router.lookupBackend(addr, true)
	if be != nil {
		// impossible here
		return
	}
	backend := be.Value.(*BackendWrapper)
	ce, ok := backend.connMap[connID]
	if !ok {
		// impossible here
		return
	}
	router.removeConn(be, ce)
	router.removeBackendIfEmpty(be)
}

func (router *RandomRouter) OnBackendChanged(backends map[string]BackendStatus) {
	router.Lock()
	defer router.Unlock()
	for addr, status := range backends {
		be := router.lookupBackend(addr, true)
		if be == nil {
			logutil.BgLogger().Info("find new backend", zap.String("url", addr),
				zap.String("status", status.String()))
			be = router.backends.PushBack(&BackendWrapper{
				status:   status,
				addr:     addr,
				connList: list.New(),
				connMap:  make(map[uint64]*list.Element),
			})
		} else {
			backend := be.Value.(*BackendWrapper)
			logutil.BgLogger().Info("update backend", zap.String("url", addr),
				zap.String("prev_status", backend.status.String()), zap.String("cur_status", status.String()))
			backend.status = status
		}
		router.adjustBackendList(be)
		router.removeBackendIfEmpty(be)
	}
}

func (router *RandomRouter) rebalanceLoop(ctx context.Context) {
	for {
		router.rebalance(rebalanceConnsPerLoop)
		select {
		case <-ctx.Done():
			return
		case <-time.After(rebalanceInterval):
		}
	}
}

func (router *RandomRouter) rebalance(maxNum int) {
	router.Lock()
	defer router.Unlock()
	for i := 0; i < maxNum; i++ {
		var busiestEle *list.Element
		for be := router.backends.Front(); be != nil; be = be.Next() {
			backend := be.Value.(*BackendWrapper)
			if backend.connList.Len() > 0 {
				busiestEle = be
				break
			}
		}
		if busiestEle == nil {
			break
		}
		busiestBackend := busiestEle.Value.(*BackendWrapper)
		idlestEle := router.backends.Back()
		idlestBackend := idlestEle.Value.(*BackendWrapper)
		if float64(busiestBackend.score())/float64(idlestBackend.score()+1) <= rebalanceMaxScoreRatio {
			break
		}
		ce := busiestBackend.connList.Front()
		router.removeConn(busiestEle, ce)
		conn := ce.Value.(*ConnWrapper)
		conn.phase = phaseRedirectNotify
		router.addConn(idlestEle, conn)
		conn.Redirect(idlestBackend.addr)
	}
}

func (router *RandomRouter) removeBackendIfEmpty(be *list.Element) {
	backend := be.Value.(*BackendWrapper)
	if backend.status == StatusCannotConnect && backend.connList.Len() == 0 {
		router.backends.Remove(be)
	}
}

func (router *RandomRouter) Close() {
	router.Lock()
	defer router.Unlock()
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
