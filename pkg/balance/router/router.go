// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"go.uber.org/zap"
)

var (
	ErrNoBackend = errors.New("no available backend")
)

// ConnEventReceiver receives connection events.
type ConnEventReceiver interface {
	OnRedirectSucceed(from, to string, conn RedirectableConn) error
	OnRedirectFail(from, to string, conn RedirectableConn) error
	OnConnClosed(addr, redirectingAddr string, conn RedirectableConn) error
}

// Router routes client connections to backends.
type Router interface {
	// ConnEventReceiver handles connection events to balance connections if possible.
	ConnEventReceiver

	GetBackendSelector() BackendSelector
	HealthyBackendCount() int
	RefreshBackend()
	RedirectConnections() error
	ConnCount() int
	// ServerVersion returns the TiDB version.
	ServerVersion() string
	Close()
}

type connPhase int

const (
	// The session is never redirected.
	phaseNotRedirected connPhase = iota
	// The session is redirecting.
	phaseRedirectNotify
	// The session redirected successfully last time.
	phaseRedirectEnd
	// The session failed to redirect last time.
	phaseRedirectFail
	// The connection is closed.
	phaseClosed
)

const (
	// The interval to rebalance connections.
	rebalanceInterval = 10 * time.Millisecond
	// After a connection fails to redirect, it may contain some unmigratable status.
	// Limit its redirection interval to avoid unnecessary retrial to reduce latency jitter.
	redirectFailMinInterval = 3 * time.Second
)

// RedirectableConn indicates a redirect-able connection.
type RedirectableConn interface {
	SetEventReceiver(receiver ConnEventReceiver)
	SetValue(key, val any)
	Value(key any) any
	// Redirect returns false if the current conn is not redirectable.
	Redirect(backend BackendInst) bool
	ConnectionID() uint64
}

// BackendInst defines a backend that a connection is redirecting to.
type BackendInst interface {
	Addr() string
	Healthy() bool
	Local() bool
}

// backendWrapper contains the connections on the backend.
type backendWrapper struct {
	mu struct {
		sync.RWMutex
		observer.BackendHealth
	}
	addr string
	// connScore is used for calculating backend scores and check if the backend can be removed from the list.
	// connScore = connList.Len() + incoming connections - outgoing connections.
	connScore int
	// A list of *connWrapper and is ordered by the connecting or redirecting time.
	// connList only includes the connections that are currently on this backend.
	connList *glist.List[*connWrapper]
	incoming map[uint64]struct{}
	pending  map[uint64]struct{}
	lg       *zap.Logger
}

func newBackendWrapper(addr string, health observer.BackendHealth, lg *zap.Logger) *backendWrapper {
	wrapper := &backendWrapper{
		addr:     addr,
		connList: glist.New[*connWrapper](),
		incoming: make(map[uint64]struct{}),
		pending:  make(map[uint64]struct{}),
		lg:       lg.With(zap.String("backend", addr)),
	}
	wrapper.setHealth(health)
	return wrapper
}

func (b *backendWrapper) setHealth(health observer.BackendHealth) {
	b.mu.Lock()
	b.mu.BackendHealth = health
	b.mu.Unlock()
}

func (b *backendWrapper) ConnScore() int {
	return b.connScore
}

func (b *backendWrapper) Addr() string {
	return b.addr
}

func (b *backendWrapper) Healthy() bool {
	b.mu.RLock()
	healthy := b.mu.Healthy
	b.mu.RUnlock()
	return healthy
}

func (b *backendWrapper) ServerVersion() string {
	b.mu.RLock()
	version := b.mu.ServerVersion
	b.mu.RUnlock()
	return version
}

func (b *backendWrapper) ConnCount() int {
	return b.connList.Len()
}

func (b *backendWrapper) AddIncoming(id uint64) {
	if _, ok := b.incoming[id]; ok {
		b.lg.Error("score error", zap.Uint64("id", id), zap.Stack("second"))
	} else {
		b.incoming[id] = struct{}{}
	}
}

func (b *backendWrapper) DecIncoming(id uint64) {
	if _, ok := b.incoming[id]; !ok {
		b.lg.Error("score error", zap.Uint64("id", id), zap.Stack("nonexist"))
	} else {
		delete(b.incoming, id)
	}
	if b.connScore < 0 {
		b.lg.Error("score error", zap.Int("connScore", b.connScore), zap.Stack("negative"))
	}
}

func (b *backendWrapper) AddPending(id uint64) {
	if _, ok := b.pending[id]; ok {
		b.lg.Error("pending error", zap.Uint64("id", id), zap.Stack("second"))
	} else {
		b.pending[id] = struct{}{}
	}
}

func (b *backendWrapper) DecPending(id uint64) {
	if _, ok := b.pending[id]; !ok {
		b.lg.Error("pending error", zap.Uint64("id", id), zap.Stack("nonexist"))
	} else {
		delete(b.pending, id)
	}
}

func (b *backendWrapper) Local() bool {
	b.mu.RLock()
	local := b.mu.Local
	b.mu.RUnlock()
	return local
}

func (b *backendWrapper) GetBackendInfo() observer.BackendInfo {
	b.mu.RLock()
	info := b.mu.BackendInfo
	b.mu.RUnlock()
	return info
}

func (b *backendWrapper) SupportRedirection() bool {
	b.mu.RLock()
	supportRedirection := b.mu.SupportRedirection
	b.mu.RUnlock()
	return supportRedirection
}

func (b *backendWrapper) String() string {
	b.mu.RLock()
	str := b.mu.String()
	b.mu.RUnlock()
	return str
}

// connWrapper wraps RedirectableConn.
type connWrapper struct {
	RedirectableConn
	// The reason why the redirection happens.
	redirectReason string
	// Last redirect start time of this connection.
	lastRedirect time.Time
	phase        connPhase
}
