// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
)

var (
	ErrNoBackend = errors.New("no available backend")
)

// ConnEventReceiver receives connection events.
type ConnEventReceiver interface {
	OnRedirectSucceed(from, to string, conn RedirectableConn) error
	OnRedirectFail(from, to string, conn RedirectableConn) error
	OnConnClosed(addr string, conn RedirectableConn) error
}

// Router routes client connections to backends.
type Router interface {
	// ConnEventReceiver handles connection events to balance connections if possible.
	ConnEventReceiver

	GetBackendSelector() BackendSelector
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
)

const (
	// The interval to rebalance connections.
	rebalanceInterval = time.Second
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
}

// backendWrapper contains the connections on the backend.
type backendWrapper struct {
	mu struct {
		sync.RWMutex
		observer.BackendHealth
	}
	addr      string
	scoreBits uint64
	// connScore is used for calculating backend scores and check if the backend can be removed from the list.
	// connScore = connList.Len() + incoming connections - outgoing connections.
	connScore int
	// A list of *connWrapper and is ordered by the connecting or redirecting time.
	// connList only includes the connections that are currently on this backend.
	connList *glist.List[*connWrapper]
}

func newBackendWrapper(addr string, health observer.BackendHealth) *backendWrapper {
	wrapper := &backendWrapper{
		addr:     addr,
		connList: glist.New[*connWrapper](),
	}
	wrapper.setHealth(health)
	return wrapper
}

func (b *backendWrapper) setHealth(health observer.BackendHealth) {
	b.mu.Lock()
	b.mu.BackendHealth = health
	b.mu.Unlock()
}

func (b *backendWrapper) addScore(score int, bitNum int) {
	b.scoreBits = b.scoreBits<<bitNum | uint64(score)
}

func (b *backendWrapper) clearScore() {
	b.scoreBits = 0
}

// score calculates the score of the backend. Larger score indicates higher load.
func (b *backendWrapper) score() uint64 {
	return b.scoreBits
}

func (b *backendWrapper) Addr() string {
	return b.addr
}

func (b *backendWrapper) Status() observer.BackendStatus {
	b.mu.RLock()
	status := b.mu.Status
	b.mu.RUnlock()
	return status
}

func (b *backendWrapper) Healthy() bool {
	b.mu.RLock()
	healthy := b.mu.Status == observer.StatusHealthy
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

func (b *backendWrapper) Equals(health observer.BackendHealth) bool {
	b.mu.RLock()
	equal := b.mu.BackendHealth.Equals(health)
	b.mu.RUnlock()
	return equal
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
	// Reference to the target backend if it's redirecting, otherwise nil.
	redirectingBackend *backendWrapper
	// Last redirect start time of this connection.
	lastRedirect monotime.Time
	phase        connPhase
}
