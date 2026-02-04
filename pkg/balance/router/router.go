// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strings"
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/config"
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
	GetBackendSelector(clientInfo ClientInfo) BackendSelector
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
	ConnInfo() []zap.Field
}

// BackendInst defines a backend that a connection is redirecting to.
type BackendInst interface {
	Addr() string
	Healthy() bool
	Local() bool
	Keyspace() string
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
	// The group that this backend belongs to.
	group *Group
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

func (b *backendWrapper) getHealth() observer.BackendHealth {
	b.mu.RLock()
	health := b.mu.BackendHealth
	b.mu.RUnlock()
	return health
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

func (b *backendWrapper) Keyspace() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	labels := b.mu.BackendHealth.Labels
	if labels == nil {
		return ""
	}
	return labels[config.KeyspaceLabelName]
}

func (b *backendWrapper) Cidr() []string {
	labels := b.getHealth().Labels
	if len(labels) == 0 {
		return nil
	}
	cidr := labels[config.CidrLabelName]
	if len(cidr) == 0 {
		return nil
	}
	cidrs := strings.Split(cidr, ",")
	for i := len(cidrs) - 1; i >= 0; i-- {
		cidr = strings.TrimSpace(cidrs[i])
		if len(cidr) == 0 {
			cidrs = append(cidrs[:i], cidrs[i+1:]...)
		} else {
			cidrs[i] = cidr
		}
	}
	return cidrs
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
	createTime   time.Time
	phase        connPhase
}
