// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"net"
	"reflect"
	"slices"
	"sync"
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

type MatchType int

const (
	// Match all connections, used when there is only one backend group.
	MatchAll MatchType = iota
	// Match connections based on the client CIDR.
	MatchClientCIDR
	// Match connections based on proxy CIDR. If proxy-protocol is disabled, route by the client CIDR.
	MatchProxyCIDR
)

const (
	// MatchClientCIDRStr is used for MatchClientCIDR.
	MatchClientCIDRStr = "client_cidr"
	// MatchProxyCIDRStr is used for MatchProxyCIDR.
	MatchProxyCIDRStr = "proxy_cidr"
)

var _ ConnEventReceiver = (*Group)(nil)

// Group is used for one backend group that can be matched by CIDR, username, database, or resource group list.
type Group struct {
	sync.Mutex
	matchType MatchType
	lg        *zap.Logger
	policy    policy.BalancePolicy
	// The values that this group is matched by. E.g. for MatchCIDR, the value is the CIDR list.
	values []string
	// parsed CIDR list for faster match
	cidrList []*net.IPNet
	backends map[string]*backendWrapper
	// To limit the speed of redirection.
	lastRedirectTime time.Time
}

func NewGroup(values []string, bpCreator func(lg *zap.Logger) policy.BalancePolicy, matchType MatchType, lg *zap.Logger) (*Group, error) {
	if len(values) > 0 {
		lg = lg.With(zap.Strings("values", values))
	}
	lg.Info("new group created")

	group := &Group{
		matchType: matchType,
		lg:        lg,
		values:    values,
		backends:  make(map[string]*backendWrapper),
		policy:    bpCreator(lg.Named("policy")),
	}
	err := group.parseValues()
	if err != nil {
		err = errors.Wrapf(err, "failed to parse values")
	}
	return group, err
}

func (g *Group) parseValues() error {
	var parseErr error
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		cidrList := make([]*net.IPNet, 0, len(g.values))
		for _, v := range g.values {
			_, cidr, err := net.ParseCIDR(v)
			if err == nil {
				cidrList = append(cidrList, cidr)
			} else {
				parseErr = err
			}
		}
		g.cidrList = cidrList
	}
	return parseErr
}

func (g *Group) Match(clientInfo ClientInfo) bool {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		addr := clientInfo.ProxyAddr
		if g.matchType == MatchClientCIDR {
			addr = clientInfo.ClientAddr
		}
		if addr == nil || reflect.ValueOf(addr).IsNil() {
			return false
		}
		value := addr.String()
		ipStr, _, err := net.SplitHostPort(value)
		if err != nil {
			g.lg.Error("parsing address failed", zap.String("addr", value), zap.Error(err))
			return false
		}
		ip := net.ParseIP(ipStr)
		if ip == nil {
			g.lg.Error("parsing IP failed", zap.String("ip", value))
			return false
		}
		for _, cidr := range g.cidrList {
			if cidr.Contains(ip) {
				return true
			}
		}
		return false
	}
	return true
}

func (g *Group) EqualValues(values []string) bool {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		if len(g.values) != len(values) {
			return false
		}
		for _, v := range g.values {
			if !slices.Contains(values, v) {
				return false
			}
		}
		return true
	}
	return false
}

func (g *Group) AddBackend(addr string, backend *backendWrapper) {
	g.Lock()
	defer g.Unlock()
	g.backends[addr] = backend
	backend.group = g
}

func (g *Group) RemoveBackend(addr string) {
	g.Lock()
	defer g.Unlock()
	delete(g.backends, addr)
}

func (g *Group) Empty() bool {
	g.Lock()
	defer g.Unlock()
	return len(g.backends) == 0
}

func getConnWrapper(conn RedirectableConn) *glist.Element[*connWrapper] {
	return conn.Value(_routerKey).(*glist.Element[*connWrapper])
}

func setConnWrapper(conn RedirectableConn, ce *glist.Element[*connWrapper]) {
	conn.SetValue(_routerKey, ce)
}

func (g *Group) Route(excluded []BackendInst) (policy.BackendCtx, error) {
	g.Lock()
	defer g.Unlock()

	if len(g.backends) == 0 {
		return nil, ErrNoBackend
	}
	backends := make([]policy.BackendCtx, 0, len(g.backends))
	for _, backend := range g.backends {
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

	idlestBackend := g.policy.BackendToRoute(backends)
	if idlestBackend == nil || reflect.ValueOf(idlestBackend).IsNil() {
		return nil, ErrNoBackend
	}
	backend := idlestBackend.(*backendWrapper)
	backend.connScore++
	return backend, nil
}

func (g *Group) Balance(ctx context.Context) {
	g.Lock()
	defer g.Unlock()
	backends := make([]policy.BackendCtx, 0, len(g.backends))
	for _, backend := range g.backends {
		backends = append(backends, backend)
	}

	busiestBackend, idlestBackend, balanceCount, reason, logFields := g.policy.BackendsToBalance(backends)
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
		if curTime.Sub(g.lastRedirectTime) >= migrationInterval {
			count = 1
		} else {
			return
		}
	}
	// Migrate balanceCount connections.
	i := 0
	for ele := fromBackend.connList.Front(); ele != nil && ctx.Err() == nil && i < count; ele = ele.Next() {
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
		if g.redirectConn(conn, fromBackend, toBackend, reason, logFields, curTime) {
			g.lastRedirectTime = curTime
			i++
		}
	}
}

func (g *Group) onCreateConn(backendInst BackendInst, conn RedirectableConn, succeed bool) {
	g.Lock()
	defer g.Unlock()
	backend := g.ensureBackend(backendInst.Addr())
	if succeed {
		connWrapper := &connWrapper{
			RedirectableConn: conn,
			phase:            phaseNotRedirected,
		}
		g.addConn(backend, connWrapper)
		conn.SetEventReceiver(g)
	} else {
		backend.connScore--
	}
}

func (g *Group) removeConn(backend *backendWrapper, ce *glist.Element[*connWrapper]) {
	backend.connList.Remove(ce)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
}

func (g *Group) addConn(backend *backendWrapper, conn *connWrapper) {
	ce := backend.connList.PushBack(conn)
	setBackendConnMetrics(backend.addr, backend.connList.Len())
	setConnWrapper(conn, ce)
}

// RedirectConnections implements Router.RedirectConnections interface.
// It redirects all connections compulsively. It's only used for testing.
func (g *Group) RedirectConnections() error {
	g.Lock()
	defer g.Unlock()
	for _, backend := range g.backends {
		for ce := backend.connList.Front(); ce != nil; ce = ce.Next() {
			// This is only for test, so we allow it to reconnect to the same backend.
			connWrapper := ce.Value
			if connWrapper.phase != phaseRedirectNotify {
				connWrapper.phase = phaseRedirectNotify
				connWrapper.redirectReason = "test"
				if connWrapper.Redirect(backend) {
					metrics.PendingMigrateGuage.WithLabelValues(backend.addr, backend.addr, connWrapper.redirectReason).Inc()
				}
			}
		}
	}
	return nil
}

func (g *Group) ensureBackend(addr string) *backendWrapper {
	backend, ok := g.backends[addr]
	if ok {
		return backend
	}
	// The backend should always exist if it will be needed. Add a warning and add it back.
	g.lg.Warn("backend is not found in the router", zap.String("backend_addr", addr), zap.Stack("stack"))
	ip, _, _ := net.SplitHostPort(addr)
	backend = newBackendWrapper(addr, observer.BackendHealth{
		BackendInfo: observer.BackendInfo{
			IP:         ip,
			StatusPort: 10080, // impossible anyway
		},
		SupportRedirection: true,
		Healthy:            false,
	})
	g.backends[addr] = backend
	return backend
}

// OnRedirectSucceed implements ConnEventReceiver.OnRedirectSucceed interface.
func (g *Group) OnRedirectSucceed(from, to string, conn RedirectableConn) error {
	g.onRedirectFinished(from, to, conn, true)
	return nil
}

// OnRedirectFail implements ConnEventReceiver.OnRedirectFail interface.
func (g *Group) OnRedirectFail(from, to string, conn RedirectableConn) error {
	g.onRedirectFinished(from, to, conn, false)
	return nil
}

func (g *Group) onRedirectFinished(from, to string, conn RedirectableConn, succeed bool) {
	g.Lock()
	defer g.Unlock()
	fromBackend := g.ensureBackend(from)
	toBackend := g.ensureBackend(to)
	connWrapper := getConnWrapper(conn).Value
	addMigrateMetrics(from, to, connWrapper.redirectReason, succeed, connWrapper.lastRedirect)
	// The connection may be closed when this function is waiting for the lock.
	if connWrapper.phase == phaseClosed {
		return
	}

	if succeed {
		g.removeConn(fromBackend, getConnWrapper(conn))
		g.addConn(toBackend, connWrapper)
		connWrapper.phase = phaseRedirectEnd
	} else {
		fromBackend.connScore++
		toBackend.connScore--
		connWrapper.phase = phaseRedirectFail
	}
}

// OnConnClosed implements ConnEventReceiver.OnConnClosed interface.
func (g *Group) OnConnClosed(addr, redirectingAddr string, conn RedirectableConn) error {
	g.Lock()
	defer g.Unlock()
	backend := g.ensureBackend(addr)
	connWrapper := getConnWrapper(conn)
	// If this connection has not redirected yet, decrease the score of the target backend.
	if redirectingAddr != "" {
		redirectingBackend := g.ensureBackend(redirectingAddr)
		redirectingBackend.connScore--
		metrics.PendingMigrateGuage.WithLabelValues(addr, redirectingAddr, connWrapper.Value.redirectReason).Dec()
	} else {
		backend.connScore--
	}
	g.removeConn(backend, connWrapper)
	connWrapper.Value.phase = phaseClosed
	return nil
}

func (g *Group) redirectConn(conn *connWrapper, fromBackend *backendWrapper, toBackend *backendWrapper,
	reason string, logFields []zap.Field, curTime time.Time) bool {
	// Skip the connection if it's closing.
	fields := []zap.Field{
		zap.Uint64("connID", conn.ConnectionID()),
		zap.String("from", fromBackend.addr),
		zap.String("to", toBackend.addr),
	}
	fields = append(fields, logFields...)
	succeed := conn.Redirect(toBackend)
	if succeed {
		g.lg.Debug("begin redirect connection", fields...)
		fromBackend.connScore--
		toBackend.connScore++
		conn.phase = phaseRedirectNotify
		conn.redirectReason = reason
		metrics.PendingMigrateGuage.WithLabelValues(fromBackend.addr, toBackend.addr, reason).Inc()
	} else {
		// Avoid it to be redirected again immediately.
		conn.phase = phaseRedirectFail
		g.lg.Debug("skip redirecting because it's closing", fields...)
	}
	conn.lastRedirect = curTime
	return succeed
}

func (g *Group) ConnCount() int {
	g.Lock()
	defer g.Unlock()
	j := 0
	for _, backend := range g.backends {
		j += backend.connList.Len()
	}
	return j
}

func (g *Group) SetConfig(cfg *config.Config) {
	g.policy.SetConfig(cfg)
}
