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
	"github.com/pingcap/tiproxy/pkg/manager/backendcluster"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/netutil"
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
	// Match connections based on the local SQL listener port.
	MatchPort
)

var _ ConnEventReceiver = (*Group)(nil)

type routeCheckBackend struct {
	*backendWrapper
	healthy bool
}

func (b routeCheckBackend) Healthy() bool {
	return b.healthy
}

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
	// failoverTargets contains backend pod names or addresses configured in fail-backend-list.
	failoverTargets map[string]struct{}
	failoverTimeout time.Duration
	ignoreFailover  bool
	// To limit the speed of redirection.
	lastRedirectTime time.Time
}

func NewGroup(values []string, bpCreator func(lg *zap.Logger) policy.BalancePolicy, matchType MatchType, lg *zap.Logger) (*Group, error) {
	if len(values) > 0 {
		lg = lg.With(zap.Strings("values", values))
	}
	lg.Info("new group created")

	group := &Group{
		matchType:       matchType,
		lg:              lg,
		values:          values,
		backends:        make(map[string]*backendWrapper),
		failoverTargets: make(map[string]struct{}),
		policy:          bpCreator(lg.Named("policy")),
	}
	err := group.parseValues()
	if err != nil {
		err = errors.Wrapf(err, "failed to parse values")
	}
	return group, err
}

func (g *Group) parseValues() error {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		cidrList, parseErr := netutil.ParseCIDRList(g.values)
		if parseErr != nil {
			return parseErr
		}
		g.cidrList = cidrList
	}
	return nil
}

func (g *Group) Match(clientInfo ClientInfo) bool {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		addr := clientInfo.ProxyAddr
		if g.matchType == MatchClientCIDR {
			addr = clientInfo.ClientAddr
		}
		ip, err := netutil.NetAddr2IP(addr)
		if err != nil {
			g.lg.Error("checking CIDR failed", zap.Stringer("addr", addr), zap.Error(err))
			return false
		}
		contains, err := netutil.CIDRContainsIP(g.cidrList, ip)
		if err != nil {
			g.lg.Error("checking CIDR failed", zap.Stringer("addr", addr), zap.Error(err))
		}
		return contains
	}
	return true
}

func (g *Group) EqualValues(values []string) bool {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR, MatchPort:
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

// Intersect returns if any cidrs are the same.
// In next-gen, backend cidrs may increase or decrease but they stay in the same group.
// E.g. enable public endpoint (3 cidrs) -> enable private endpoint (6 cidrs) -> disable public endpoint (3 cidrs).
func (g *Group) Intersect(values []string) bool {
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR, MatchPort:
		for _, v := range g.values {
			if slices.Contains(values, v) {
				return true
			}
		}
		return false
	}
	return false
}

// Backend CIDRs may change anytime.
func (g *Group) RefreshCidr() {
	g.Lock()
	defer g.Unlock()
	switch g.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		valueMap := make(map[string]struct{}, len(g.values))
		for _, b := range g.backends {
			cidrs := b.Cidr()
			for _, cidr := range cidrs {
				valueMap[cidr] = struct{}{}
			}
		}
		values := make([]string, 0, len(valueMap))
		for k := range valueMap {
			values = append(values, k)
		}
		g.values = values
		if err := g.parseValues(); err != nil {
			g.lg.Error("failed to parse values", zap.Error(err))
		}
	}
}

func (g *Group) AddBackend(backendID string, backend *backendWrapper) {
	g.Lock()
	defer g.Unlock()
	g.backends[backendID] = backend
	backend.group = g
}

func (g *Group) RemoveBackend(backendID string) {
	g.Lock()
	defer g.Unlock()
	delete(g.backends, backendID)
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

func (g *Group) routeableObservedBackendsLocked(failoverBackendIDs map[string]struct{}) []policy.BackendCtx {
	backends := make([]policy.BackendCtx, 0, len(g.backends))
	for _, backend := range g.backends {
		if !backend.ObservedHealthy() {
			continue
		}
		healthy := true
		if failoverBackendIDs != nil {
			_, healthy = failoverBackendIDs[backend.ID()]
			healthy = !healthy
		}
		backends = append(backends, routeCheckBackend{
			backendWrapper: backend,
			healthy:        healthy,
		})
	}
	return g.policy.RouteableBackends(backends)
}

func (g *Group) backendInFailoverListLocked(backend *backendWrapper) bool {
	_, active := g.failoverTargets[backend.PodName()]
	if !active {
		_, active = g.failoverTargets[backend.Addr()]
	}
	return active
}

func (g *Group) setFailoverConfigLocked(cfg *config.Config) {
	targets := make(map[string]struct{}, len(cfg.Proxy.FailBackendList))
	for _, backend := range cfg.Proxy.FailBackendList {
		targets[backend] = struct{}{}
	}
	g.failoverTargets = targets
	g.failoverTimeout = time.Duration(cfg.Proxy.FailoverTimeout) * time.Second
}

func (g *Group) updateFailoverLocked(now time.Time) {
	failoverBackendIDs := make(map[string]struct{}, len(g.backends))
	for _, backend := range g.backends {
		if g.backendInFailoverListLocked(backend) {
			failoverBackendIDs[backend.ID()] = struct{}{}
		}
	}

	routeable := g.routeableObservedBackendsLocked(nil)
	if len(routeable) > 0 {
		remaining := g.routeableObservedBackendsLocked(failoverBackendIDs)
		if len(remaining) == 0 {
			matched := 0
			for _, backend := range routeable {
				if _, ok := failoverBackendIDs[backend.ID()]; ok {
					matched++
				}
			}
			if !g.ignoreFailover {
				g.lg.Warn("fail-backend-list would leave no routeable backend in group, ignore the list for this group",
					zap.Int("routeable_backend_count", len(routeable)),
					zap.Int("matched_routeable_backend_count", matched))
			}
			g.ignoreFailover = true
			clear(failoverBackendIDs)
		} else {
			g.ignoreFailover = false
		}
	} else {
		g.ignoreFailover = false
	}

	for _, backend := range g.backends {
		_, active := failoverBackendIDs[backend.ID()]
		since := time.Time{}
		if active {
			since = now
		}
		changed, since := backend.setFailover(since)
		if !changed {
			continue
		}
		fields := []zap.Field{
			zap.String("backend_addr", backend.Addr()),
			zap.String("backend_pod", backend.PodName()),
			zap.Duration("failover_timeout", g.failoverTimeout),
		}
		if active {
			fields = append(fields, zap.Time("failover_since", since))
			g.lg.Warn("backend enters failover", fields...)
			continue
		}
		g.lg.Info("backend exits failover", fields...)
	}
}

func (g *Group) UpdateFailover(now time.Time) {
	g.Lock()
	defer g.Unlock()
	g.updateFailoverLocked(now)
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
			if backend.ID() == e.ID() {
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
		if conn.forceClosing {
			continue
		}
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
	backend := g.ensureBackend(backendInst.ID())
	if succeed {
		connWrapper := &connWrapper{
			RedirectableConn: conn,
			createTime:       time.Now(),
			phase:            phaseNotRedirected,
			forceClosing:     false,
		}
		g.addConn(backend, connWrapper)
		conn.SetEventReceiver(g)
	} else {
		backend.connScore--
	}
}

func (g *Group) CloseTimedOutFailoverConnections(now time.Time) {
	g.Lock()
	defer g.Unlock()
	for _, backend := range g.backends {
		since := backend.FailoverSince()
		if since.IsZero() {
			continue
		}
		if g.failoverTimeout > 0 && since.Add(g.failoverTimeout).After(now) {
			continue
		}
		for ele := backend.connList.Front(); ele != nil; ele = ele.Next() {
			conn := ele.Value
			if conn.phase == phaseClosed || conn.forceClosing {
				continue
			}
			fields := []zap.Field{
				zap.Uint64("connID", conn.ConnectionID()),
				zap.String("backend_addr", backend.addr),
				zap.String("backend_pod", backend.PodName()),
				zap.Duration("failover_timeout", g.failoverTimeout),
				zap.Duration("failover_elapsed", now.Sub(since)),
			}
			if conn.ForceClose() {
				conn.forceClosing = true
				g.lg.Info("force close connection on failover backend", fields...)
			}
		}
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

func (g *Group) ensureBackend(backendID string) *backendWrapper {
	backend, ok := g.backends[backendID]
	if ok {
		return backend
	}
	// The backend should always exist if it will be needed. Add a warning and add it back.
	g.lg.Warn("backend is not found in the router", zap.String("backend_id", backendID), zap.Stack("stack"))
	// Try to parse the IP from the backendID. It's generally not suggested to parse it, but in this
	// strange case we tried our best to recover and make the backend ip valid.
	// For the formats of backendID, ref `backend_id.go`. It's generated and recorded in `GetTiDBTopology`
	// for the first time.
	_, addr := backendcluster.ParseBackendID(backendID)
	ip, _, _ := net.SplitHostPort(addr)
	backend = newBackendWrapper(backendID, observer.BackendHealth{
		BackendInfo: observer.BackendInfo{
			Addr:       addr,
			IP:         ip,
			StatusPort: 10080, // impossible anyway
		},
		SupportRedirection: true,
		Healthy:            false,
	})
	g.backends[backendID] = backend
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
func (g *Group) OnConnClosed(backendID, redirectingBackendID string, conn RedirectableConn) error {
	g.Lock()
	defer g.Unlock()
	backend := g.ensureBackend(backendID)
	connWrapper := getConnWrapper(conn)
	// If this connection has not redirected yet, decrease the score of the target backend.
	if redirectingBackendID != "" {
		redirectingBackend := g.ensureBackend(redirectingBackendID)
		redirectingBackend.connScore--
		metrics.PendingMigrateGuage.WithLabelValues(backendID, redirectingBackendID, connWrapper.Value.redirectReason).Dec()
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
	if !conn.lastRedirect.IsZero() {
		fields = append(fields, zap.Duration("since_last_redirect", curTime.Sub(conn.lastRedirect)))
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
	g.Lock()
	defer g.Unlock()
	g.policy.SetConfig(cfg)
	g.setFailoverConfigLocked(cfg)
	g.updateFailoverLocked(time.Now())
}
