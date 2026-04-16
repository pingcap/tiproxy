// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
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
	logger *zap.Logger
	// Tests may use a different balance policy.
	bpCreator  func(lg *zap.Logger) policy.BalancePolicy
	observer   observer.BackendObserver
	healthCh   <-chan observer.HealthResult
	cfgGetter  config.ConfigGetter
	cfgCh      <-chan *config.Config
	cancelFunc context.CancelFunc
	wg         waitgroup.WaitGroup
	// Some backends may not belonging to any group because their labels are not set yet.
	backends map[string]*backendWrapper
	// TODO: sort the groups to leverage binary search.
	groups []*Group
	// portConflictDetector dispatches listener ports to cluster-scoped backend groups.
	portConflictDetector *portConflictDetector
	// The routing rule for categorizing backends to groups.
	matchType           MatchType
	observeError        error
	routeLabelName      string
	routeSelfLabelValue string
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
	// The backend supports redirection only when they have signing certs.
	supportRedirection bool
	failoverTargets    map[string]struct{}
	failoverTimeout    time.Duration
	ignoreFailoverList bool
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:          logger,
		backends:        make(map[string]*backendWrapper),
		groups:          make([]*Group, 0),
		failoverTargets: make(map[string]struct{}),
	}
}

func (r *ScoreBasedRouter) Init(ctx context.Context, ob observer.BackendObserver, bpCreator func(lg *zap.Logger) policy.BalancePolicy,
	cfgGetter config.ConfigGetter, cfgCh <-chan *config.Config) {
	r.observer = ob
	r.bpCreator = bpCreator
	r.cfgGetter = cfgGetter
	r.cfgCh = cfgCh
	r.healthCh = r.observer.Subscribe("score_based_router")
	cfg := cfgGetter.GetConfig()

	r.matchType = MatchAll
	switch strings.ToLower(cfg.Balance.RoutingRule) {
	case config.MatchClientCIDRStr:
		r.matchType = MatchClientCIDR
	case config.MatchProxyCIDRStr:
		r.matchType = MatchProxyCIDR
	case config.MatchPortStr:
		r.matchType = MatchPort
	case "":
	default:
		r.logger.Error("unsupported routing rule, use the default rule", zap.String("rule", cfg.Balance.RoutingRule))
	}
	r.Lock()
	r.setRouteConstraintsConfigLocked(cfg)
	r.setFailoverConfigLocked(cfg)
	r.Unlock()

	childCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	// Failing to route connections may cause even more serious problems than TiProxy reboot, so we don't recover panics.
	r.wg.Run(func() {
		r.rebalanceLoop(childCtx)
	}, r.logger)
}

// GetBackendSelector implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) GetBackendSelector(clientInfo ClientInfo) BackendSelector {
	var group *Group
	return BackendSelector{
		routeOnce: func(excluded []BackendInst) (backend BackendInst, err error) {
			// Prevent the group from being removed after it's chosen. In that case,
			// the connection will be on a orphan group.
			router.Lock()
			defer func() {
				router.Unlock()
				if errors.Is(err, ErrNoBackend) {
					// No available backends, maybe the health check result is outdated during rolling restart.
					// Refresh the backends asynchronously in this case.
					if router.observer != nil {
						router.observer.Refresh()
					}
				}
			}()
			if router.observeError != nil {
				err = router.observeError
				return
			}
			// The group may change from round to round because the backends are updated.
			group, err = router.routeToGroup(clientInfo)
			if err != nil {
				return
			}
			if group == nil {
				err = ErrNoBackend
				return
			}
			// The router may remove this group concurrently, make sure the group can be accessed after it's removed.
			var backendCtx policy.BackendCtx
			backendCtx, err = group.Route(excluded)
			if err == nil && backendCtx != nil {
				backend = backendCtx.(BackendInst)
			}
			return
		},
		onCreate: func(backend BackendInst, conn RedirectableConn, succeed bool) {
			group.onCreateConn(backend, conn, succeed)
		},
	}
}

func (router *ScoreBasedRouter) HealthyBackendCount() int {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return 0
	}

	count := 0
	for _, backend := range router.backends {
		if backend.Healthy() {
			count++
		}
	}
	return count
}

// called in the lock
func (router *ScoreBasedRouter) routeToGroup(clientInfo ClientInfo) (*Group, error) {
	if router.matchType == MatchPort {
		if router.portConflictDetector == nil {
			return nil, nil
		}
		return router.portConflictDetector.groupFor(clientInfo.ListenerPort)
	}
	// TODO: binary search
	for _, group := range router.groups {
		if group.Match(clientInfo) {
			return group, nil
		}
	}
	return nil, nil
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
	for _, group := range router.groups {
		if err := group.RedirectConnections(); err != nil {
			return err
		}
	}
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
	for backendID, backend := range router.backends {
		if _, ok := backends[backendID]; !ok {
			health := backend.getHealth()
			router.logger.Debug("backend is removed from the list, add it back to router",
				zap.String("backend_id", backendID), zap.String("addr", backend.Addr()), zap.Stringer("health", &health))
			backends[backendID] = &observer.BackendHealth{
				BackendInfo:        backend.GetBackendInfo(),
				SupportRedirection: backend.SupportRedirection(),
				Healthy:            false,
				PingErr:            errors.New("removed from backend list"),
			}
		}
	}
	var serverVersion string
	supportRedirection := true
	now := time.Now()
	for backendID, health := range backends {
		backend, ok := router.backends[backendID]
		if !ok && health.Healthy {
			router.logger.Debug("add new backend to router",
				zap.String("backend_id", backendID), zap.String("addr", health.Addr), zap.Stringer("health", health))
			backend = newBackendWrapper(backendID, *health)
			router.backends[backendID] = backend
			serverVersion = health.ServerVersion
		} else if ok {
			if !health.Equals(backend.getHealth()) {
				router.logger.Debug("update backend in router",
					zap.String("backend_id", backendID), zap.String("addr", health.Addr), zap.Stringer("health", health))
			}
			backend.setHealth(*health)
			if health.Healthy {
				serverVersion = health.ServerVersion
			}
		} else {
			router.logger.Debug("unhealthy backend is not in router",
				zap.String("backend_id", backendID), zap.String("addr", health.Addr), zap.Stringer("health", health))
		}
		supportRedirection = health.SupportRedirection && supportRedirection
	}

	router.updateGroups()
	router.updateBackendFailoverLocked(now)
	if len(serverVersion) > 0 {
		router.serverVersion = serverVersion
	}
	if router.supportRedirection != supportRedirection {
		router.logger.Info("updated supporting redirection", zap.Bool("support", supportRedirection))
		router.supportRedirection = supportRedirection
	}
}

func matchPortValue(clusterName, port string) string {
	if clusterName == "" {
		return port
	}
	return fmt.Sprintf("%s:%s", clusterName, port)
}

func (router *ScoreBasedRouter) backendGroupValues(backend *backendWrapper) []string {
	switch router.matchType {
	case MatchClientCIDR, MatchProxyCIDR:
		return backend.Cidr()
	case MatchPort:
		port := backend.TiProxyPort()
		if port != "" {
			return []string{matchPortValue(backend.ClusterName(), port)}
		}
	}
	return nil
}

func (router *ScoreBasedRouter) rebuildPortConflictDetector() {
	if router.matchType != MatchPort {
		router.portConflictDetector = nil
		return
	}
	detector := newPortConflictDetector()
	for _, group := range router.groups {
		for _, value := range group.values {
			clusterName, port, ok := strings.Cut(value, ":")
			if !ok {
				port = value
				clusterName = ""
			}
			detector.bind(port, clusterName, group)
		}
	}
	router.portConflictDetector = detector
}

// Update the groups after the backend list is updated.
// called in the lock.
func (router *ScoreBasedRouter) updateGroups() {
	for _, backend := range router.backends {
		// If connList.Len() == 0, there won't be any outgoing connections.
		// And if also connScore == 0, there won't be any incoming connections.
		if !backend.ObservedHealthy() && backend.connList.Len() == 0 && backend.connScore <= 0 {
			delete(router.backends, backend.id)
			if backend.group != nil {
				backend.group.RemoveBackend(backend.id)
				// remove empty groups
				if backend.group.Empty() {
					router.groups = slices.DeleteFunc(router.groups, func(g *Group) bool {
						return g == backend.group
					})
				}
			}
			continue
		}
		// If the labels were correctly set, we won't update its group even if the labels change.
		if backend.group != nil {
			switch router.matchType {
			case MatchClientCIDR, MatchProxyCIDR, MatchPort:
				values := router.backendGroupValues(backend)
				if !backend.group.EqualValues(values) {
					router.logger.Warn("backend routing values changed, keep the existing group until it is removed",
						zap.String("backend_id", backend.id),
						zap.String("addr", backend.Addr()),
						zap.Strings("current_values", values),
						zap.Strings("group_values", backend.group.values))
				}
			}
			continue
		}

		// If the backend is not in any group, add it to a new group if its label is set.
		// In operator deployment, the labels are set dynamically.
		var group *Group
		switch router.matchType {
		case MatchAll:
			if len(router.groups) == 0 {
				group, _ = NewGroup(nil, router.bpCreator, router.matchType, router.logger)
				router.groups = append(router.groups, group)
			}
			group = router.groups[0]
		case MatchClientCIDR, MatchProxyCIDR, MatchPort:
			values := router.backendGroupValues(backend)
			if len(values) == 0 {
				break
			}
			for _, g := range router.groups {
				if g.Intersect(values) {
					group = g
					break
				}
			}
			if group == nil {
				g, err := NewGroup(values, router.bpCreator, router.matchType, router.logger)
				if err == nil {
					group = g
					router.groups = append(router.groups, group)
				}
				// maybe too many logs, ignore the error now
			}
		}
		if group == nil {
			continue
		}
		group.AddBackend(backend.id, backend)
	}
	for _, group := range router.groups {
		group.RefreshCidr()
	}
	router.rebuildPortConflictDetector()
}

func (router *ScoreBasedRouter) rebalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(rebalanceInterval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case healthResults, ok := <-router.healthCh:
			if !ok {
				router.logger.Warn("health channel is closed, stop watching channel")
				router.healthCh = nil
				continue
			}
			router.updateBackendHealth(healthResults)
		case cfg, ok := <-router.cfgCh:
			if !ok {
				router.logger.Warn("config channel is closed, stop watching channel")
				router.cfgCh = nil
				continue
			}
			router.setConfig(cfg)
		case <-ticker.C:
			router.rebalance(ctx)
		}
	}
}

func (router *ScoreBasedRouter) setConfig(cfg *config.Config) {
	router.Lock()
	defer router.Unlock()
	router.setRouteConstraintsConfigLocked(cfg)
	router.setFailoverConfigLocked(cfg)
	for _, group := range router.groups {
		group.SetConfig(cfg)
	}
}

// Rebalance every a short time and migrate only a few connections in each round so that:
// - The lock is not held too long
// - The connections are migrated to different backends
func (router *ScoreBasedRouter) rebalance(ctx context.Context) {
	router.Lock()
	defer router.Unlock()

	if router.supportRedirection {
		for _, group := range router.groups {
			group.Balance(ctx)
		}
	}
	for _, group := range router.groups {
		group.CloseTimedOutFailoverConnections(time.Now(), router.failoverTimeout)
	}
}

func (router *ScoreBasedRouter) setFailoverConfigLocked(cfg *config.Config) {
	failoverTargets := make(map[string]struct{}, len(cfg.Proxy.FailBackendList))
	for _, backend := range cfg.Proxy.FailBackendList {
		failoverTargets[backend] = struct{}{}
	}
	router.failoverTargets = failoverTargets
	router.failoverTimeout = time.Duration(cfg.Proxy.FailoverTimeout) * time.Second
	router.updateBackendFailoverLocked(time.Now())
}

func (router *ScoreBasedRouter) backendInFailoverListLocked(backend *backendWrapper) bool {
	_, active := router.failoverTargets[backend.PodName()]
	if !active {
		// Also support IP:port.
		_, active = router.failoverTargets[backend.Addr()]
	}
	return active
}

func (router *ScoreBasedRouter) setRouteConstraintsConfigLocked(cfg *config.Config) {
	router.routeLabelName = cfg.Balance.LabelName
	router.routeSelfLabelValue = ""
	if router.routeLabelName != "" && cfg.Labels != nil {
		router.routeSelfLabelValue = cfg.Labels[router.routeLabelName]
	}
}

func (router *ScoreBasedRouter) backendRouteableByLabelLocked(backend *backendWrapper) bool {
	if router.routeLabelName == "" || router.routeSelfLabelValue == "" {
		return true
	}
	labels := backend.GetBackendInfo().Labels
	return labels != nil && labels[router.routeLabelName] == router.routeSelfLabelValue
}

func (router *ScoreBasedRouter) shouldIgnoreFailoverListLocked(failoverBackendIDs map[string]struct{}) (ignore bool, healthyBackends int, matchedHealthyBackends int, groupValues []string) {
	type groupStats struct {
		healthy int
		matched int
	}

	globalHealthyBackends := 0
	globalMatchedHealthyBackends := 0
	statsByGroup := make(map[*Group]*groupStats, len(router.groups))
	for _, backend := range router.backends {
		if !backend.ObservedHealthy() {
			continue
		}
		if !router.backendRouteableByLabelLocked(backend) {
			continue
		}
		globalHealthyBackends++
		_, matched := failoverBackendIDs[backend.ID()]
		if matched {
			globalMatchedHealthyBackends++
		}
		if backend.group == nil {
			continue
		}
		stats := statsByGroup[backend.group]
		if stats == nil {
			stats = &groupStats{}
			statsByGroup[backend.group] = stats
		}
		stats.healthy++
		if matched {
			stats.matched++
		}
	}

	if len(statsByGroup) == 0 {
		return globalHealthyBackends > 0 && globalMatchedHealthyBackends == globalHealthyBackends, globalHealthyBackends, globalMatchedHealthyBackends, nil
	}

	for group, stats := range statsByGroup {
		if stats.healthy > 0 && stats.matched == stats.healthy {
			return true, stats.healthy, stats.matched, group.values
		}
	}
	return false, globalHealthyBackends, globalMatchedHealthyBackends, nil
}

func (router *ScoreBasedRouter) updateBackendFailoverLocked(now time.Time) {
	failoverBackendIDs := make(map[string]struct{}, len(router.backends))
	for _, backend := range router.backends {
		if router.backendInFailoverListLocked(backend) {
			failoverBackendIDs[backend.ID()] = struct{}{}
		}
	}
	ignoreFailoverList, healthyBackends, matchedHealthyBackends, groupValues := router.shouldIgnoreFailoverListLocked(failoverBackendIDs)
	if ignoreFailoverList {
		if !router.ignoreFailoverList {
			fields := []zap.Field{
				zap.Int("healthy_backend_count", healthyBackends),
				zap.Int("matched_healthy_backend_count", matchedHealthyBackends),
			}
			if len(groupValues) > 0 {
				fields = append(fields, zap.Strings("group_values", groupValues))
			}
			router.logger.Warn("fail-backend-list would leave no healthy backend, ignore the whole list", fields...)
		}
		router.ignoreFailoverList = true
		clear(failoverBackendIDs)
	} else {
		router.ignoreFailoverList = false
	}
	for _, backend := range router.backends {
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
			zap.Duration("failover_timeout", router.failoverTimeout),
		}
		if active {
			fields = append(fields, zap.Time("failover_since", since))
			router.logger.Warn("backend enters failover", fields...)
			continue
		}
		router.logger.Info("backend exits failover", fields...)
	}
}

func (router *ScoreBasedRouter) ConnCount() int {
	router.Lock()
	defer router.Unlock()
	j := 0
	for _, group := range router.groups {
		j += group.ConnCount()
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
}
