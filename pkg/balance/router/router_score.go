// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
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
	// The routing rule for categorizing backends to groups.
	matchType    MatchType
	observeError error
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
	// The backend supports redirection only when they have signing certs.
	supportRedirection bool
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:   logger,
		backends: make(map[string]*backendWrapper),
		groups:   make([]*Group, 0),
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
	case "cidr":
		r.matchType = MatchCIDR
	case "":
	default:
		r.logger.Error("unsupported routing rule, use the default rule", zap.String("rule", cfg.Balance.RoutingRule))
	}

	childCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	// Failing to route connections may cause even more serious problems than TiProxy reboot, so we don't recover panics.
	r.wg.Run(func() {
		r.rebalanceLoop(childCtx)
	}, r.logger)
}

// GetBackendSelector implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) GetBackendSelector() BackendSelector {
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
			// TODO: fill ip
			group = router.routeToGroup("")
			if group == nil {
				err = ErrNoBackend
				return
			}
			// The router may remove this group concurrently, make sure the group can be accessed after it's removed.
			backend, err = group.Route(excluded)
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
func (router *ScoreBasedRouter) routeToGroup(ip string) *Group {
	// TODO: binary search
	for _, group := range router.groups {
		if group.Match(ip) {
			return group
		}
	}
	return nil
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
	for addr, backend := range router.backends {
		if _, ok := backends[addr]; !ok {
			health := backend.getHealth()
			router.logger.Debug("backend is removed from the list, add it back to router", zap.String("addr", addr), zap.Stringer("health", &health))
			backends[addr] = &observer.BackendHealth{
				BackendInfo:        backend.GetBackendInfo(),
				SupportRedirection: backend.SupportRedirection(),
				Healthy:            false,
				PingErr:            errors.New("removed from backend list"),
			}
		}
	}
	var serverVersion string
	supportRedirection := true
	for addr, health := range backends {
		backend, ok := router.backends[addr]
		if !ok && health.Healthy {
			router.logger.Debug("add new backend to router", zap.String("addr", addr), zap.Stringer("health", health))
			router.backends[addr] = newBackendWrapper(addr, *health)
			serverVersion = health.ServerVersion
		} else if ok {
			if !health.Equals(backend.getHealth()) {
				router.logger.Debug("update backend in router", zap.String("addr", addr), zap.Stringer("health", health))
			}
			backend.setHealth(*health)
			if health.Healthy {
				serverVersion = health.ServerVersion
			}
		} else {
			router.logger.Debug("unhealthy backend is not in router", zap.String("addr", addr), zap.Stringer("health", health))
		}
		supportRedirection = health.SupportRedirection && supportRedirection
	}

	router.updateGroups()
	if len(serverVersion) > 0 {
		router.serverVersion = serverVersion
	}
	if router.supportRedirection != supportRedirection {
		router.logger.Info("updated supporting redirection", zap.Bool("support", supportRedirection))
		router.supportRedirection = supportRedirection
	}
}

// Update the groups after the backend list is updated.
// called in the lock.
func (router *ScoreBasedRouter) updateGroups() {
	for _, backend := range router.backends {
		// If connList.Len() == 0, there won't be any outgoing connections.
		// And if also connScore == 0, there won't be any incoming connections.
		if !backend.Healthy() && backend.connList.Len() == 0 && backend.connScore <= 0 {
			delete(router.backends, backend.addr)
			if backend.group != nil {
				backend.group.RemoveBackend(backend.addr)
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
		case MatchCIDR:
			labels := backend.getHealth().Labels
			if len(labels) == 0 {
				break
			}
			cidr := labels["cidr"]
			if len(cidr) == 0 {
				break
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
			if len(cidrs) == 0 {
				break
			}
			for _, g := range router.groups {
				if g.EqualValues(cidrs) {
					group = g
					break
				}
			}
			if group == nil {
				g, err := NewGroup(cidrs, router.bpCreator, router.matchType, router.logger)
				if err == nil {
					group = g
					router.groups = append(router.groups, group)
				}
				// maybe too many logs, ignore the error now
			}
		}
		if group != nil {
			group.AddBackend(backend.addr, backend)
		}
	}
}

func (router *ScoreBasedRouter) rebalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(rebalanceInterval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case healthResults := <-router.healthCh:
			router.updateBackendHealth(healthResults)
		case cfg := <-router.cfgCh:
			router.setConfig(cfg)
		case <-ticker.C:
			router.rebalance(ctx)
		}
	}
}

func (router *ScoreBasedRouter) setConfig(cfg *config.Config) {
	router.Lock()
	defer router.Unlock()
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

	if !router.supportRedirection {
		return
	}
	for _, group := range router.groups {
		group.Balance(ctx)
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
