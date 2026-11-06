// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/factor"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	mconfig "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"go.uber.org/zap"
)

type NamespaceManager interface {
	SetBackendNetwork(backendNetwork observer.BackendNetwork)
	Init(logger *zap.Logger, nscs []*config.Namespace, tpFetcher observer.TopologyFetcher,
		promFetcher metricsreader.PromInfoFetcher, httpCli *http.Client, cfgMgr *mconfig.ConfigManager,
		metricsReader metricsreader.MetricsQuerier) error
	// CommitNamespaces applies namespace configs to the live routing table.
	// A nil nssDelete means nss is the complete desired set: every live namespace
	// absent from nss is removed and stops accepting new traffic.
	// A non-nil nssDelete must have the same length as nss. A true entry removes
	// that namespace; a false entry creates or replaces it. Namespaces that are
	// not listed stay in place.
	CommitNamespaces(nss []*config.Namespace, nssDelete []bool) error
	GetNamespace(nm string) (*Namespace, bool)
	GetNamespaceByUser(user string) (*Namespace, bool)
	RedirectConnections() []error
	Ready() bool
	Close() error
}

type namespaceManager struct {
	sync.RWMutex
	nsm            map[string]*Namespace
	tpFetcher      observer.TopologyFetcher
	promFetcher    metricsreader.PromInfoFetcher
	metricsReader  metricsreader.MetricsQuerier
	httpCli        *http.Client
	backendNetwork observer.BackendNetwork
	logger         *zap.Logger
	cfgMgr         *mconfig.ConfigManager
	// newNamespace, when set, replaces the real namespace constructor.
	newNamespace func(cfg *config.Namespace) (*Namespace, error)
}

func NewNamespaceManager() *namespaceManager {
	return &namespaceManager{}
}

func (mgr *namespaceManager) buildNamespace(cfg *config.Namespace) (*Namespace, error) {
	if mgr.newNamespace != nil {
		return mgr.newNamespace(cfg)
	}
	logger := mgr.logger.With(zap.String("namespace", cfg.Namespace))

	healthCheckCfg := config.NewDefaultHealthCheckConfig()
	dynamicFetcher := observer.NewPDFetcher(mgr.tpFetcher, logger.Named("be_fetcher"), healthCheckCfg)
	staticFetcher := observer.NewStaticFetcher(cfg.Backend.Instances)
	fetcher := observer.NewFallbackFetcher(mgr.tpFetcher, dynamicFetcher, staticFetcher)

	// init Router
	rt := router.NewScoreBasedRouter(logger.Named("router"))
	hc := observer.NewDefaultHealthCheckWithNetwork(mgr.backendNetwork, healthCheckCfg, logger.Named("hc"))
	bo := observer.NewDefaultBackendObserver(logger.Named("observer"), healthCheckCfg, fetcher, hc, mgr.cfgMgr)
	bo.Start(context.Background())
	bpCreator := func(lg *zap.Logger) policy.BalancePolicy {
		policy := factor.NewFactorBasedBalance(lg, mgr.metricsReader)
		policy.Init(mgr.cfgMgr.GetConfig())
		return policy
	}
	rt.Init(context.Background(), bo, bpCreator, mgr.cfgMgr, mgr.cfgMgr.WatchConfig())

	return &Namespace{
		name:   cfg.Namespace,
		user:   cfg.Frontend.User,
		bo:     bo,
		router: rt,
	}, nil
}

func (mgr *namespaceManager) CommitNamespaces(nss []*config.Namespace, nssDelete []bool) error {
	if nssDelete != nil && len(nssDelete) != len(nss) {
		return fmt.Errorf("namespace delete flags length %d does not match namespaces length %d", len(nssDelete), len(nss))
	}
	// A nil delete slice is a full snapshot of what should be routing. Anything
	// still live but missing from that snapshot must be dropped, otherwise a
	// namespace removed from storage keeps serving until process restart.
	fullSnapshot := nssDelete == nil

	built := make(map[string]*Namespace, len(nss))
	deleted := make(map[string]struct{})
	closeBuilt := func() {
		for _, ns := range built {
			ns.Close()
		}
		clear(built)
	}
	for i, nsc := range nss {
		if nsc == nil || nsc.Namespace == "" {
			closeBuilt()
			return fmt.Errorf("invalid namespace at index %d", i)
		}
		if !fullSnapshot && nssDelete[i] {
			deleted[nsc.Namespace] = struct{}{}
			if prev := built[nsc.Namespace]; prev != nil {
				prev.Close()
				delete(built, nsc.Namespace)
			}
			continue
		}
		delete(deleted, nsc.Namespace)
		ns, err := mgr.buildNamespace(nsc)
		if err != nil {
			closeBuilt()
			return fmt.Errorf("%w: create namespace error, namespace: %s", err, nsc.Namespace)
		}
		if prev := built[nsc.Namespace]; prev != nil {
			prev.Close()
		}
		built[nsc.Namespace] = ns
	}

	mgr.Lock()
	next := make(map[string]*Namespace, len(built)+len(mgr.nsm))
	if !fullSnapshot {
		for name, ns := range mgr.nsm {
			if _, drop := deleted[name]; drop {
				continue
			}
			next[name] = ns
		}
	}
	for name, ns := range built {
		next[name] = ns
	}
	var stale []*Namespace
	var removed []string
	for name, old := range mgr.nsm {
		cur, ok := next[name]
		if !ok {
			removed = append(removed, name)
			stale = append(stale, old)
			continue
		}
		if cur != old {
			stale = append(stale, old)
		}
	}
	mgr.nsm = next
	logger := mgr.logger
	mgr.Unlock()

	if logger != nil {
		for _, name := range removed {
			logger.Info("removed namespace from routing table", zap.String("namespace", name))
		}
	}
	for _, ns := range stale {
		ns.Close()
	}
	return nil
}

func (mgr *namespaceManager) Init(logger *zap.Logger, nscs []*config.Namespace, tpFetcher observer.TopologyFetcher,
	promFetcher metricsreader.PromInfoFetcher, httpCli *http.Client, cfgMgr *mconfig.ConfigManager,
	metricsReader metricsreader.MetricsQuerier) error {
	mgr.Lock()
	mgr.tpFetcher = tpFetcher
	mgr.promFetcher = promFetcher
	mgr.httpCli = httpCli
	mgr.logger = logger
	mgr.cfgMgr = cfgMgr
	mgr.metricsReader = metricsReader
	mgr.Unlock()
	return mgr.CommitNamespaces(nscs, nil)
}

func (mgr *namespaceManager) SetBackendNetwork(backendNetwork observer.BackendNetwork) {
	mgr.Lock()
	mgr.backendNetwork = backendNetwork
	mgr.Unlock()
}

func (mgr *namespaceManager) GetNamespace(nm string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	ns, ok := mgr.nsm[nm]
	return ns, ok
}

func (mgr *namespaceManager) GetNamespaceByUser(user string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	for _, ns := range mgr.nsm {
		if ns.User() == user {
			return ns, true
		}
	}
	return nil, false
}

func (mgr *namespaceManager) RedirectConnections() []error {
	mgr.RLock()
	defer mgr.RUnlock()

	var errs []error
	for _, ns := range mgr.nsm {
		err1 := ns.GetRouter().RedirectConnections()
		if err1 != nil {
			errs = append(errs, err1)
		}
	}
	return errs
}

func (mgr *namespaceManager) Ready() bool {
	mgr.RLock()
	defer mgr.RUnlock()
	if len(mgr.nsm) == 0 {
		return false
	}
	for _, ns := range mgr.nsm {
		if ns.GetRouter().HealthyBackendCount() <= 0 {
			return false
		}
	}
	return true
}

func (mgr *namespaceManager) Close() error {
	mgr.RLock()
	for _, ns := range mgr.nsm {
		ns.Close()
	}
	mgr.RUnlock()
	return nil
}
