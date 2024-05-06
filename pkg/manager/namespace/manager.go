// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/factor"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	sync.RWMutex
	tpFetcher     observer.TopologyFetcher
	promFetcher   metricsreader.PromInfoFetcher
	metricsReader metricsreader.MetricsReader
	httpCli       *http.Client
	logger        *zap.Logger
	nsm           map[string]*Namespace
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{}
}

func (mgr *NamespaceManager) buildNamespace(cfg *config.Namespace) (*Namespace, error) {
	logger := mgr.logger.With(zap.String("namespace", cfg.Namespace))

	// init BackendFetcher
	var fetcher observer.BackendFetcher
	healthCheckCfg := config.NewDefaultHealthCheckConfig()
	if !reflect.ValueOf(mgr.tpFetcher).IsNil() {
		fetcher = observer.NewPDFetcher(mgr.tpFetcher, logger.Named("be_fetcher"), healthCheckCfg)
	} else {
		fetcher = observer.NewStaticFetcher(cfg.Backend.Instances)
	}

	// init Router
	rt := router.NewScoreBasedRouter(logger.Named("router"))
	hc := observer.NewDefaultHealthCheck(mgr.httpCli, healthCheckCfg, logger.Named("hc"))
	bo := observer.NewDefaultBackendObserver(logger.Named("observer"), healthCheckCfg, fetcher, hc)
	bo.Start(context.Background())
	rt.Init(context.Background(), bo, factor.NewFactorBasedBalance(logger.Named("factor"), mgr.metricsReader))

	return &Namespace{
		name:   cfg.Namespace,
		user:   cfg.Frontend.User,
		bo:     bo,
		router: rt,
	}, nil
}

func (mgr *NamespaceManager) CommitNamespaces(nss []*config.Namespace, nss_delete []bool) error {
	nsm := make(map[string]*Namespace)
	mgr.RLock()
	for k, v := range mgr.nsm {
		nsm[k] = v
	}
	mgr.RUnlock()

	for i, nsc := range nss {
		if nss_delete != nil && nss_delete[i] {
			delete(nsm, nsc.Namespace)
			continue
		}

		ns, err := mgr.buildNamespace(nsc)
		if err != nil {
			return fmt.Errorf("%w: create namespace error, namespace: %s", err, nsc.Namespace)
		}
		nsm[ns.Name()] = ns
	}

	mgr.Lock()
	mgr.nsm = nsm
	mgr.Unlock()
	return nil
}

func (mgr *NamespaceManager) Init(logger *zap.Logger, nscs []*config.Namespace, tpFetcher observer.TopologyFetcher,
	promFetcher metricsreader.PromInfoFetcher, httpCli *http.Client) error {
	mgr.Lock()
	mgr.tpFetcher = tpFetcher
	mgr.promFetcher = promFetcher
	mgr.httpCli = httpCli
	mgr.logger = logger
	healthCheckCfg := config.NewDefaultHealthCheckConfig()
	mgr.metricsReader = metricsreader.NewDefaultMetricsReader(logger.Named("mr"), mgr.promFetcher, healthCheckCfg)
	mgr.Unlock()

	mgr.metricsReader.Start(context.Background())
	return mgr.CommitNamespaces(nscs, nil)
}

func (mgr *NamespaceManager) GetNamespace(nm string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	ns, ok := mgr.nsm[nm]
	return ns, ok
}

func (mgr *NamespaceManager) GetNamespaceByUser(user string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	for _, ns := range mgr.nsm {
		if ns.User() == user {
			return ns, true
		}
	}
	return nil, false
}

func (mgr *NamespaceManager) RedirectConnections() []error {
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

func (mgr *NamespaceManager) Close() error {
	mgr.RLock()
	for _, ns := range mgr.nsm {
		ns.Close()
	}
	mgr.RUnlock()
	mgr.metricsReader.Close()
	return nil
}
