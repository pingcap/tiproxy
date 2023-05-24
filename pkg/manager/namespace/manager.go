// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	"github.com/pingcap/TiProxy/pkg/manager/infosync"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	sync.RWMutex
	logger  *zap.Logger
	certmgr *cert.CertManager
	cfg     *config.Config
	ctx     context.Context
	// protected by mutext
	nsm map[string]*Namespace
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{}
}

func (mgr *NamespaceManager) buildNamespace(cfg config.Namespace) (*Namespace, error) {
	logger := mgr.logger.With(zap.String("namespace", cfg.Name))

	var fetcher router.BackendFetcher
	var is *infosync.InfoSyncer
	switch cfg.Backend.SelectorType {
	case "pd":
		client, err := infosync.InitEtcdClient(logger.Named("pd"), cfg.Backend.Instances, mgr.certmgr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		fetcher = router.NewPDFetcher(client, logger.Named("be_fetcher"), config.NewDefaultHealthCheckConfig())
		is = infosync.NewInfoSyncer(client, logger.Named("infosync"))
		if err = is.Init(mgr.ctx, mgr.cfg); err != nil {
			return nil, errors.WithStack(err)
		}
	case "random":
		fetcher = router.NewStaticFetcher(cfg.Backend.Instances)
	}
	rt := router.NewScoreBasedRouter(logger.Named("router"))
	if err := rt.Init(&http.Client{
		Transport: &http.Transport{
			TLSClientConfig: mgr.certmgr.ClusterTLS(),
		},
	}, fetcher, config.NewDefaultHealthCheckConfig()); err != nil {
		return nil, errors.Errorf("build router error: %w", err)
	}
	return &Namespace{
		name:   cfg.Name,
		user:   cfg.Frontend.User,
		router: rt,
	}, nil
}

func (mgr *NamespaceManager) CommitNamespaces(nss []config.Namespace, nss_delete []bool) error {
	nsm := make(map[string]*Namespace)
	mgr.RLock()
	for k, v := range mgr.nsm {
		nsm[k] = v
	}
	mgr.RUnlock()

	for i, nsc := range nss {
		if nss_delete != nil && nss_delete[i] {
			delete(nsm, nsc.Name)
			continue
		}

		ns, err := mgr.buildNamespace(nsc)
		if err != nil {
			return fmt.Errorf("%w: create namespace error, namespace: %s", err, nsc.Name)
		}
		nsm[ns.Name()] = ns
	}

	mgr.Lock()
	mgr.nsm = nsm
	mgr.Unlock()
	return nil
}

func (mgr *NamespaceManager) Init(ctx context.Context, logger *zap.Logger, nscs []config.Namespace, certmgr *cert.CertManager, cfg *config.Config) error {
	mgr.logger = logger
	mgr.certmgr = certmgr
	mgr.cfg = cfg
	mgr.ctx = ctx

	return mgr.CommitNamespaces(nscs, nil)
}

func (n *NamespaceManager) GetNamespace(nm string) (*Namespace, bool) {
	n.RLock()
	defer n.RUnlock()

	ns, ok := n.nsm[nm]
	return ns, ok
}

func (n *NamespaceManager) GetNamespaceByUser(user string) (*Namespace, bool) {
	n.RLock()
	defer n.RUnlock()

	for _, ns := range n.nsm {
		if ns.User() == user {
			return ns, true
		}
	}
	return nil, false
}

func (n *NamespaceManager) RedirectConnections() []error {
	n.RLock()
	defer n.RUnlock()

	var errs []error
	for _, ns := range n.nsm {
		err1 := ns.GetRouter().RedirectConnections()
		if err1 != nil {
			errs = append(errs, err1)
		}
	}
	return errs
}

func (n *NamespaceManager) Close() error {
	n.RLock()
	for _, ns := range n.nsm {
		ns.Close()
	}
	n.RUnlock()
	return nil
}
