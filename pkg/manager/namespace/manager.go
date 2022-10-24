// Copyright 2020 Ipalfish, Inc.
// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package namespace

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	sync.RWMutex
	client  *clientv3.Client
	httpCli *http.Client
	logger  *zap.Logger
	nsm     map[string]*Namespace
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{}
}

func (mgr *NamespaceManager) buildNamespace(cfg *config.Namespace) (*Namespace, error) {
	logger := mgr.logger.With(zap.String("namespace", cfg.Namespace))

	rt, err := router.NewScoreBasedRouter(logger.Named("router"), &cfg.Backend, mgr.client, mgr.httpCli)
	if err != nil {
		return nil, errors.Errorf("build router error: %w", err)
	}
	return &Namespace{
		name:   cfg.Namespace,
		user:   cfg.Frontend.User,
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

func (mgr *NamespaceManager) Init(logger *zap.Logger, nss []*config.Namespace, client *clientv3.Client, httpCli *http.Client) error {
	mgr.Lock()
	mgr.client = client
	mgr.httpCli = httpCli
	mgr.logger = logger
	mgr.Unlock()

	return mgr.CommitNamespaces(nss, nil)
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
