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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type NamespaceManager struct {
	sync.RWMutex
	client *clientv3.Client
	logger *zap.Logger
	nsm    map[string]*Namespace
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{}
}
func (mgr *NamespaceManager) buildNamespace(cfg *config.Namespace, client *clientv3.Client) (*Namespace, error) {
	logger := mgr.logger.With(zap.String("namespace", cfg.Namespace))
	rt, err := router.NewScoreBasedRouter(&cfg.Backend, client)
	if err != nil {
		return nil, errors.Errorf("build router error: %w", err)
	}
	r := &Namespace{
		name:   cfg.Namespace,
		router: rt,
	}

	// frontend tls configuration
	{
		r.frontendTLS = &tls.Config{}

		if !cfg.Frontend.Security.HasCert() {
			// TODO: require certs here
			logger.Warn("require certificates to secure frontend tls connections")
		} else {
			cert, err := tls.LoadX509KeyPair(cfg.Frontend.Security.Cert, cfg.Frontend.Security.Key)
			if err != nil {
				return nil, errors.Errorf("failed to load server certs: %w", err)
			}
			r.frontendTLS.Certificates = append(r.frontendTLS.Certificates, cert)
		}

		if cfg.Frontend.Security.HasCA() {
			r.frontendTLS.ClientAuth = tls.RequireAndVerifyClientCert
			r.frontendTLS.ClientCAs = x509.NewCertPool()
			certBytes, err := ioutil.ReadFile(cfg.Frontend.Security.CA)
			if err != nil {
				return nil, errors.Errorf("failed to read server signed certs from disk: %w", err)
			}
			if !r.frontendTLS.ClientCAs.AppendCertsFromPEM(certBytes) {
				return nil, errors.Errorf("failed to load server signed certs")
			}
		} else {
			logger.Warn("no signed certs for frontend, proxy will not authenticate clients (connection is still secured)")
		}
	}

	{
		r.backendTLS = &tls.Config{}
		// backend tls configuration
		if !cfg.Backend.Security.HasCA() {
			// TODO: require certs here
			logger.Error("require signed certs to verify backend tls connections")
		} else {
			r.backendTLS.RootCAs = x509.NewCertPool()
			certBytes, err := ioutil.ReadFile(cfg.Backend.Security.CA)
			if err != nil {
				return nil, errors.Errorf("failed to read server signed certs from disk: %w", err)
			}
			if !r.backendTLS.RootCAs.AppendCertsFromPEM(certBytes) {
				return nil, errors.Errorf("failed to load server signed certs")
			}
		}

		if cfg.Backend.Security.HasCert() {
			cert, err := tls.LoadX509KeyPair(cfg.Backend.Security.Cert, cfg.Backend.Security.Key)
			if err != nil {
				return nil, errors.Errorf("failed to load cluster certs: %w", err)
			}
			r.backendTLS.Certificates = append(r.backendTLS.Certificates, cert)
		} else {
			logger.Warn("no certs for backend authentication, backend may reject proxy connections (connection is still secured)")
		}
	}

	return r, nil
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

		ns, err := mgr.buildNamespace(nsc, mgr.client)
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

func (mgr *NamespaceManager) Init(logger *zap.Logger, nss []*config.Namespace, client *clientv3.Client) error {
	mgr.Lock()
	mgr.client = client
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
	return nil
}
