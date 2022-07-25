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
	"sync/atomic"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy/driver"
	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type NamespaceHolder struct {
	nss map[string]Namespace
}

type NamespaceWrapper struct {
	nsmgr       *NamespaceManager
	name        string
	connCounter int64
}

func CreateNamespaceHolder(cfgs []*config.Namespace, client *clientv3.Client) (*NamespaceHolder, error) {
	nss := make(map[string]Namespace, len(cfgs))

	for _, cfg := range cfgs {
		ns, err := BuildNamespace(cfg, client)
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf("create namespace error, namespace: %s", cfg.Namespace))
		}
		nss[cfg.Namespace] = ns
	}

	holder := &NamespaceHolder{
		nss: nss,
	}
	return holder, nil
}

func (n *NamespaceHolder) Get(name string) (Namespace, bool) {
	ns, ok := n.nss[name]
	return ns, ok
}

func (n *NamespaceHolder) Set(name string, ns Namespace) {
	n.nss[name] = ns
}

func (n *NamespaceHolder) Delete(name string) {
	delete(n.nss, name)
}

func (n *NamespaceHolder) Clone() *NamespaceHolder {
	nss := make(map[string]Namespace)
	for name, ns := range n.nss {
		nss[name] = ns
	}
	return &NamespaceHolder{
		nss: nss,
	}
}

func (n *NamespaceHolder) RedirectConnections() error {
	var err error
	for _, ns := range n.nss {
		err1 := ns.GetRouter().RedirectConnections()
		if err == nil && err1 != nil {
			err = err1
		}
	}
	return err
}

func (n *NamespaceWrapper) Name() string {
	return n.name
}

func (n *NamespaceWrapper) IncrConnCount() {
	currCnt := atomic.AddInt64(&n.connCounter, 1)
	metrics.QueryCtxGauge.WithLabelValues(n.name).Set(float64(currCnt))
}

func (n *NamespaceWrapper) DescConnCount() {
	currCnt := atomic.AddInt64(&n.connCounter, -1)
	metrics.QueryCtxGauge.WithLabelValues(n.name).Set(float64(currCnt))
}

func (n *NamespaceWrapper) Closed() bool {
	_, ok := n.nsmgr.getCurrentNamespaces().Get(n.name)
	return !ok
}

func (n *NamespaceWrapper) GetRouter() driver.Router {
	return n.mustGetCurrentNamespace().GetRouter()
}

func (n *NamespaceWrapper) mustGetCurrentNamespace() Namespace {
	ns, ok := n.nsmgr.getCurrentNamespaces().Get(n.name)
	if !ok {
		panic(errors.New("namespace not found"))
	}
	return ns
}
