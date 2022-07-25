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
	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/proxy/driver"
	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type NamespaceImpl struct {
	name   string
	router driver.Router
}

func BuildNamespace(cfg *config.Namespace, client *clientv3.Client) (Namespace, error) {
	rt, err := BuildRouter(&cfg.Backend, client)
	if err != nil {
		return nil, errors.WithMessage(err, "build router error")
	}
	wrapper := &NamespaceImpl{
		name:   cfg.Namespace,
		router: rt,
	}

	return wrapper, nil
}

func (n *NamespaceImpl) Name() string {
	return n.name
}

func (n *NamespaceImpl) GetRouter() driver.Router {
	return n.router
}

func (n *NamespaceImpl) Close() {
	n.router.Close()
}

func BuildRouter(cfg *config.BackendNamespace, client *clientv3.Client) (driver.Router, error) {
	return router.NewRandomRouter(cfg, client)
}

func DefaultAsyncCloseNamespace(ns Namespace) error {
	return nil
}
