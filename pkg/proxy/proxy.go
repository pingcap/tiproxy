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

package proxy

import (
	"time"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/configcenter"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/djshow832/weir/pkg/proxy/metrics"
	"github.com/djshow832/weir/pkg/proxy/namespace"
	"github.com/djshow832/weir/pkg/proxy/router"
	"github.com/djshow832/weir/pkg/proxy/server"
	"github.com/djshow832/weir/pkg/proxy/sessionmgr/backend"
	"github.com/djshow832/weir/pkg/proxy/sessionmgr/client"
)

type Proxy struct {
	cfg          *config.Proxy
	svr          *server.Server
	apiServer    *HttpApiServer
	nsmgr        *namespace.NamespaceManager
	configCenter configcenter.ConfigCenter
}

func NewProxy(cfg *config.Proxy) *Proxy {
	return &Proxy{
		cfg: cfg,
	}
}

func (p *Proxy) Init() error {
	metrics.RegisterProxyMetrics(p.cfg.Cluster)
	cc, err := configcenter.CreateConfigCenter(p.cfg.ConfigCenter)
	if err != nil {
		return err
	}
	p.configCenter = cc

	if err = router.InitEtcdClient(p.cfg); err != nil {
		return err
	}
	nss, err := cc.ListAllNamespace()
	if err != nil {
		return err
	}
	nsmgr, err := namespace.CreateNamespaceManager(nss)
	if err != nil {
		return err
	}
	p.nsmgr = nsmgr
	driverImpl := driver.NewDriverImpl(nsmgr, client.NewClientConnectionImpl, backend.NewBackendConnManager)
	svr, err := server.NewServer(p.cfg, driverImpl)
	if err != nil {
		return err
	}
	p.svr = svr
	apiServer, err := CreateHttpApiServer(svr, nsmgr, cc, p.cfg)
	if err != nil {
		return err
	}
	p.apiServer = apiServer

	return nil
}

// TODO(eastfisher): refactor this function
func (p *Proxy) Run() error {
	go func() {
		time.Sleep(200 * time.Millisecond)
		p.apiServer.Run()
	}()
	return p.svr.Run()
}

func (p *Proxy) Close() {
	if p.apiServer != nil {
		p.apiServer.Close()
	}
	if p.svr != nil {
		p.svr.Close()
	}
}
