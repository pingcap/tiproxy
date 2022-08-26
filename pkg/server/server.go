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

package server

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/pkg/config"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy"
	"github.com/pingcap/TiProxy/pkg/server/api"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	// managers
	ConfigManager    *mgrcfg.ConfigManager
	NamespaceManager *mgrns.NamespaceManager
	ObserverClient   *clientv3.Client

	// HTTP/GRPC services
	Etcd *embed.Etcd

	// L7 proxy
	Proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, cfg *config.Config, logger *zap.Logger, namespaceFiles string) (srv *Server, err error) {
	srv = &Server{
		ConfigManager:    mgrcfg.NewConfigManager(),
		NamespaceManager: mgrns.NewNamespaceManager(),
	}

	ready := atomic.NewBool(false)

	// setup metrics
	metrics.RegisterProxyMetrics(cfg.Metrics.PromCluster)

	// setup gin and etcd
	{

		gin.SetMode(gin.ReleaseMode)
		engine := gin.New()
		engine.Use(
			gin.Recovery(),
			ginzap.Ginzap(logger.Named("gin"), "", true),
			func(c *gin.Context) {
				if !ready.Load() {
					c.Abort()
					c.String(http.StatusInternalServerError, "service not ready")
				}
			},
		)

		// This is the tricky part. While HTTP services rely on managers, managers also rely on the etcd server.
		// Etcd server is used to bring up the config manager and HTTP services itself.
		// That means we have cyclic dependencies. Here's the solution:
		// 1. create managers first, and pass them down
		// 2. start etcd and HTTP, but HTTP will wait for managers to init
		// 3. init managers using bootstrapped etcd
		//
		// We have some alternative solution, for example:
		// 1. globally lazily creation of managers. It introduced racing/chaos-management/hard-code-reading as in TiDB.
		// 2. pass down '*Server' struct such that the underlying relies on the pointer only. But it does not work well for golang. To avoid cyclic imports between 'api' and `server` packages, two packages needs to be merged. That is basically what happened to TiDB '*Session'.
		api.Register(engine.Group("/api"), ready, cfg.API, logger.Named("api"), srv.NamespaceManager, srv.ConfigManager)

		etcd_cfg := embed.NewConfig()
		etcd_cfg.LCUrls = cfg.LCUrls
		etcd_cfg.ACUrls = cfg.ACUrls
		etcd_cfg.LPUrls = cfg.LPUrls
		etcd_cfg.APUrls = cfg.APUrls
		etcd_cfg.Name = "proxy-" + fmt.Sprint(time.Now().UnixMicro())
		etcd_cfg.InitialCluster = etcd_cfg.InitialClusterFromName(etcd_cfg.Name)
		etcd_cfg.Dir = filepath.Join(cfg.Workdir, "etcd")
		etcd_cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger.Named("etcd"))
		etcd_cfg.UserHandlers = map[string]http.Handler{
			"/api/": engine,
		}
		srv.Etcd, err = embed.StartEtcd(etcd_cfg)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		// wait for etcd server
		select {
		case <-ctx.Done():
			err = fmt.Errorf("timeout on creating etcd")
			return
		case <-srv.Etcd.Server.ReadyNotify():
		}
	}

	// setup config manager
	{
		addrs := make([]string, len(srv.Etcd.Clients))
		for i := range addrs {
			addrs[i] = srv.Etcd.Clients[i].Addr().String()
		}
		err = srv.ConfigManager.Init(ctx, addrs, cfg.Config, logger.Named("config"))
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		if namespaceFiles != "" {
			err = srv.ConfigManager.ImportNamespaceFromDir(ctx, namespaceFiles)
			if err != nil {
				return
			}
		}
	}

	// setup namespace manager
	{
		srv.ObserverClient, err = router.InitEtcdClient(cfg)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		var nss []*config.Namespace
		nss, err = srv.ConfigManager.ListAllNamespace(ctx)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		err = srv.NamespaceManager.Init(logger.Named("nsmgr"), nss, srv.ObserverClient)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}

	// setup proxy server
	{
		srv.Proxy, err = proxy.NewSQLServer(logger.Named("proxy"), cfg.Workdir, cfg.Proxy, cfg.Security, srv.NamespaceManager)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}

	ready.Toggle()
	return
}

func (s *Server) Run(ctx context.Context) error {
	return s.Proxy.Run(ctx, s.ConfigManager.GetProxyConfig())
}

func (s *Server) Close() error {
	var errs []error
	if s.Proxy != nil {
		if err := s.Proxy.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.NamespaceManager != nil {
		if err := s.NamespaceManager.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.ConfigManager != nil {
		if err := s.ConfigManager.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.ObserverClient != nil {
		if err := s.ObserverClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.Etcd != nil {
		var wg waitgroup.WaitGroup
		wg.Run(func() {
			for {
				err, ok := <-s.Etcd.Err()
				if !ok {
					break
				}
				errs = append(errs, err)
			}
		})
		s.Etcd.Close()
		wg.Wait()
	}
	return errors.Collect(ErrCloseServer, errs...)
}
