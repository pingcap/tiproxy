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
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy"
	"github.com/pingcap/TiProxy/pkg/server/api"
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
	MetricsManager   *metrics.MetricsManager

	// HTTP/GRPC services
	Etcd *embed.Etcd

	// L7 proxy
	Proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, cfg *config.Config, logger *zap.Logger, pubAddr string) (srv *Server, err error) {
	srv = &Server{
		ConfigManager:    mgrcfg.NewConfigManager(),
		NamespaceManager: mgrns.NewNamespaceManager(),
		MetricsManager:   metrics.NewMetricsManager(),
	}

	ready := atomic.NewBool(false)

	// setup metrics
	srv.MetricsManager.Init(ctx, logger.Named("metrics"), cfg.Metrics.MetricsAddr, cfg.Metrics.MetricsInterval, cfg.Proxy.Addr)

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
					c.JSON(http.StatusInternalServerError, "service not ready")
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

		srv.Etcd, err = buildEtcd(ctx, cfg, logger, pubAddr, engine)
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
		err = srv.ConfigManager.Init(ctx, addrs, cfg.Advance, logger.Named("config"))
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		nscs, nerr := srv.ConfigManager.ListAllNamespace(ctx)
		if nerr != nil {
			err = errors.WithStack(nerr)
			return
		}
		if len(nscs) == 0 {
			// no existed namespace
			nsc := &config.Namespace{
				Namespace: "default",
				Backend: config.BackendNamespace{
					Instances:    []string{},
					SelectorType: "random",
				},
			}
			if err = srv.ConfigManager.SetNamespace(ctx, nsc.Namespace, nsc); err != nil {
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

func (s *Server) Run(ctx context.Context) {
	s.Proxy.Run(ctx, s.ConfigManager.GetProxyConfig())
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
	if s.MetricsManager != nil {
		s.MetricsManager.Close()
	}
	return errors.Collect(ErrCloseServer, errs...)
}

func buildEtcd(ctx context.Context, cfg *config.Config, logger *zap.Logger, pubAddr string, engine *gin.Engine) (srv *embed.Etcd, err error) {
	etcd_cfg := embed.NewConfig()

	apiAddrStr := cfg.API.Addr
	if !strings.HasPrefix(apiAddrStr, "http://") {
		apiAddrStr = fmt.Sprintf("http://%s", apiAddrStr)
	}
	apiAddr, uerr := url.Parse(apiAddrStr)
	if uerr != nil {
		err = errors.WithStack(uerr)
		return
	}
	etcd_cfg.LCUrls = []url.URL{*apiAddr}
	apiAddrAdvertise := *apiAddr
	apiAddrAdvertise.Host = fmt.Sprintf("%s:%s", pubAddr, apiAddrAdvertise.Port())
	etcd_cfg.ACUrls = []url.URL{apiAddrAdvertise}

	peerPort := cfg.Advance.PeerPort
	if peerPort == "" {
		peerPortNum, uerr := strconv.Atoi(apiAddr.Port())
		if uerr != nil {
			err = errors.WithStack(uerr)
			return
		}
		peerPort = strconv.Itoa(peerPortNum + 1)
	}
	peerAddr := *apiAddr
	peerAddr.Host = fmt.Sprintf("%s:%s", peerAddr.Hostname(), peerPort)
	etcd_cfg.LPUrls = []url.URL{peerAddr}
	peerAddrAdvertise := *apiAddr
	peerAddrAdvertise.Host = fmt.Sprintf("%s:%s", pubAddr, peerPort)
	etcd_cfg.APUrls = []url.URL{peerAddrAdvertise}

	etcd_cfg.Name = "proxy-" + fmt.Sprint(time.Now().UnixMicro())
	etcd_cfg.InitialCluster = etcd_cfg.InitialClusterFromName(etcd_cfg.Name)
	etcd_cfg.Dir = filepath.Join(cfg.Workdir, "etcd")
	etcd_cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger.Named("etcd"))
	etcd_cfg.UserHandlers = map[string]http.Handler{
		"/api/": engine,
	}
	return embed.StartEtcd(etcd_cfg)
}
