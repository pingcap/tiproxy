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
	"crypto/tls"
	"net"
	"net/http"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	"github.com/pingcap/TiProxy/pkg/manager/logger"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	"github.com/pingcap/TiProxy/pkg/sctx"
	"github.com/pingcap/TiProxy/pkg/server/api"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	wg waitgroup.WaitGroup
	// managers
	ConfigManager    *mgrcfg.ConfigManager
	NamespaceManager *mgrns.NamespaceManager
	MetricsManager   *metrics.MetricsManager
	LoggerManager    *logger.LoggerManager
	CertManager      *cert.CertManager
	ObserverClient   *clientv3.Client
	// HTTP client
	Http *http.Client
	// HTTP server
	HTTPListener net.Listener
	// L7 proxy
	Proxy   *proxy.SQLServer
	proxyCh <-chan *config.ProxyServerOnline
}

func NewServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		ConfigManager:    mgrcfg.NewConfigManager(),
		MetricsManager:   metrics.NewMetricsManager(),
		NamespaceManager: mgrns.NewNamespaceManager(),
		CertManager:      cert.NewCertManager(),
		wg:               waitgroup.WaitGroup{},
	}

	cfg := sctx.Config
	handler := sctx.Handler

	// set up logger
	var lg *zap.Logger
	if srv.LoggerManager, lg, err = logger.NewLoggerManager(&cfg.Log); err != nil {
		return
	}

	// setup certs
	{
		clogger := lg.Named("cert")
		if err = srv.CertManager.Init(cfg, clogger); err != nil {
			return
		}
	}

	ready := atomic.NewBool(false)

	// setup metrics
	srv.MetricsManager.Init(ctx, lg.Named("metrics"), cfg.Metrics.MetricsAddr, cfg.Metrics.MetricsInterval, cfg.Proxy.Addr)

	// setup gin and etcd
	{
		slogger := lg.Named("gin")
		gin.SetMode(gin.ReleaseMode)
		engine := gin.New()
		engine.Use(
			gin.Recovery(),
			ginzap.Ginzap(slogger, "", true),
			func(c *gin.Context) {
				if !ready.Load() {
					c.Abort()
					c.JSON(http.StatusInternalServerError, "service not ready")
				}
			},
		)

		api.Register(engine.Group("/api"), cfg.API, lg.Named("api"), srv.NamespaceManager, srv.ConfigManager)

		srv.HTTPListener, err = net.Listen("tcp", cfg.API.Addr)
		if err != nil {
			return nil, err
		}
		if tlscfg := srv.CertManager.ServerTLS(); tlscfg != nil {
			srv.HTTPListener = tls.NewListener(srv.HTTPListener, tlscfg)
		}

		if handler != nil {
			if err := handler.RegisterHTTP(engine); err != nil {
				return nil, errors.WithStack(err)
			}
		}

		srv.wg.Run(func() {
			slogger.Info("HTTP closed", zap.Error(engine.RunListener(srv.HTTPListener)))
		})
	}

	// general cluster HTTP client
	{
		srv.Http = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: srv.CertManager.ClusterTLS(),
			},
		}
	}

	// setup config manager
	{
		err = srv.ConfigManager.Init(ctx, cfg, lg.Named("config"))
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		srv.LoggerManager.Init(mgrcfg.MakeConfigChan(srv.ConfigManager, &cfg.Log.LogOnline))

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
		srv.ObserverClient, err = router.InitEtcdClient(lg.Named("pd"), cfg, srv.CertManager)
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

		err = srv.NamespaceManager.Init(lg.Named("nsmgr"), nss, srv.ObserverClient, srv.Http)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}

	// setup proxy server
	{
		var hsHandler backend.HandshakeHandler
		if handler != nil {
			hsHandler = handler
		} else {
			hsHandler = backend.NewDefaultHandshakeHandler(srv.NamespaceManager)
		}
		srv.Proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg.Proxy, srv.CertManager, hsHandler)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		srv.proxyCh = mgrcfg.MakeConfigChan(srv.ConfigManager, &cfg.Proxy.ProxyServerOnline)

		srv.wg.Run(func() {
			srv.Proxy.Run(ctx, srv.proxyCh)
		})
	}

	ready.Toggle()
	return
}

func (s *Server) Close() error {
	errs := make([]error, 0, 4)
	if s.Proxy != nil {
		errs = append(errs, s.Proxy.Close())
	}
	if s.HTTPListener != nil {
		s.HTTPListener.Close()
	}
	if s.NamespaceManager != nil {
		errs = append(errs, s.NamespaceManager.Close())
	}
	if s.ConfigManager != nil {
		errs = append(errs, s.ConfigManager.Close())
	}
	if s.ObserverClient != nil {
		errs = append(errs, s.ObserverClient.Close())
	}
	if s.MetricsManager != nil {
		s.MetricsManager.Close()
	}
	if s.LoggerManager != nil {
		if err := s.LoggerManager.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
