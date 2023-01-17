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
	"time"

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
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

const (
	// DefAPILimit is the global API limit per second.
	DefAPILimit = 100
	// DefConnTimeout is used as timeout duration in the HTTP server.
	DefConnTimeout = 30 * time.Second
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
	Proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		ConfigManager:    mgrcfg.NewConfigManager(),
		MetricsManager:   metrics.NewMetricsManager(),
		NamespaceManager: mgrns.NewNamespaceManager(),
		CertManager:      cert.NewCertManager(),
		wg:               waitgroup.WaitGroup{},
	}

	handler := sctx.Handler
	ready := atomic.NewBool(false)

	// set up logger
	var lg *zap.Logger
	if srv.LoggerManager, lg, err = logger.NewLoggerManager(&sctx.Overlay.Log); err != nil {
		return
	}

	// setup config manager
	if err = srv.ConfigManager.Init(ctx, lg.Named("config"), sctx.ConfigFile, &sctx.Overlay); err != nil {
		err = errors.WithStack(err)
		return
	}
	cfg := srv.ConfigManager.GetConfig()

	// also hook logger
	srv.LoggerManager.Init(srv.ConfigManager.WatchConfig())

	// setup metrics
	srv.MetricsManager.Init(ctx, lg.Named("metrics"), cfg.Metrics.MetricsAddr, cfg.Metrics.MetricsInterval, cfg.Proxy.Addr)

	// setup certs
	if err = srv.CertManager.Init(cfg, lg.Named("cert")); err != nil {
		return
	}

	// setup gin
	{
		slogger := lg.Named("gin")
		gin.SetMode(gin.ReleaseMode)
		engine := gin.New()
		limit := ratelimit.New(DefAPILimit)
		engine.Use(
			gin.Recovery(),
			ginzap.Ginzap(slogger, "", true),
			func(c *gin.Context) {
				_ = limit.Take()
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
			hsrv := http.Server{
				Handler:           engine.Handler(),
				ReadHeaderTimeout: DefConnTimeout,
				IdleTimeout:       DefConnTimeout,
			}
			slogger.Info("HTTP closed", zap.Error(hsrv.Serve(srv.HTTPListener)))
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

	// setup namespace manager
	{
		srv.ObserverClient, err = router.InitEtcdClient(lg.Named("pd"), cfg, srv.CertManager)
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
			nscs = append(nscs, nsc)
		}

		err = srv.NamespaceManager.Init(lg.Named("nsmgr"), nscs, srv.ObserverClient, srv.Http)
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

		srv.Proxy.Run(ctx, srv.ConfigManager.WatchConfig())
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
