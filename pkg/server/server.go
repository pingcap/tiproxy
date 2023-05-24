// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"strings"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	"github.com/pingcap/TiProxy/pkg/manager/logger"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	"github.com/pingcap/TiProxy/pkg/sctx"
	"github.com/pingcap/TiProxy/pkg/server/api"
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
	// HTTP server
	HTTPServer *api.HTTPServer
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
	srv.LoggerManager.Init(srv.ConfigManager.WatchConfig())

	// setup config manager
	if err = srv.ConfigManager.Init(ctx, lg.Named("config"), sctx.ConfigFile, &sctx.Overlay); err != nil {
		err = errors.WithStack(err)
		return
	}
	cfg := srv.ConfigManager.GetConfig()

	// setup metrics
	srv.MetricsManager.Init(ctx, lg.Named("metrics"), cfg.Metrics.MetricsAddr, cfg.Metrics.MetricsInterval, cfg.Proxy.Addr)
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// setup certs
	if err = srv.CertManager.Init(cfg, lg.Named("cert"), srv.ConfigManager.WatchConfig()); err != nil {
		return
	}

	// setup namespace manager
	{
		nscs, nerr := srv.ConfigManager.ListAllNamespace(ctx)
		if nerr != nil {
			err = errors.WithStack(nerr)
			return
		}

		if len(nscs) == 0 {
			// no existed namespace
			pdAddrs := []string{"localhost:2379"}
			if cfg.Proxy.PDAddrs != "" {
				pdAddrs = strings.Split(cfg.Proxy.PDAddrs, ",")
			}
			nsc := config.Namespace{
				Name: "default",
				Backend: config.BackendNamespace{
					Instances:    pdAddrs,
					SelectorType: "pd",
				},
			}
			if err = srv.ConfigManager.SetNamespace(ctx, nsc); err != nil {
				return
			}
			nscs = append(nscs, nsc)
		}

		err = srv.NamespaceManager.Init(ctx, lg.Named("nsmgr"), nscs, srv.CertManager, cfg)
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
			hsHandler = backend.NewDefaultHandshakeHandler(srv.NamespaceManager, cfg.Proxy.ServerVersion)
		}
		srv.Proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg.Proxy, srv.CertManager, hsHandler)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		srv.Proxy.Run(ctx, srv.ConfigManager.WatchConfig())
	}

	// setup http
	if srv.HTTPServer, err = api.NewHTTPServer(cfg.API, lg.Named("api"), srv.Proxy, srv.NamespaceManager, srv.ConfigManager, srv.CertManager, handler, ready); err != nil {
		return
	}

	ready.Toggle()
	return
}

func (s *Server) Close() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()

	errs := make([]error, 0, 4)
	if s.Proxy != nil {
		errs = append(errs, s.Proxy.Close())
	}
	if s.HTTPServer != nil {
		errs = append(errs, s.HTTPServer.Close())
	}
	if s.NamespaceManager != nil {
		errs = append(errs, s.NamespaceManager.Close())
	}
	if s.ConfigManager != nil {
		errs = append(errs, s.ConfigManager.Close())
	}
	if s.MetricsManager != nil {
		s.MetricsManager.Close()
	}
	if s.LoggerManager != nil {
		errs = append(errs, s.LoggerManager.Close())
	}
	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
