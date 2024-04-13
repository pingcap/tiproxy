// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"net/http"
	"runtime"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/manager/logger"
	mgrns "github.com/pingcap/tiproxy/pkg/manager/namespace"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/server/api"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
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
	InfoSyncer       *infosync.InfoSyncer
	// HTTP client
	Http *http.Client
	// HTTP server
	APIServer *api.Server
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

	// welcome messages must be printed after initialization of configmager, because
	// logfile backended zaplogger is enabled after cfgmgr.Init(..).
	// otherwise, printInfo will output to stdout, which can not be redirected to the log file on tiup-cluster.
	//
	// TODO: there is a race condition that printInfo and logmgr may concurrently execute:
	// logmgr may havenot been initialized with logfile yet
	// Make sure the TiProxy info is always printed.
	level := lg.Level()
	srv.LoggerManager.SetLoggerLevel(zap.InfoLevel)
	printInfo(lg)
	srv.LoggerManager.SetLoggerLevel(level)

	// setup metrics
	srv.MetricsManager.Init(ctx, lg.Named("metrics"))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// setup certs
	if err = srv.CertManager.Init(cfg, lg.Named("cert"), srv.ConfigManager.WatchConfig()); err != nil {
		return
	}

	// general cluster HTTP client
	{
		srv.Http = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: srv.CertManager.ClusterTLS(),
			},
		}
	}

	// setup info syncer
	if cfg.Proxy.PDAddrs != "" {
		srv.InfoSyncer = infosync.NewInfoSyncer(lg.Named("infosync"))
		if err = srv.InfoSyncer.Init(ctx, cfg, srv.CertManager); err != nil {
			return
		}
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
			nsc := &config.Namespace{
				Namespace: "default",
				Backend: config.BackendNamespace{
					Instances: []string{},
				},
			}
			if err = srv.ConfigManager.SetNamespace(ctx, nsc.Namespace, nsc); err != nil {
				return
			}
			nscs = append(nscs, nsc)
		}

		err = srv.NamespaceManager.Init(lg.Named("nsmgr"), nscs, srv.InfoSyncer, srv.InfoSyncer, srv.Http)
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
		srv.Proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg, srv.CertManager, hsHandler)
		if err != nil {
			err = errors.WithStack(err)
			return
		}

		srv.Proxy.Run(ctx, srv.ConfigManager.WatchConfig())
	}

	// setup http & grpc
	if srv.APIServer, err = api.NewServer(cfg.API, lg.Named("api"), srv.Proxy.IsClosing, srv.NamespaceManager, srv.ConfigManager, srv.CertManager, handler, ready); err != nil {
		return
	}

	ready.Toggle()
	return
}

func printInfo(lg *zap.Logger) {
	fields := []zap.Field{
		zap.String("Release Version", versioninfo.TiProxyVersion),
		zap.String("Git Commit Hash", versioninfo.TiProxyGitHash),
		zap.String("Git Branch", versioninfo.TiProxyGitBranch),
		zap.String("UTC Build Time", versioninfo.TiProxyBuildTS),
		zap.String("GoVersion", runtime.Version()),
		zap.String("OS", runtime.GOOS),
		zap.String("Arch", runtime.GOARCH),
	}
	lg.Info("Welcome to TiProxy.", fields...)
}

func (s *Server) Close() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()

	errs := make([]error, 0, 4)
	if s.Proxy != nil {
		errs = append(errs, s.Proxy.Close())
	}
	if s.APIServer != nil {
		errs = append(errs, s.APIServer.Close())
	}
	if s.NamespaceManager != nil {
		errs = append(errs, s.NamespaceManager.Close())
	}
	if s.InfoSyncer != nil {
		errs = append(errs, s.InfoSyncer.Close())
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
