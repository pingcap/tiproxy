// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"reflect"
	"runtime"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/manager/logger"
	mgrns "github.com/pingcap/tiproxy/pkg/manager/namespace"
	"github.com/pingcap/tiproxy/pkg/manager/vip"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/server/api"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	wg waitgroup.WaitGroup
	// managers
	configManager    *mgrcfg.ConfigManager
	namespaceManager *mgrns.NamespaceManager
	metricsManager   *metrics.MetricsManager
	loggerManager    *logger.LoggerManager
	certManager      *cert.CertManager
	vipManager       vip.VIPManager
	infoSyncer       *infosync.InfoSyncer
	metricsReader    metricsreader.MetricsReader
	// etcd client
	etcdCli *clientv3.Client
	// HTTP client
	httpCli *http.Client
	// HTTP server
	apiServer *api.Server
	// L7 proxy
	proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		configManager:    mgrcfg.NewConfigManager(),
		metricsManager:   metrics.NewMetricsManager(),
		namespaceManager: mgrns.NewNamespaceManager(),
		certManager:      cert.NewCertManager(),
		wg:               waitgroup.WaitGroup{},
	}

	handler := sctx.Handler
	ready := atomic.NewBool(false)

	// set up logger
	var lg *zap.Logger
	if srv.loggerManager, lg, err = logger.NewLoggerManager(&sctx.Overlay.Log); err != nil {
		return
	}
	srv.loggerManager.Init(srv.configManager.WatchConfig())

	// setup config manager
	if err = srv.configManager.Init(ctx, lg.Named("config"), sctx.ConfigFile, &sctx.Overlay); err != nil {
		return
	}
	cfg := srv.configManager.GetConfig()

	// welcome messages must be printed after initialization of configmager, because
	// logfile backended zaplogger is enabled after cfgmgr.Init(..).
	// otherwise, printInfo will output to stdout, which can not be redirected to the log file on tiup-cluster.
	//
	// TODO: there is a race condition that printInfo and logmgr may concurrently execute:
	// logmgr may havenot been initialized with logfile yet
	// Make sure the TiProxy info is always printed.
	level := lg.Level()
	srv.loggerManager.SetLoggerLevel(zap.InfoLevel)
	printInfo(lg)
	srv.loggerManager.SetLoggerLevel(level)

	// setup metrics
	srv.metricsManager.Init(ctx, lg.Named("metrics"))
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// setup certs
	if err = srv.certManager.Init(cfg, lg.Named("cert"), srv.configManager.WatchConfig()); err != nil {
		return
	}

	// setup etcd client
	srv.etcdCli, err = etcd.InitEtcdClient(lg.Named("etcd"), cfg, srv.certManager)
	if err != nil {
		return
	}

	// general cluster HTTP client
	{
		srv.httpCli = http.NewHTTPClient(srv.certManager.ClusterTLS)
	}

	// setup info syncer
	if cfg.Proxy.PDAddrs != "" {
		srv.infoSyncer = infosync.NewInfoSyncer(lg.Named("infosync"), srv.etcdCli)
		if err = srv.infoSyncer.Init(ctx, cfg); err != nil {
			return
		}
	}

	// setup metrics reader
	{
		healthCheckCfg := config.NewDefaultHealthCheckConfig()
		srv.metricsReader = metricsreader.NewDefaultMetricsReader(lg.Named("mr"), srv.infoSyncer, srv.infoSyncer, srv.httpCli, srv.etcdCli, healthCheckCfg, srv.configManager)
		if err = srv.metricsReader.Start(context.Background()); err != nil {
			return
		}
	}

	// setup namespace manager
	{
		nscs, nerr := srv.configManager.ListAllNamespace(ctx)
		if nerr != nil {
			err = nerr
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
			if err = srv.configManager.SetNamespace(ctx, nsc.Namespace, nsc); err != nil {
				return
			}
			nscs = append(nscs, nsc)
		}

		err = srv.namespaceManager.Init(lg.Named("nsmgr"), nscs, srv.infoSyncer, srv.infoSyncer, srv.httpCli, srv.configManager, srv.metricsReader)
		if err != nil {
			return
		}
	}

	// setup proxy server
	{
		var hsHandler backend.HandshakeHandler
		if handler != nil {
			hsHandler = handler
		} else {
			hsHandler = backend.NewDefaultHandshakeHandler(srv.namespaceManager)
		}
		srv.proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg, srv.certManager, hsHandler)
		if err != nil {
			return
		}

		srv.proxy.Run(ctx, srv.configManager.WatchConfig())
	}

	// setup http & grpc
	if srv.apiServer, err = api.NewServer(cfg.API, lg.Named("api"), srv.proxy.IsClosing, srv.namespaceManager, srv.configManager, srv.certManager, srv.metricsReader, handler, ready); err != nil {
		return
	}

	// setup vip manager
	{
		srv.vipManager, err = vip.NewVIPManager(lg.Named("vipmgr"), srv.configManager)
		if err != nil {
			return
		}
		if srv.vipManager != nil && !reflect.ValueOf(srv.vipManager).IsNil() {
			if err = srv.vipManager.Start(ctx, srv.etcdCli); err != nil {
				return
			}
		}
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
	// Resign the VIP owner before graceful wait so that clients connect to other nodes.
	if s.vipManager != nil && !reflect.ValueOf(s.vipManager).IsNil() {
		s.vipManager.Resign()
	}
	if s.proxy != nil {
		errs = append(errs, s.proxy.Close())
	}
	// Delete VIP after graceful wait.
	if s.vipManager != nil && !reflect.ValueOf(s.vipManager).IsNil() {
		s.vipManager.Close()
	}
	if s.apiServer != nil {
		errs = append(errs, s.apiServer.Close())
	}
	if s.namespaceManager != nil {
		errs = append(errs, s.namespaceManager.Close())
	}
	if s.metricsReader != nil {
		s.metricsReader.Close()
	}
	if s.infoSyncer != nil {
		errs = append(errs, s.infoSyncer.Close())
	}
	if s.configManager != nil {
		errs = append(errs, s.configManager.Close())
	}
	if s.metricsManager != nil {
		s.metricsManager.Close()
	}
	if s.loggerManager != nil {
		errs = append(errs, s.loggerManager.Close())
	}
	if s.etcdCli != nil {
		errs = append(errs, s.etcdCli.Close())
	}
	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
