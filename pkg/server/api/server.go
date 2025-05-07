// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	mgrns "github.com/pingcap/tiproxy/pkg/manager/namespace"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	mgrrp "github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"go.uber.org/atomic"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// DefAPILimit is the global API limit per second.
	DefAPILimit = 100
	// DefConnTimeout is used as timeout duration in the HTTP server.
	DefConnTimeout = 30 * time.Second
)

type HTTPHandler interface {
	RegisterHTTP(c *gin.Engine) error
}

type Managers struct {
	CfgMgr        *mgrcfg.ConfigManager
	NsMgr         mgrns.NamespaceManager
	CertMgr       *mgrcrt.CertManager
	BackendReader BackendReader
	ReplayJobMgr  mgrrp.JobManager
}

type Server struct {
	listener  net.Listener
	wg        waitgroup.WaitGroup
	limit     ratelimit.Limiter
	ready     *atomic.Bool
	lg        *zap.Logger
	grpc      *grpc.Server
	isClosing atomic.Bool
	mgr       Managers
}

func NewServer(cfg config.API, lg *zap.Logger, mgr Managers, handler HTTPHandler, ready *atomic.Bool) (*Server, error) {
	grpcOpts := []grpc_zap.Option{
		grpc_zap.WithLevels(func(code codes.Code) zapcore.Level {
			return zap.InfoLevel
		}),
	}
	h := &Server{
		limit: ratelimit.New(DefAPILimit),
		ready: ready,
		lg:    lg,
		grpc: grpc.NewServer(
			grpc_middleware.WithUnaryServerChain(
				grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
				grpc_zap.UnaryServerInterceptor(lg.Named("grpcu"), grpcOpts...),
			),
			grpc_middleware.WithStreamServerChain(
				grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
				grpc_zap.StreamServerInterceptor(lg.Named("grpcs"), grpcOpts...),
			),
		),
		mgr: mgr,
	}

	var err error
	h.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}
	switch cfg.ProxyProtocol {
	case "v2":
		h.listener = proxyprotocol.NewListener(h.listener)
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.UseH2C = true
	engine.Use(
		gin.Recovery(),
		h.rateLimit,
		h.readyState,
		h.grpcServer,
		h.attachLogger,
	)

	h.registerGrpc(mgr.CfgMgr)
	h.registerAPI(engine.Group("/api"))
	// The paths are consistent with other components.
	h.registerMetrics(engine.Group("metrics"))
	h.registerDebug(engine.Group("debug"))

	if handler != nil {
		if err := handler.RegisterHTTP(engine); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if tlscfg := mgr.CertMgr.ServerHTTPTLS(); tlscfg != nil {
		h.listener = tls.NewListener(h.listener, tlscfg)
	}

	hsrv := http.Server{
		Handler:           engine.Handler(),
		ReadHeaderTimeout: DefConnTimeout,
		IdleTimeout:       DefConnTimeout,
	}

	h.wg.RunWithRecover(func() {
		lg.Info("HTTP closed", zap.Error(hsrv.Serve(h.listener)))
	}, nil, h.lg)

	return h, nil
}

func (h *Server) rateLimit(c *gin.Context) {
	_ = h.limit.Take()
}

func (h *Server) attachLogger(c *gin.Context) {
	start := time.Now()
	c.Next()
	latency := time.Since(start)

	fields := make([]zapcore.Field, 0, 7)
	fields = append(fields,
		zap.Int("status", c.Writer.Status()),
		zap.String("method", c.Request.Method),
		zap.String("query", c.Request.URL.RawQuery),
		zap.String("ip", c.ClientIP()),
		zap.String("user-agent", c.Request.UserAgent()),
		zap.Duration("latency", latency),
	)

	path := c.Request.URL.Path
	switch {
	case len(c.Errors) > 0:
		errs := make([]error, 0, len(c.Errors))
		for _, e := range c.Errors {
			errs = append(errs, e)
		}
		fields = append(fields, zap.Errors("errs", errs))
		h.lg.Warn(path, fields...)
	default:
		h.lg.Debug(path, fields...)
	}
}

func (h *Server) readyState(c *gin.Context) {
	if !h.ready.Load() {
		c.Abort()
		c.JSON(http.StatusInternalServerError, "service not ready")
	}
}

func (h *Server) registerGrpc(cfgmgr *mgrcfg.ConfigManager) {
	diagnosticspb.RegisterDiagnosticsServer(h.grpc, sysutil.NewDiagnosticsServer(cfgmgr.GetConfig().Log.LogFile.Filename))
}

func (h *Server) grpcServer(ctx *gin.Context) {
	if ctx.Request.ProtoMajor == 2 && strings.HasPrefix(ctx.GetHeader("Content-Type"), "application/grpc") {
		ctx.Status(http.StatusOK)
		h.grpc.ServeHTTP(ctx.Writer, ctx.Request)
		ctx.Abort()
	} else {
		ctx.Next()
	}
}

func (h *Server) registerAPI(g *gin.RouterGroup) {
	{
		adminGroup := g.Group("admin")
		h.registerNamespace(adminGroup.Group("namespace"))
		h.registerConfig(adminGroup.Group("config"))
	}

	h.registerMetrics(g.Group("metrics"))
	h.registerDebug(g.Group("debug"))
	h.registerBackend(g.Group("backend"))
	h.registerTraffic(g.Group("traffic"))
}

func (h *Server) PreClose() {
	h.isClosing.Store(true)
}

func (h *Server) Close() error {
	err := h.listener.Close()
	h.wg.Wait()
	h.grpc.Stop()
	return err
}
