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

type managers struct {
	cfg *mgrcfg.ConfigManager
	ns  *mgrns.NamespaceManager
	crt *mgrcrt.CertManager
}

type Server struct {
	listener  net.Listener
	wg        waitgroup.WaitGroup
	limit     ratelimit.Limiter
	ready     *atomic.Bool
	lg        *zap.Logger
	grpc      *grpc.Server
	isClosing func() bool
	mgr       managers
}

func NewServer(cfg config.API, lg *zap.Logger,
	isClosing func() bool,
	nsmgr *mgrns.NamespaceManager, cfgmgr *mgrcfg.ConfigManager,
	crtmgr *mgrcrt.CertManager, handler HTTPHandler,
	ready *atomic.Bool) (*Server, error) {
	grpcOpts := []grpc_zap.Option{
		grpc_zap.WithLevels(func(code codes.Code) zapcore.Level {
			return zap.InfoLevel
		}),
	}
	h := &Server{
		limit:     ratelimit.New(DefAPILimit),
		ready:     ready,
		lg:        lg,
		isClosing: isClosing,
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
		mgr: managers{cfgmgr, nsmgr, crtmgr},
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

	h.registerGrpc(engine, cfg, cfgmgr)
	h.registerAPI(engine.Group("/api"), cfg, nsmgr, cfgmgr)

	if handler != nil {
		if err := handler.RegisterHTTP(engine); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if tlscfg := crtmgr.ServerTLS(); tlscfg != nil {
		h.listener = tls.NewListener(h.listener, tlscfg)
	}

	hsrv := http.Server{
		Handler:           engine.Handler(),
		ReadHeaderTimeout: DefConnTimeout,
		IdleTimeout:       DefConnTimeout,
	}

	h.wg.Run(func() {
		lg.Info("HTTP closed", zap.Error(hsrv.Serve(h.listener)))
	})

	return h, nil
}

func (h *Server) rateLimit(c *gin.Context) {
	_ = h.limit.Take()
}

func (h *Server) attachLogger(c *gin.Context) {
	path := c.Request.URL.Path

	fields := make([]zapcore.Field, 0, 9)

	fields = append(fields,
		zap.Int("status", c.Writer.Status()),
		zap.String("method", c.Request.Method),
		zap.String("path", path),
		zap.String("query", c.Request.URL.RawQuery),
		zap.String("ip", c.ClientIP()),
		zap.String("user-agent", c.Request.UserAgent()),
	)

	start := time.Now().UTC()
	c.Next()
	end := time.Now().UTC()
	latency := end.Sub(start)

	fields = append(fields,
		zap.Duration("latency", latency),
		zap.String("time", end.Format("")),
	)

	if len(c.Errors) > 0 {
		errs := make([]error, 0, len(c.Errors))
		for _, e := range c.Errors {
			errs = append(errs, e)
		}
		fields = append(fields, zap.Errors("errs", errs))
	}

	if len(c.Errors) > 0 {
		h.lg.Warn(path, fields...)
	} else if strings.HasPrefix(path, "/api/debug") || strings.HasPrefix(path, "/api/metrics") {
		h.lg.Debug(path, fields...)
	} else {
		h.lg.Info(path, fields...)
	}
}

func (h *Server) readyState(c *gin.Context) {
	if !h.ready.Load() {
		c.Abort()
		c.JSON(http.StatusInternalServerError, "service not ready")
	}
}

func (h *Server) registerGrpc(g *gin.Engine, cfg config.API, cfgmgr *mgrcfg.ConfigManager) {
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

func (h *Server) registerAPI(g *gin.RouterGroup, cfg config.API, nsmgr *mgrns.NamespaceManager, cfgmgr *mgrcfg.ConfigManager) {
	{
		adminGroup := g.Group("admin")
		if cfg.EnableBasicAuth {
			adminGroup.Use(gin.BasicAuth(gin.Accounts{cfg.User: cfg.Password}))
		}
		h.registerNamespace(adminGroup.Group("namespace"))
		h.registerConfig(adminGroup.Group("config"))
	}

	h.registerMetrics(g.Group("metrics"))
	h.registerDebug(g.Group("debug"))
}

func (h *Server) Close() error {
	err := h.listener.Close()
	h.wg.Wait()
	h.grpc.Stop()
	return err
}
