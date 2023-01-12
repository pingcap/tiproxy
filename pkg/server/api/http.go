// Copyright 2023 PingCAP, Inc.
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

package api

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	mgrcrt "github.com/pingcap/TiProxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
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

type HTTPHandler interface {
	RegisterHTTP(c *gin.Engine) error
}

type HTTPServer struct {
	listener net.Listener
	wg       waitgroup.WaitGroup
}

func NewHTTPServer(cfg config.API, lg *zap.Logger,
	nsmgr *mgrns.NamespaceManager, cfgmgr *mgrcfg.ConfigManager,
	crtmgr *mgrcrt.CertManager, handler HTTPHandler,
	ready *atomic.Bool) (*HTTPServer, error) {
	h := &HTTPServer{}

	var err error
	h.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	limit := ratelimit.New(DefAPILimit)
	engine.Use(
		gin.Recovery(),
		ginzap.GinzapWithConfig(lg.Named("gin"), &ginzap.Config{
			UTC: true,
			SkipPaths: []string{
				"/api/debug/health",
			},
		}),
		func(c *gin.Context) {
			_ = limit.Take()
			if !ready.Load() {
				c.Abort()
				c.JSON(http.StatusInternalServerError, "service not ready")
			}
		},
	)

	register(engine.Group("/api"), cfg, lg, nsmgr, cfgmgr)

	if tlscfg := crtmgr.ServerTLS(); tlscfg != nil {
		h.listener = tls.NewListener(h.listener, tlscfg)
	}

	if handler != nil {
		if err := handler.RegisterHTTP(engine); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	h.wg.Run(func() {
		hsrv := http.Server{
			Handler:           engine.Handler(),
			ReadHeaderTimeout: DefConnTimeout,
			IdleTimeout:       DefConnTimeout,
		}
		lg.Info("HTTP closed", zap.Error(hsrv.Serve(h.listener)))
	})

	return h, nil
}

func (h *HTTPServer) Close() error {
	err := h.listener.Close()
	h.wg.Wait()
	return err
}
