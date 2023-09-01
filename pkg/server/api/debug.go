// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
)

func (h *HTTPServer) DebugHealth(c *gin.Context) {
	status := http.StatusOK
	if h.proxy.IsClosing() {
		status = http.StatusBadGateway
	}
	c.JSON(status, config.HealthInfo{
		ConfigChecksum: h.mgr.cfg.GetConfigChecksum(),
	})
}

func (h *HTTPServer) DebugRedirect(c *gin.Context) {
	errs := h.mgr.ns.RedirectConnections()
	if len(errs) != 0 {
		for _, err := range errs {
			c.Errors = append(c.Errors, &gin.Error{
				Err:  err,
				Type: gin.ErrorTypePrivate,
			})
		}
		c.JSON(http.StatusInternalServerError, "redirect connections error")
	} else {
		c.JSON(http.StatusOK, "")
	}
}

func (h *HTTPServer) registerDebug(group *gin.RouterGroup) {
	group.POST("/redirect", h.DebugRedirect)
	group.GET("/health", h.DebugHealth)
	pprof.RouteRegister(group, "/pprof")
}
