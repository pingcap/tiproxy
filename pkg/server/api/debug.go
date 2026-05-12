// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
)

type manualHealthOverride struct {
	Healthy bool   `json:"healthy"`
	Reason  string `json:"reason"`
}

func (h *Server) DebugHealth(c *gin.Context) {
	status := http.StatusOK
	health := config.HealthInfo{
		ConfigChecksum: h.mgr.CfgMgr.GetConfigChecksum(),
	}
	if healthOverride := h.manualHealthOverride.Load(); healthOverride != nil {
		if !healthOverride.Healthy {
			status = http.StatusBadGateway
			health.UnhealthyReason = healthOverride.Reason
		}
	} else if h.isClosing.Load() {
		status = http.StatusBadGateway
		health.UnhealthyReason = "server is closing"
	} else if !h.mgr.NsMgr.Ready() {
		status = http.StatusBadGateway
		health.UnhealthyReason = "namespace manager is not ready"
	}
	c.JSON(status, health)
}

func (h *Server) DebugSetManualHealthOverride(c *gin.Context) {
	var req manualHealthOverride
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, "bad health override json")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	override := &manualHealthOverride{
		Healthy: req.Healthy,
		Reason:  reason,
	}
	h.manualHealthOverride.Store(override)
	c.JSON(http.StatusOK, "")
}

func (h *Server) DebugClearManualHealthOverride(c *gin.Context) {
	h.manualHealthOverride.Store(nil)
	c.JSON(http.StatusOK, "")
}

func (h *Server) DebugRedirect(c *gin.Context) {
	errs := h.mgr.NsMgr.RedirectConnections()
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

func (h *Server) registerDebug(group *gin.RouterGroup) {
	group.POST("/redirect", h.DebugRedirect)
	group.GET("/health", h.DebugHealth)
	group.PUT("/health", h.DebugSetManualHealthOverride)
	group.DELETE("/health", h.DebugClearManualHealthOverride)
	pprof.RouteRegister(group, "/pprof")
}
