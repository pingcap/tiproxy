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

const manualHealthStatusUnhealthy = "unhealthy"

type manualHealthOverride struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

func (h *Server) DebugHealth(c *gin.Context) {
	status := http.StatusOK
	health := config.HealthInfo{
		ConfigChecksum: h.mgr.CfgMgr.GetConfigChecksum(),
	}
	if h.unhealthyMark.Load() {
		status = http.StatusBadGateway
		health.UnhealthyReason = h.unhealthyReason.Load()
	} else if h.isClosing.Load() || !h.mgr.NsMgr.Ready() {
		status = http.StatusBadGateway
	}
	c.JSON(status, health)
}

func (h *Server) DebugSetHealthUnhealthy(c *gin.Context) {
	var req manualHealthOverride
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, "bad health override json")
		return
	}
	if req.Status != manualHealthStatusUnhealthy {
		c.JSON(http.StatusBadRequest, "bad health override status")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		c.JSON(http.StatusBadRequest, "health override reason is required")
		return
	}
	h.unhealthyReason.Store(reason)
	h.unhealthyMark.Store(true)
	c.JSON(http.StatusOK, "")
}

func (h *Server) DebugUnsetHealthUnhealthy(c *gin.Context) {
	h.unhealthyMark.Store(false)
	h.unhealthyReason.Store("")
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
	group.PUT("/health", h.DebugSetHealthUnhealthy)
	group.DELETE("/health", h.DebugUnsetHealthUnhealthy)
	pprof.RouteRegister(group, "/pprof")
}
