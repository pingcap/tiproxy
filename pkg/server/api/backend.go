// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type BackendReader interface {
	GetBackendMetrics() ([]byte, error)
}

func (h *Server) BackendMetrics(c *gin.Context) {
	metrics, err := h.mgr.br.GetBackendMetrics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}
	c.Writer.Header().Set("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	if _, err := c.Writer.Write(metrics); err != nil {
		h.lg.Error("write backend metrics failed", zap.Error(err))
	}
}

func (h *Server) registerBackend(group *gin.RouterGroup) {
	group.GET("/metrics", h.BackendMetrics)
}
