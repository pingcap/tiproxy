// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (h *Server) registerMetrics(group *gin.RouterGroup) {
	group.GET("/", gin.WrapF(promhttp.Handler().ServeHTTP))
}
