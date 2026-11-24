// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

func (h *Server) ConfigSet(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("fail to read config: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "fail to read config")
		return
	}

	if err := h.mgr.CfgMgr.SetTOMLConfig(data); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not update config: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *Server) ConfigGet(c *gin.Context) {
	// TiDB cluster_config uses format=json, while tiproxyctl expects toml (both PUT and GET) by default.
	// Users can choose the format on TiDB-Dashboard.
	if strings.EqualFold(c.Query("format"), "json") || c.GetHeader("Accept") == "application/json" {
		c.JSON(http.StatusOK, h.mgr.CfgMgr.GetConfig())
	} else {
		c.TOML(http.StatusOK, h.mgr.CfgMgr.GetConfig())
	}
}

func (h *Server) registerConfig(group *gin.RouterGroup) {
	group.PUT("/", h.ConfigSet)
	group.GET("/", h.ConfigGet)
}
