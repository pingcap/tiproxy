// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

func (h *HTTPServer) ConfigSet(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("fail to read config: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "fail to read config")
		return
	}

	if err := h.mgr.cfg.SetTOMLConfig(data); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not update config: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *HTTPServer) ConfigGet(c *gin.Context) {
	switch c.Query("format") {
	case "json":
		c.JSON(http.StatusOK, h.mgr.cfg.GetConfig())
	default:
		c.TOML(http.StatusOK, h.mgr.cfg.GetConfig())
	}
}

func (h *HTTPServer) registerConfig(group *gin.RouterGroup) {
	group.PUT("/", h.ConfigSet)
	group.GET("/", h.ConfigGet)
}
