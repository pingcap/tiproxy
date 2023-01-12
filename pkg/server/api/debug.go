// Copyright 2022 PingCAP, Inc.
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
	"net/http"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"go.uber.org/zap"
)

type debugHttpHandler struct {
	logger *zap.Logger
	nsmgr  *mgrns.NamespaceManager
}

func (h *debugHttpHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, "")
}

func (h *debugHttpHandler) Redirect(c *gin.Context) {
	errs := h.nsmgr.RedirectConnections()
	if len(errs) != 0 {
		errMsg := "redirect connections error"

		var err_fields []zap.Field
		for _, err := range errs {
			err_fields = append(err_fields, zap.Error(err))
		}
		h.logger.Error(errMsg, err_fields...)

		c.JSON(http.StatusInternalServerError, errMsg)
	} else {
		c.JSON(http.StatusOK, "")
	}
}

func registerDebug(group *gin.RouterGroup, logger *zap.Logger, nsmgr *mgrns.NamespaceManager) {
	handler := &debugHttpHandler{logger, nsmgr}
	group.POST("/redirect", handler.Redirect)
	group.GET("/health", handler.Health)
	pprof.RouteRegister(group, "/pprof")
}
