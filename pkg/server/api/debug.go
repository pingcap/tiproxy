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
	"github.com/pingcap/TiProxy/lib/config"
)

func (h *HTTPServer) DebugHealth(c *gin.Context) {
	c.JSON(http.StatusOK, config.HealthInfo{
		ConfigVersion: h.mgr.cfg.GetConfigVersion(),
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
