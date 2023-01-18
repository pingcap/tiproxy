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
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/util/errors"
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
	c.TOML(http.StatusOK, h.mgr.cfg.GetConfig())
}

func (h *HTTPServer) registerConfig(group *gin.RouterGroup) {
	group.PUT("/", h.ConfigSet)
	group.GET("/", h.ConfigGet)
}
