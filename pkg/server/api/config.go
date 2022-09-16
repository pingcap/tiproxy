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

	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/config"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	"go.uber.org/zap"
)

type configHttpHandler struct {
	logger *zap.Logger
	cfgmgr *mgrcfg.ConfigManager
}

func (h *configHttpHandler) HandleSetProxyConfig(c *gin.Context) {
	pco := &config.ProxyServerOnline{}
	if c.ShouldBindJSON(pco) != nil {
		c.JSON(http.StatusBadRequest, "bad proxy config json")
		return
	}

	if err := h.cfgmgr.SetProxyConfig(c, pco); err != nil {
		h.logger.Error("can not update proxy config", zap.Error(err))
		c.JSON(http.StatusInternalServerError, "can not update proxy config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *configHttpHandler) HandleGetProxyConfig(c *gin.Context) {
	pco, err := h.cfgmgr.GetProxyConfig(c)
	if err != nil {
		h.logger.Error("can not get proxy config", zap.Error(err))
		c.JSON(http.StatusInternalServerError, "can not get proxy config")
		return
	}

	c.JSON(http.StatusOK, pco)
}

func registerConfig(group *gin.RouterGroup, logger *zap.Logger, mgrcfg *mgrcfg.ConfigManager) {
	h := &configHttpHandler{logger, mgrcfg}
	group.PUT("/proxy", h.HandleSetProxyConfig)
	group.GET("/proxy", h.HandleGetProxyConfig)
}
