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
	"github.com/pingcap/TiProxy/lib/util/errors"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	"go.uber.org/zap"
)

type configHttpHandler struct {
	logger *zap.Logger
	cfgmgr *mgrcfg.ConfigManager
}

func (h *configHttpHandler) HandleSetConfig(c *gin.Context) {
	cfg := new(config.Config)
	if c.ShouldBindJSON(cfg) != nil {
		c.JSON(http.StatusBadRequest, "bad config json")
		return
	}

	if err := h.cfgmgr.SetConfig(c, cfg); err != nil {
		h.logger.Error("can not update config", zap.Error(err))
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *configHttpHandler) HandleGetConfig(c *gin.Context) {
	var cfg config.Config
	err := h.cfgmgr.GetConfig(c, &cfg)
	if err != nil && !errors.Is(err, mgrcfg.ErrNoResults) {
		h.logger.Error("can not get config", zap.Error(err))
		c.JSON(http.StatusInternalServerError, "can not get config")
		return
	}

	c.JSON(http.StatusOK, &cfg)
}

func registerConfig(group *gin.RouterGroup, logger *zap.Logger, cfgmgr *mgrcfg.ConfigManager) {
	h := &configHttpHandler{logger, cfgmgr}
	group.PUT("/", h.HandleSetConfig)
	group.GET("/", h.HandleGetConfig)
}
