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
	"github.com/gin-gonic/gin"
	"github.com/pingcap/TiProxy/lib/config"
	mgrcfg "github.com/pingcap/TiProxy/pkg/manager/config"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"go.uber.org/zap"
)

func register(group *gin.RouterGroup, cfg config.API, logger *zap.Logger, nsmgr *mgrns.NamespaceManager, cfgmgr *mgrcfg.ConfigManager) {
	{
		adminGroup := group.Group("admin")
		if cfg.EnableBasicAuth {
			adminGroup.Use(gin.BasicAuth(gin.Accounts{cfg.User: cfg.Password}))
		}
		registerNamespace(adminGroup.Group("namespace"), logger.Named("namespace"), cfgmgr, nsmgr)
		registerConfig(adminGroup.Group("config"), logger.Named("config"), cfgmgr)
	}
	registerMetrics(group.Group("metrics"))
	registerDebug(group.Group("debug"), logger.Named("debug"), nsmgr)
}
