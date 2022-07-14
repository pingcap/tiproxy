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
	"github.com/djshow832/weir/pkg/config"
	mgrcfg "github.com/djshow832/weir/pkg/manager/config"
	mgrns "github.com/djshow832/weir/pkg/manager/namespace"
	"github.com/gin-gonic/gin"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func Register(group *gin.RouterGroup, ready *atomic.Bool, cfg *config.Proxy, logger *zap.Logger, nsmgr *mgrns.NamespaceManager, cfgmgr *mgrcfg.ConfigManager) {
	{
		adminGroup := group.Group("admin")
		if cfg.AdminServer.EnableBasicAuth {
			adminGroup.Use(gin.BasicAuth(gin.Accounts{cfg.AdminServer.User: cfg.AdminServer.Password}))
		}

		registerNamespace(adminGroup.Group("namespace"), logger, cfgmgr, nsmgr)
	}

	registerMetrics(group.Group("metrics"))
	registerDebug(group.Group("debug"), logger, nsmgr)
}
