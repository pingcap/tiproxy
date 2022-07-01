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
