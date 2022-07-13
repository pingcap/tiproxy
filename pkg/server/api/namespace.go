package api

import (
	"net/http"

	"github.com/djshow832/weir/pkg/config"
	mgrcfg "github.com/djshow832/weir/pkg/manager/config"
	mgrns "github.com/djshow832/weir/pkg/manager/namespace"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type namespaceHttpHandler struct {
	logger *zap.Logger
	cfgmgr *mgrcfg.ConfigManager
	nsmgr  *mgrns.NamespaceManager
}

func (h *namespaceHttpHandler) HandleGetNamespace(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.YAML(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc, err := h.cfgmgr.GetNamespace(c, ns)
	if err != nil {
		h.logger.Error("can not get namespace", zap.String("namespace", ns), zap.Error(err))
		c.YAML(http.StatusInternalServerError, "can not get namespace")
		return
	}

	c.YAML(http.StatusOK, nsc)
}

func (h *namespaceHttpHandler) HandleUpsertNamesapce(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.YAML(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc := &config.Namespace{}

	if c.ShouldBindYAML(nsc) != nil {
		c.YAML(http.StatusBadRequest, "bad namespace json")
		return
	}

	if err := h.cfgmgr.SetNamespace(c, nsc.Namespace, nsc); err != nil {
		c.YAML(http.StatusInternalServerError, "can not update config")
		return
	}

	if err := h.nsmgr.PrepareReloadNamespace(ns, nsc); err != nil {
		errMsg := "reload namespace error"
		h.logger.Error(errMsg, zap.Error(err), zap.String("namespace", ns))
		c.YAML(http.StatusOK, errMsg)
		return
	}

	c.YAML(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleRemoveNamespace(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.YAML(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	if err := h.cfgmgr.DelNamespace(c, ns); err != nil {
		c.YAML(http.StatusInternalServerError, "can not update config")
		return
	}

	h.nsmgr.RemoveNamespace(ns)

	c.YAML(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleCommit(c *gin.Context) {
	nss := c.QueryArray("namespace")

	if len(nss) > 0 {
		if err := h.nsmgr.CommitReloadNamespaces(nss); err != nil {
			errMsg := "commit reload namespace error"
			h.logger.Error(errMsg, zap.Error(err), zap.Strings("namespaces", nss))
			c.YAML(http.StatusInternalServerError, errMsg)
			return
		}
	}

	c.YAML(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleList(c *gin.Context) {
	nscs, err := h.cfgmgr.ListAllNamespace(c)
	if err != nil {
		errMsg := "failed to list namespaces"
		h.logger.Error(errMsg, zap.Error(err))
		c.YAML(http.StatusInternalServerError, errMsg)
		return
	}

	c.YAML(http.StatusOK, nscs)
}

func registerNamespace(group *gin.RouterGroup, logger *zap.Logger, mgrcfg *mgrcfg.ConfigManager, mgrns *mgrns.NamespaceManager) {
	h := &namespaceHttpHandler{logger, mgrcfg, mgrns}
	group.GET("/", h.HandleList)
	group.POST("/commits", h.HandleCommit)
	group.GET("/:namespace", h.HandleGetNamespace)
	group.PUT("/:namespace", h.HandleUpsertNamesapce)
	group.DELETE("/:namespace", h.HandleRemoveNamespace)
}
