package api

import (
	"net/http"

	mgrcfg "github.com/djshow832/weir/pkg/manager/config"
	mgrns "github.com/djshow832/weir/pkg/manager/namespace"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	ParamNamespace = "namespace"
)

type namespaceHttpHandler struct {
	logger *zap.Logger
	cfgmgr *mgrcfg.ConfigManager
	nsmgr  *mgrns.NamespaceManager
}

func (h *namespaceHttpHandler) HandleRemoveNamespace(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	h.nsmgr.RemoveNamespace(ns)

	h.logger.Info("remove namespace success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (h *namespaceHttpHandler) HandlePrepareReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	nscfg, err := h.cfgmgr.GetNamespace(c, ns)
	if err != nil {
		errMsg := "get namespace value from configcenter error"
		h.logger.Error(errMsg, zap.Error(err))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	if err := h.nsmgr.PrepareReloadNamespace(ns, nscfg); err != nil {
		errMsg := "prepare reload namespace error"
		h.logger.Error(errMsg, zap.Error(err), zap.String("namespace", ns))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	h.logger.Info("prepare reload success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func (h *namespaceHttpHandler) HandleCommitReload(c *gin.Context) {
	ns := c.Param(ParamNamespace)
	if ns == "" {
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusBadRequest, "bad namespace parameter"))
		return
	}

	if err := h.nsmgr.CommitReloadNamespaces([]string{ns}); err != nil {
		errMsg := "commit reload namespace error"
		h.logger.Error(errMsg, zap.Error(err), zap.String("namespace", ns))
		c.JSON(http.StatusOK, CreateJsonResp(http.StatusInternalServerError, errMsg))
		return
	}

	h.logger.Info("commit reload success", zap.String("namespace", ns))
	c.JSON(http.StatusOK, CreateSuccessJsonResp())
}

func registerNamespace(group *gin.RouterGroup, logger *zap.Logger, mgrcfg *mgrcfg.ConfigManager, mgrns *mgrns.NamespaceManager) {
	h := &namespaceHttpHandler{logger, mgrcfg, mgrns}
	//group.PUT("/upsert/:namespace", h.HandleUpsertNamesapce)
	group.POST("/remove/:namespace", h.HandleRemoveNamespace)
	group.POST("/reload/prepare/:namespace", h.HandlePrepareReload)
	group.POST("/reload/commit/:namespace", h.HandleCommitReload)
}
