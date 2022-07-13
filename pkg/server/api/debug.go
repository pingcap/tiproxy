package api

import (
	"net/http"

	mgrns "github.com/djshow832/weir/pkg/manager/namespace"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type debugHttpHandler struct {
	logger *zap.Logger
	nsmgr  *mgrns.NamespaceManager
}

func (h *debugHttpHandler) Redirect(c *gin.Context) {
	err := h.nsmgr.RedirectConnections()
	if err != nil {
		errMsg := "redirect connections error"
		h.logger.Error(errMsg, zap.Error(err))
		c.JSON(http.StatusInternalServerError, errMsg)
	} else {
		c.JSON(http.StatusOK, "")
	}
}

func registerDebug(group *gin.RouterGroup, logger *zap.Logger, nsmgr *mgrns.NamespaceManager) {
	handler := &debugHttpHandler{logger, nsmgr}
	group.POST("/redirect", handler.Redirect)
	pprof.RouteRegister(group, "/pprof")
}
