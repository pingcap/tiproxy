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
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
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
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc, err := h.cfgmgr.GetNamespace(c, ns)
	if err != nil {
		h.logger.Error("can not get namespace", zap.String("namespace", ns), zap.Error(err))
		c.JSON(http.StatusInternalServerError, "can not get namespace")
		return
	}

	c.JSON(http.StatusOK, nsc)
}

func (h *namespaceHttpHandler) HandleUpsertNamesapce(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc := &config.Namespace{}

	if c.ShouldBindJSON(nsc) != nil {
		c.JSON(http.StatusBadRequest, "bad namespace json")
		return
	}

	if err := h.cfgmgr.SetNamespace(c, nsc.Namespace, nsc); err != nil {
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleRemoveNamespace(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	if err := h.cfgmgr.DelNamespace(c, ns); err != nil {
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleCommit(c *gin.Context) {
	ns_names := c.QueryArray("namespace")

	var nss []*config.Namespace
	var nss_delete []bool
	var err error
	if len(ns_names) == 0 {
		nss, err = h.cfgmgr.ListAllNamespace(c)
		if err != nil {
			errMsg := "failed to list all namespaces"
			h.logger.Error(errMsg, zap.Error(err), zap.Any("namespaces", nss))
			c.JSON(http.StatusInternalServerError, errMsg)
			return
		}
	} else {
		nss = make([]*config.Namespace, len(ns_names))
		nss_delete = make([]bool, len(ns_names))
		for i, ns_name := range ns_names {
			ns, err := h.cfgmgr.GetNamespace(c, ns_name)
			if err != nil {
				errMsg := "failed to get namespace"
				h.logger.Error(errMsg, zap.Error(err), zap.Any("namespaces", nss))
				c.JSON(http.StatusInternalServerError, errMsg)
				return
			}
			nss[i] = ns
			nss_delete[i] = false
		}
	}

	if err := h.nsmgr.CommitNamespaces(nss, nss_delete); err != nil {
		errMsg := "commit reload namespace error"
		h.logger.Error(errMsg, zap.Error(err), zap.Any("namespaces", nss))
		c.JSON(http.StatusInternalServerError, errMsg)
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *namespaceHttpHandler) HandleList(c *gin.Context) {
	nscs, err := h.cfgmgr.ListAllNamespace(c)
	if err != nil {
		errMsg := "failed to list namespaces"
		h.logger.Error(errMsg, zap.Error(err))
		c.JSON(http.StatusInternalServerError, errMsg)
		return
	}

	c.JSON(http.StatusOK, nscs)
}

func registerNamespace(group *gin.RouterGroup, logger *zap.Logger, mgrcfg *mgrcfg.ConfigManager, mgrns *mgrns.NamespaceManager) {
	h := &namespaceHttpHandler{logger, mgrcfg, mgrns}
	group.GET("/", h.HandleList)
	group.POST("/commit", h.HandleCommit)
	group.GET("/:namespace", h.HandleGetNamespace)
	group.PUT("/:namespace", h.HandleUpsertNamesapce)
	group.DELETE("/:namespace", h.HandleRemoveNamespace)
}
