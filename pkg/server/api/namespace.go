// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	mconfig "github.com/pingcap/tiproxy/pkg/manager/config"
)

func (h *Server) NamespaceGet(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc, err := h.mgr.CfgMgr.GetNamespace(c, ns)
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not get namespace[%s]: %+v", ns, err),
		})
		c.JSON(http.StatusInternalServerError, "can not get namespace")
		return
	}

	c.JSON(http.StatusOK, nsc)
}

func (h *Server) NamespaceUpsert(c *gin.Context) {
	nsc := &config.Namespace{}
	if nsc.Namespace == "" {
		nsc.Namespace = c.Param("namespace")
	}

	if c.ShouldBindJSON(nsc) != nil {
		c.JSON(http.StatusBadRequest, "bad namespace json")
		return
	}

	if err := h.mgr.CfgMgr.SetNamespace(c, nsc.Namespace, nsc); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not update namespace[%s]: %+v", nsc.Namespace, err),
		})
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *Server) NamespaceRemove(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	if err := h.mgr.CfgMgr.DelNamespace(c, ns); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not update namespace[%s]: %+v", ns, err),
		})
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *Server) NamespaceCommit(c *gin.Context) {
	nsNames := c.QueryArray("namespace")

	var nss []*config.Namespace
	var nssDelete []bool
	if len(nsNames) == 0 {
		var err error
		nss, err = h.mgr.CfgMgr.ListAllNamespace(c)
		if err != nil {
			c.Errors = append(c.Errors, &gin.Error{
				Type: gin.ErrorTypePrivate,
				Err:  errors.Errorf("failed to list all namespace: %+v", err),
			})
			c.JSON(http.StatusInternalServerError, "failed to list all namespaces")
			return
		}
		// Leave nssDelete nil. CommitNamespaces treats that as a full snapshot and
		// drops live namespaces whose stored record has already been removed.
	} else {
		nss = make([]*config.Namespace, len(nsNames))
		nssDelete = make([]bool, len(nsNames))
		for i, nsName := range nsNames {
			ns, err := h.mgr.CfgMgr.GetNamespace(c, nsName)
			if err != nil {
				// DELETE only removes the stored record. If that namespace is still
				// routing, this commit has to carry an explicit delete so the live
				// table drops it.
				if errors.Is(err, mconfig.ErrNoResults) {
					if _, live := h.mgr.NsMgr.GetNamespace(nsName); live {
						nss[i] = &config.Namespace{Namespace: nsName}
						nssDelete[i] = true
						continue
					}
				}
				c.Errors = append(c.Errors, &gin.Error{
					Type: gin.ErrorTypePrivate,
					Err:  errors.Errorf("failed to get namespace[%s]: %+v", nsName, err),
				})
				c.JSON(http.StatusInternalServerError, "failed to get namespace")
				return
			}
			nss[i] = ns
		}
	}

	if err := h.mgr.NsMgr.CommitNamespaces(nss, nssDelete); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("failed to reload namespaces: %v, %+v", nss, err),
		})
		c.JSON(http.StatusInternalServerError, "failed to reload namespaces")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *Server) NamespaceList(c *gin.Context) {
	nscs, err := h.mgr.CfgMgr.ListAllNamespace(c)
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("failed to list namespaces: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "failed to list namespaces")
		return
	}
	if nscs != nil {
		c.JSON(http.StatusOK, nscs)
	} else {
		c.JSON(http.StatusOK, "")
	}
}

func (h *Server) registerNamespace(group *gin.RouterGroup) {
	group.GET("/", h.NamespaceList)
	group.POST("/commit", h.NamespaceCommit)
	group.GET("/:namespace", h.NamespaceGet)
	group.PUT("/:namespace", h.NamespaceUpsert)
	group.PUT("/", h.NamespaceUpsert)
	group.DELETE("/:namespace", h.NamespaceRemove)
}
