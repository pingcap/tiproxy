// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
)

func (h *Server) NamespaceGet(c *gin.Context) {
	ns := c.Param("namespace")
	if ns == "" {
		c.JSON(http.StatusBadRequest, "bad namespace parameter")
		return
	}

	nsc, err := h.mgr.cfg.GetNamespace(c, ns)
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

	if err := h.mgr.cfg.SetNamespace(c, nsc.Namespace, nsc); err != nil {
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

	if err := h.mgr.cfg.DelNamespace(c, ns); err != nil {
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
	ns_names := c.QueryArray("namespace")

	var nss []*config.Namespace
	var nss_delete []bool
	var err error
	if len(ns_names) == 0 {
		nss, err = h.mgr.cfg.ListAllNamespace(c)
		if err != nil {
			c.Errors = append(c.Errors, &gin.Error{
				Type: gin.ErrorTypePrivate,
				Err:  errors.Errorf("failed to list all namespace: %+v", err),
			})
			c.JSON(http.StatusInternalServerError, "failed to list all namespaces")
			return
		}
	} else {
		nss = make([]*config.Namespace, len(ns_names))
		nss_delete = make([]bool, len(ns_names))
		for i, ns_name := range ns_names {
			ns, err := h.mgr.cfg.GetNamespace(c, ns_name)
			if err != nil {
				c.Errors = append(c.Errors, &gin.Error{
					Type: gin.ErrorTypePrivate,
					Err:  errors.Errorf("failed to get namespace[%s]: %+v", ns_name, err),
				})
				c.JSON(http.StatusInternalServerError, "failed to get namespace")
				return
			}
			nss[i] = ns
			nss_delete[i] = false
		}
	}

	if err := h.mgr.ns.CommitNamespaces(nss, nss_delete); err != nil {
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
	nscs, err := h.mgr.cfg.ListAllNamespace(c)
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
