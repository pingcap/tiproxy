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
)

func (h *HTTPServer) NamespaceGet(c *gin.Context) {
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

func (h *HTTPServer) NamespaceUpsert(c *gin.Context) {
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

	if err := h.mgr.cfg.SetNamespace(c, nsc.Namespace, nsc); err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("can not update namespace[%s]: %+v", ns, err),
		})
		c.JSON(http.StatusInternalServerError, "can not update config")
		return
	}

	c.JSON(http.StatusOK, "")
}

func (h *HTTPServer) NamespaceRemove(c *gin.Context) {
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

func (h *HTTPServer) NamespaceCommit(c *gin.Context) {
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

func (h *HTTPServer) NamespaceList(c *gin.Context) {
	nscs, err := h.mgr.cfg.ListAllNamespace(c)
	if err != nil {
		c.Errors = append(c.Errors, &gin.Error{
			Type: gin.ErrorTypePrivate,
			Err:  errors.Errorf("failed to list namespaces: %+v", err),
		})
		c.JSON(http.StatusInternalServerError, "failed to list namespaces")
		return
	}

	c.JSON(http.StatusOK, nscs)
}

func (h *HTTPServer) registerNamespace(group *gin.RouterGroup) {
	group.GET("/", h.NamespaceList)
	group.POST("/commit", h.NamespaceCommit)
	group.GET("/:namespace", h.NamespaceGet)
	group.PUT("/:namespace", h.NamespaceUpsert)
	group.DELETE("/:namespace", h.NamespaceRemove)
}
