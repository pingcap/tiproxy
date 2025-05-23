// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
)

func (h *Server) registerTraffic(group *gin.RouterGroup) {
	group.POST("/capture", h.TrafficCapture)
	group.POST("/replay", h.TrafficReplay)
	group.POST("/cancel", h.TrafficCancel)
	group.GET("/show", h.TrafficShow)
}

func (h *Server) TrafficCapture(c *gin.Context) {
	globalCfg := h.mgr.CfgMgr.GetConfig()
	if !globalCfg.EnableTrafficReplay {
		c.String(http.StatusBadRequest, "traffic capture is disabled")
		return
	}

	cfg := capture.CaptureConfig{}
	cfg.Output = c.PostForm("output")
	if durationStr := c.PostForm("duration"); durationStr != "" {
		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.Duration = duration
	}

	if err := h.mgr.ReplayJobMgr.StartCapture(cfg); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "capture started")
}

func (h *Server) TrafficReplay(c *gin.Context) {
	globalCfg := h.mgr.CfgMgr.GetConfig()
	if !globalCfg.EnableTrafficReplay {
		c.String(http.StatusBadRequest, "traffic replay is disabled")
		return
	}

	cfg := replay.ReplayConfig{}
	cfg.Input = c.PostForm("input")
	if speedStr := c.PostForm("speed"); speedStr != "" {
		speed, err := strconv.ParseFloat(speedStr, 64)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.Speed = speed
	}
	cfg.Username = c.PostForm("username")
	cfg.Password = c.PostForm("password")

	if err := h.mgr.ReplayJobMgr.StartReplay(cfg); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "replay started")
}

func (h *Server) TrafficCancel(c *gin.Context) {
	globalCfg := h.mgr.CfgMgr.GetConfig()
	if !globalCfg.EnableTrafficReplay {
		c.String(http.StatusBadRequest, "traffic cancel is disabled")
		return
	}

	result := h.mgr.ReplayJobMgr.Stop()
	c.String(http.StatusOK, result)
}

func (h *Server) TrafficShow(c *gin.Context) {
	globalCfg := h.mgr.CfgMgr.GetConfig()
	if !globalCfg.EnableTrafficReplay {
		c.String(http.StatusBadRequest, "traffic show is disabled")
		return
	}

	result := h.mgr.ReplayJobMgr.Jobs()
	c.String(http.StatusOK, result)
}
