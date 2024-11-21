// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"go.uber.org/zap"
)

func (h *Server) registerTraffic(group *gin.RouterGroup) {
	group.POST("/capture", h.TrafficCapture)
	group.POST("/replay", h.TrafficReplay)
	group.POST("/cancel", h.TrafficStop)
	group.GET("/show", h.TrafficShow)
}

func (h *Server) TrafficCapture(c *gin.Context) {
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
	cfg.EncryptMethod = c.PostForm("encrypt-method")

	compress := true
	if compressStr := c.PostForm("compress"); compressStr != "" {
		var err error
		if compress, err = strconv.ParseBool(compressStr); err != nil {
			h.lg.Warn("parsing argument 'compress' error, using true", zap.String("compress", c.PostForm("compress")), zap.Error(err))
			compress = true
		}
	}
	cfg.Compress = compress
	cfg.KeyFile = h.mgr.CfgMgr.GetConfig().Security.Encryption.KeyPath

	if err := h.mgr.ReplayJobMgr.StartCapture(cfg); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "capture started")
}

func (h *Server) TrafficReplay(c *gin.Context) {
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
	cfg.ReadOnly = strings.EqualFold(c.PostForm("readonly"), "true")
	cfg.KeyFile = h.mgr.CfgMgr.GetConfig().Security.Encryption.KeyPath

	if err := h.mgr.ReplayJobMgr.StartReplay(cfg); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "replay started")
}

func (h *Server) TrafficStop(c *gin.Context) {
	result := h.mgr.ReplayJobMgr.Stop()
	c.String(http.StatusOK, result)
}

func (h *Server) TrafficShow(c *gin.Context) {
	result := h.mgr.ReplayJobMgr.Jobs()
	c.String(http.StatusOK, result)
}
