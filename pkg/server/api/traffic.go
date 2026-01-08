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
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/manager"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"go.uber.org/zap"
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
	cfg.EncryptionMethod = c.PostForm("encrypt-method")

	compress := true
	if compressStr := c.PostForm("compress"); compressStr != "" {
		var err error
		if compress, err = strconv.ParseBool(compressStr); err != nil {
			h.lg.Warn("parsing argument 'compress' error, using true", zap.String("compress", c.PostForm("compress")), zap.Error(err))
			compress = true
		}
	}
	cfg.Compress = compress
	cfg.KeyFile = globalCfg.Security.EncryptionKeyPath
	if startTimeStr := c.PostForm("start-time"); startTimeStr != "" {
		startTime, err := time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.StartTime = startTime
	} else {
		cfg.StartTime = time.Now()
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
	if startTimeStr := c.PostForm("start-time"); startTimeStr != "" {
		startTime, err := time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.StartTime = startTime
	} else {
		cfg.StartTime = time.Now()
	}
	cfg.Username = c.PostForm("username")
	cfg.Password = c.PostForm("password")
	cfg.Format = cmd.TrafficFormat(c.PostForm("format"))
	cfg.ReadOnly = strings.EqualFold(c.PostForm("readonly"), "true")
	cfg.IgnoreErrs = strings.EqualFold(c.PostForm("ignore-errs"), "true")
	cfg.KeyFile = globalCfg.Security.EncryptionKeyPath
	// By default, if `cmdstarttime` is not specified, use zero time
	if cmdStartTimeStr := c.PostForm("cmdstarttime"); cmdStartTimeStr != "" {
		cmdStartTime, err := time.Parse(time.RFC3339, cmdStartTimeStr)
		if err != nil {
			cmdStartTime, err = time.Parse(time.RFC3339Nano, cmdStartTimeStr)
			if err != nil {
				c.String(http.StatusBadRequest, err.Error())
				return
			}
		}
		cfg.CommandStartTime = cmdStartTime
	}
	// By default, if `cmdendtime` is not specified, use zero time
	if cmdEndTimeStr := c.PostForm("cmdendtime"); cmdEndTimeStr != "" {
		cmdEndTime, err := time.Parse(time.RFC3339, cmdEndTimeStr)
		if err != nil {
			cmdEndTime, err = time.Parse(time.RFC3339Nano, cmdEndTimeStr)
			if err != nil {
				c.String(http.StatusBadRequest, err.Error())
				return
			}
		}
		cfg.CommandEndTime = cmdEndTime
	}
	cfg.BufSize, _ = strconv.Atoi(c.PostForm("bufsize"))
	cfg.PSCloseStrategy = cmd.PSCloseStrategy(c.PostForm("ps-close"))
	if cfg.PSCloseStrategy == "" {
		// set the default value to `directed`
		cfg.PSCloseStrategy = cmd.PSCloseStrategyDirected
	}

	cfg.CheckPointFilePath = c.PostForm("checkpointpath")
	cfg.DynamicInput = strings.EqualFold(c.PostForm("dynamicinput"), "true")
	if replayerCountStr := c.PostForm("replayercount"); replayerCountStr != "" {
		replayerCount, err := strconv.ParseUint(replayerCountStr, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.ReplayerCount = replayerCount
	}
	if replayerIndexStr := c.PostForm("replayerindex"); replayerIndexStr != "" {
		replayerIndex, err := strconv.ParseUint(replayerIndexStr, 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		cfg.ReplayerIndex = replayerIndex
	}
	cfg.OutputPath = c.PostForm("outputpath")
	cfg.Addr = c.PostForm("addr")
	cfg.DryRun = strings.EqualFold(c.PostForm("dryrun"), "true")
	cfg.FilterCommandWithRetry = strings.EqualFold(c.PostForm("filtercommandwithretry"), "true")
	cfg.WaitOnEOF = strings.EqualFold(c.PostForm("wait-on-eof"), "true")
	h.lg.Info("request: traffic replay", zap.Any("cfg", cfg))

	if err := h.mgr.ReplayJobMgr.StartReplay(cfg); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		h.lg.Info("response: traffic replay", zap.Error(err))
		return
	}
	h.lg.Info("response: traffic replay")
	c.String(http.StatusOK, "replay started")
}

func (h *Server) TrafficCancel(c *gin.Context) {
	globalCfg := h.mgr.CfgMgr.GetConfig()
	if !globalCfg.EnableTrafficReplay {
		c.String(http.StatusBadRequest, "traffic cancel is disabled")
		return
	}

	cfg := manager.CancelConfig{}
	cfg.Type = manager.Capture | manager.Replay
	if tp := c.PostForm("type"); tp != "" {
		switch strings.ToLower(tp) {
		case "capture":
			cfg.Type = manager.Capture
		case "replay":
			cfg.Type = manager.Replay
		}
	}
	cfg.Graceful = strings.EqualFold(c.PostForm("graceful"), "true")
	h.lg.Info("request: traffic cancel", zap.Any("cfg", cfg))
	result := h.mgr.ReplayJobMgr.Stop(cfg)
	h.lg.Info("response: traffic cancel", zap.String("result", result))
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
