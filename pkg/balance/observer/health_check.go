// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/util/httputil"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"go.uber.org/zap"
)

// HealthCheck is used to check the backends of one backend. One can pass a customized health check function to the observer.
type HealthCheck interface {
	Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth
}

const (
	statusPathSuffix = "/status"
)

type backendHttpStatusRespBody struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

type DefaultHealthCheck struct {
	cfg        *config.HealthCheck
	logger     *zap.Logger
	httpCli    *http.Client
	httpSchema string
}

func NewDefaultHealthCheck(httpCli *http.Client, cfg *config.HealthCheck, logger *zap.Logger) *DefaultHealthCheck {
	if httpCli == nil {
		httpCli = http.DefaultClient
	}
	httpSchema := "http"
	if v, ok := httpCli.Transport.(*http.Transport); ok && v != nil && v.TLSClientConfig != nil {
		httpSchema = "https"
	}
	return &DefaultHealthCheck{
		httpCli:    httpCli,
		httpSchema: httpSchema,
		cfg:        cfg,
		logger:     logger,
	}
}

func (dhc *DefaultHealthCheck) Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth {
	bh := &BackendHealth{
		BackendInfo: *info,
		Healthy:     true,
	}
	if !dhc.cfg.Enable {
		return bh
	}
	dhc.checkStatusPort(ctx, info, bh)
	if !bh.Healthy {
		return bh
	}
	dhc.checkSqlPort(ctx, addr, bh)
	return bh
}

func (dhc *DefaultHealthCheck) checkSqlPort(ctx context.Context, addr string, bh *BackendHealth) {
	// Also dial the SQL port just in case that the SQL port hangs.
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	err := httputil.ConnectWithRetry(func() error {
		startTime := monotime.Now()
		conn, err := net.DialTimeout("tcp", addr, dhc.cfg.DialTimeout)
		setPingBackendMetrics(addr, startTime)
		if err != nil {
			return err
		}
		if err = conn.SetReadDeadline(time.Now().Add(dhc.cfg.DialTimeout)); err != nil {
			return err
		}
		if err = pnet.CheckSqlPort(conn); err != nil {
			return err
		}
		if ignoredErr := conn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			dhc.logger.Warn("close connection in health check failed", zap.Error(ignoredErr))
		}
		return err
	}, b)
	if err != nil {
		bh.Healthy = false
		bh.PingErr = errors.Wrapf(err, "connect sql port failed")
	}
}

// When a backend gracefully shut down, the status port returns 500 but the SQL port still accepts
// new connections.
func (dhc *DefaultHealthCheck) checkStatusPort(ctx context.Context, info *BackendInfo, bh *BackendHealth) {
	if ctx.Err() != nil {
		return
	}
	// Using static backends, no status port.
	if info == nil || len(info.IP) == 0 {
		return
	}

	httpCli := *dhc.httpCli
	httpCli.Timeout = dhc.cfg.DialTimeout
	addr := net.JoinHostPort(info.IP, strconv.Itoa(int(info.StatusPort)))
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	resp, err := httputil.Get(httpCli, dhc.httpSchema, addr, statusPathSuffix, b)
	if err == nil {
		var respBody backendHttpStatusRespBody
		err = json.Unmarshal(resp, &respBody)
		if err != nil {
			dhc.logger.Error("unmarshal body in healthy check failed", zap.String("addr", addr), zap.String("resp body", string(resp)), zap.Error(err))
		} else {
			bh.ServerVersion = respBody.Version
		}
	}

	if err != nil {
		bh.Healthy = false
		bh.PingErr = errors.Wrapf(err, "connect status port failed")
	}
}
