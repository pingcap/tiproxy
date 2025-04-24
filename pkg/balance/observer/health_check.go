// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"go.uber.org/zap"
)

// HealthCheck is used to check the backends of one backend. One can pass a customized health check function to the observer.
type HealthCheck interface {
	Check(ctx context.Context, addr string, info *BackendInfo, lastHealth *BackendHealth) *BackendHealth
}

const (
	statusPathSuffix = "/status"
	configPathSuffix = "/config"

	checkSigningCertInterval = time.Minute
)

type backendHttpStatusRespBody struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

type backendHttpConfigRespBody struct {
	Security security `json:"security"`
}

type security struct {
	SessionTokenSigningCert string `json:"session-token-signing-cert"`
}

type DefaultHealthCheck struct {
	cfg     *config.HealthCheck
	logger  *zap.Logger
	httpCli *http.Client
}

func NewDefaultHealthCheck(httpCli *http.Client, cfg *config.HealthCheck, logger *zap.Logger) *DefaultHealthCheck {
	if httpCli == nil {
		httpCli = http.NewHTTPClient(func() *tls.Config { return nil })
	}
	return &DefaultHealthCheck{
		httpCli: httpCli,
		cfg:     cfg,
		logger:  logger,
	}
}

func (dhc *DefaultHealthCheck) Check(ctx context.Context, addr string, info *BackendInfo, lastBh *BackendHealth) *BackendHealth {
	bh := &BackendHealth{
		BackendInfo: *info,
		Healthy:     true,
		// Assume it has the signing cert if reading config fails.
		SupportRedirection: true,
	}
	if lastBh != nil {
		bh.SupportRedirection = lastBh.SupportRedirection
		bh.lastCheckSigningCertTime = lastBh.lastCheckSigningCertTime
	}
	if !dhc.cfg.Enable {
		return bh
	}
	dhc.checkStatusPort(ctx, info, bh)
	if !bh.Healthy {
		return bh
	}
	dhc.checkSqlPort(ctx, addr, bh)
	if !bh.Healthy {
		return bh
	}
	dhc.queryConfig(ctx, info, bh, lastBh)
	return bh
}

func (dhc *DefaultHealthCheck) checkSqlPort(ctx context.Context, addr string, bh *BackendHealth) {
	// Also dial the SQL port just in case that the SQL port hangs.
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	err := http.ConnectWithRetry(func() error {
		startTime := time.Now()
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

	addr := net.JoinHostPort(info.IP, strconv.Itoa(int(info.StatusPort)))
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	resp, err := dhc.httpCli.Get(addr, statusPathSuffix, b, dhc.cfg.DialTimeout)
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

func (dhc *DefaultHealthCheck) queryConfig(ctx context.Context, info *BackendInfo, bh *BackendHealth, lastBh *BackendHealth) {
	if ctx.Err() != nil {
		return
	}
	// Using static backends, no status port.
	if info == nil || len(info.IP) == 0 {
		return
	}

	now := time.Now()
	if bh.lastCheckSigningCertTime.Add(checkSigningCertInterval).After(now) {
		return
	}
	bh.lastCheckSigningCertTime = now

	var err error
	addr := net.JoinHostPort(info.IP, strconv.Itoa(int(info.StatusPort)))
	defer func() {
		if lastBh == nil || lastBh.SupportRedirection != bh.SupportRedirection {
			dhc.logger.Info("backend has updated signing cert", zap.String("addr", addr), zap.Bool("support_redirection", bh.SupportRedirection), zap.Error(err))
		}
	}()

	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	var resp []byte
	if resp, err = dhc.httpCli.Get(addr, configPathSuffix, b, dhc.cfg.DialTimeout); err != nil {
		return
	}
	var respBody backendHttpConfigRespBody
	if err = json.Unmarshal(resp, &respBody); err != nil {
		dhc.logger.Error("unmarshal body in healthy check failed", zap.String("addr", addr), zap.String("resp body", string(resp)), zap.Error(err))
		return
	}
	if len(respBody.Security.SessionTokenSigningCert) == 0 {
		bh.SupportRedirection = false
	}
}
