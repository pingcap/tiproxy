// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
)

// HealthCheck is used to check the backends of one backend. One can pass a customized backends check function to the observer.
type HealthCheck interface {
	Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth
}

const (
	statusPathSuffix = "/status"
)

type DefaultHealthCheck struct {
	cfg     *config.HealthCheck
	logger  *zap.Logger
	httpCli *http.Client
	httpTLS bool
}

func NewDefaultHealthCheck(httpCli *http.Client, cfg *config.HealthCheck, logger *zap.Logger) *DefaultHealthCheck {
	if httpCli == nil {
		httpCli = http.DefaultClient
	}
	httpTLS := false
	if v, ok := httpCli.Transport.(*http.Transport); ok && v != nil && v.TLSClientConfig != nil {
		httpTLS = true
	}
	return &DefaultHealthCheck{
		httpCli: httpCli,
		httpTLS: httpTLS,
		cfg:     cfg,
		logger:  logger,
	}
}

func (dhc *DefaultHealthCheck) Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth {
	bh := &BackendHealth{
		Status: StatusHealthy,
	}
	if !dhc.cfg.Enable {
		return bh
	}
	// Skip checking the Status port if it's not fetched.
	if info != nil && len(info.IP) > 0 {
		// When a backend gracefully shut down, the Status port returns 500 but the SQL port still accepts
		// new connections, so we must check the Status port first.
		schema := "http"
		if dhc.httpTLS {
			schema = "https"
		}
		httpCli := *dhc.httpCli
		httpCli.Timeout = dhc.cfg.DialTimeout
		url := fmt.Sprintf("%s://%s:%d%s", schema, info.IP, info.StatusPort, statusPathSuffix)
		err := dhc.connectWithRetry(ctx, func() error {
			resp, err := httpCli.Get(url)
			if err == nil {
				if resp.StatusCode != http.StatusOK {
					err = backoff.Permanent(errors.Errorf("http Status %d", resp.StatusCode))
				}
				if ignoredErr := resp.Body.Close(); ignoredErr != nil {
					dhc.logger.Warn("close http response in backends check failed", zap.Error(ignoredErr))
				}
			}
			return err
		})
		if err != nil {
			bh.Status = StatusCannotConnect
			bh.PingErr = errors.Wrapf(err, "connect Status port failed")
			return bh
		}
	}

	// Also dial the SQL port just in case that the SQL port hangs.
	var serverVersion string
	err := dhc.connectWithRetry(ctx, func() error {
		startTime := time.Now()
		conn, err := net.DialTimeout("tcp", addr, dhc.cfg.DialTimeout)
		setPingBackendMetrics(addr, err == nil, startTime)
		if err != nil {
			return err
		}
		if err = conn.SetReadDeadline(time.Now().Add(dhc.cfg.DialTimeout)); err != nil {
			return err
		}
		serverVersion, err = pnet.ReadServerVersion(conn)
		if ignoredErr := conn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			dhc.logger.Warn("close connection in backends check failed", zap.Error(ignoredErr))
		}
		bh.ServerVersion = serverVersion
		return err
	})
	if err != nil {
		bh.Status = StatusCannotConnect
		bh.PingErr = errors.Wrapf(err, "connect sql port failed")
	}
	return bh
}

func (dhc *DefaultHealthCheck) connectWithRetry(ctx context.Context, connect func() error) error {
	err := backoff.Retry(func() error {
		err := connect()
		if !isRetryableError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx))
	return err
}

// When the server refused to connect, the port is shut down, so no need to retry.
var notRetryableError = []string{
	"connection refused",
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, errStr := range notRetryableError {
		if strings.Contains(msg, errStr) {
			return false
		}
	}
	return true
}
