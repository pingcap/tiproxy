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

package router

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type BackendStatus int

func (bs *BackendStatus) ToScore() int {
	return statusScores[*bs]
}

func (bs *BackendStatus) String() string {
	status, ok := statusNames[*bs]
	if !ok {
		return "unknown"
	}
	return status
}

const (
	StatusHealthy BackendStatus = iota
	StatusCannotConnect
	StatusMemoryHigh
	StatusRunSlow
	StatusSchemaOutdated
)

var statusNames = map[BackendStatus]string{
	StatusHealthy:        "healthy",
	StatusCannotConnect:  "down",
	StatusMemoryHigh:     "memory high",
	StatusRunSlow:        "run slow",
	StatusSchemaOutdated: "schema outdated",
}

var statusScores = map[BackendStatus]int{
	StatusHealthy:        0,
	StatusCannotConnect:  10000000,
	StatusMemoryHigh:     5000,
	StatusRunSlow:        5000,
	StatusSchemaOutdated: 10000000,
}

type backendHealth struct {
	status        BackendStatus
	pingErr       error
	serverVersion string
}

func (bh *backendHealth) String() string {
	str := fmt.Sprintf("status: %s", bh.status.String())
	if bh.pingErr != nil {
		str += fmt.Sprintf(", err: %s", bh.pingErr.Error())
	}
	return str
}

const (
	ttlPathSuffix    = "/ttl"
	infoPathSuffix   = "/info"
	statusPathSuffix = "/status"
)

// BackendEventReceiver receives the event of backend status change.
type BackendEventReceiver interface {
	// OnBackendChanged is called when the backend list changes.
	OnBackendChanged(backends map[string]*backendHealth, err error)
}

// BackendInfo stores the status info of each backend.
type BackendInfo struct {
	IP         string
	StatusPort uint
}

// BackendObserver refreshes backend list and notifies BackendEventReceiver.
type BackendObserver struct {
	logger            *zap.Logger
	healthCheckConfig *config.HealthCheck
	// The current backend status synced to the receiver.
	curBackendInfo map[string]*backendHealth
	fetcher        BackendFetcher
	httpCli        *http.Client
	httpTLS        bool
	eventReceiver  BackendEventReceiver
	wg             waitgroup.WaitGroup
	cancelFunc     context.CancelFunc
	refreshChan    chan struct{}
}

// StartBackendObserver creates a BackendObserver and starts watching.
func StartBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, httpCli *http.Client,
	config *config.HealthCheck, backendFetcher BackendFetcher) (*BackendObserver, error) {
	bo, err := NewBackendObserver(logger, eventReceiver, httpCli, config, backendFetcher)
	if err != nil {
		return nil, err
	}
	bo.Start()
	return bo, nil
}

// NewBackendObserver creates a BackendObserver.
func NewBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, httpCli *http.Client,
	config *config.HealthCheck, backendFetcher BackendFetcher) (*BackendObserver, error) {
	if httpCli == nil {
		httpCli = http.DefaultClient
	}
	httpTLS := false
	if v, ok := httpCli.Transport.(*http.Transport); ok && v != nil && v.TLSClientConfig != nil {
		httpTLS = true
	}
	bo := &BackendObserver{
		logger:            logger,
		healthCheckConfig: config,
		curBackendInfo:    make(map[string]*backendHealth),
		httpCli:           httpCli,
		httpTLS:           httpTLS,
		eventReceiver:     eventReceiver,
		refreshChan:       make(chan struct{}),
	}
	bo.fetcher = backendFetcher
	return bo, nil
}

// Start starts watching.
func (bo *BackendObserver) Start() {
	childCtx, cancelFunc := context.WithCancel(context.Background())
	bo.cancelFunc = cancelFunc
	bo.wg.Run(func() {
		bo.observe(childCtx)
	})
}

// Refresh indicates the observer to refresh immediately.
func (bo *BackendObserver) Refresh() {
	// If the observer happens to be refreshing, skip this round.
	select {
	case bo.refreshChan <- struct{}{}:
	default:
	}
}

func (bo *BackendObserver) observe(ctx context.Context) {
	for ctx.Err() == nil {
		backendInfo, err := bo.fetcher.GetBackendList(ctx)
		if err != nil {
			bo.logger.Error("fetching backends encounters error", zap.Error(err))
			bo.eventReceiver.OnBackendChanged(nil, err)
		} else {
			bhMap := bo.checkHealth(ctx, backendInfo)
			if ctx.Err() != nil {
				return
			}
			bo.notifyIfChanged(bhMap)
		}
		select {
		case <-time.After(bo.healthCheckConfig.Interval):
		case <-bo.refreshChan:
		case <-ctx.Done():
			return
		}
	}
}

func (bo *BackendObserver) connectWithRetry(ctx context.Context, connect func() error) error {
	err := backoff.Retry(func() error {
		err := connect()
		if !isRetryableError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(bo.healthCheckConfig.RetryInterval), uint64(bo.healthCheckConfig.MaxRetries)), ctx))
	return err
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]*backendHealth {
	curBackendHealth := make(map[string]*backendHealth, len(backends))
	for addr, info := range backends {
		bh := &backendHealth{
			status: StatusHealthy,
		}
		curBackendHealth[addr] = bh
		if !bo.healthCheckConfig.Enable {
			continue
		}
		if ctx.Err() != nil {
			return nil
		}

		// Skip checking the status port if it's not fetched.
		if info != nil && len(info.IP) > 0 {
			// When a backend gracefully shut down, the status port returns 500 but the SQL port still accepts
			// new connections, so we must check the status port first.
			schema := "http"
			if bo.httpTLS {
				schema = "https"
			}
			httpCli := *bo.httpCli
			httpCli.Timeout = bo.healthCheckConfig.DialTimeout
			url := fmt.Sprintf("%s://%s:%d%s", schema, info.IP, info.StatusPort, statusPathSuffix)
			err := bo.connectWithRetry(ctx, func() error {
				resp, err := httpCli.Get(url)
				if err == nil {
					if resp.StatusCode != http.StatusOK {
						err = backoff.Permanent(errors.Errorf("http status %d", resp.StatusCode))
					}
					if ignoredErr := resp.Body.Close(); ignoredErr != nil {
						bo.logger.Warn("close http response in health check failed", zap.Error(ignoredErr))
					}
				}
				return err
			})
			if err != nil {
				bh.status = StatusCannotConnect
				bh.pingErr = errors.Wrapf(err, "connect status port failed")
				continue
			}
		}

		// Also dial the SQL port just in case that the SQL port hangs.
		var serverVersion string
		err := bo.connectWithRetry(ctx, func() error {
			startTime := time.Now()
			conn, err := net.DialTimeout("tcp", addr, bo.healthCheckConfig.DialTimeout)
			setPingBackendMetrics(addr, err == nil, startTime)
			if err != nil {
				return err
			}
			if err = conn.SetReadDeadline(time.Now().Add(bo.healthCheckConfig.DialTimeout)); err != nil {
				return err
			}
			serverVersion, err = pnet.ReadServerVersion(conn)
			if ignoredErr := conn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
				bo.logger.Warn("close connection in health check failed", zap.Error(ignoredErr))
			}
			bh.serverVersion = serverVersion
			return err
		})
		if err != nil {
			bh.status = StatusCannotConnect
			bh.pingErr = errors.Wrapf(err, "connect sql port failed")
		}
	}
	return curBackendHealth
}

func (bo *BackendObserver) notifyIfChanged(bhMap map[string]*backendHealth) {
	updatedBackends := make(map[string]*backendHealth)
	for addr, lastHealth := range bo.curBackendInfo {
		if lastHealth.status == StatusHealthy {
			if newHealth, ok := bhMap[addr]; !ok {
				updatedBackends[addr] = &backendHealth{
					status:  StatusCannotConnect,
					pingErr: errors.New("removed from backend list"),
				}
				updateBackendStatusMetrics(addr, lastHealth.status, StatusCannotConnect)
			} else if newHealth.status != StatusHealthy {
				updatedBackends[addr] = newHealth
				updateBackendStatusMetrics(addr, lastHealth.status, newHealth.status)
			}
		}
	}
	for addr, newHealth := range bhMap {
		if newHealth.status == StatusHealthy {
			lastHealth, ok := bo.curBackendInfo[addr]
			if !ok {
				lastHealth = &backendHealth{
					status: StatusCannotConnect,
				}
			}
			if lastHealth.status != StatusHealthy {
				updatedBackends[addr] = newHealth
				updateBackendStatusMetrics(addr, lastHealth.status, newHealth.status)
			} else if lastHealth.serverVersion != newHealth.serverVersion {
				// Not possible here: the backend finishes upgrading between two health checks.
				updatedBackends[addr] = newHealth
			}
		}
	}
	// Notify it even when the updatedBackends is empty, in order to clear the last error.
	bo.eventReceiver.OnBackendChanged(updatedBackends, nil)
	bo.curBackendInfo = bhMap
}

// Close releases all resources.
func (bo *BackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
	bo.wg.Wait()
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
