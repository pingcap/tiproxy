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

	"github.com/pingcap/TiProxy/lib/config"
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

const (
	ttlPathSuffix    = "/ttl"
	infoPathSuffix   = "/info"
	statusPathSuffix = "/status"
)

// BackendEventReceiver receives the event of backend status change.
type BackendEventReceiver interface {
	// OnBackendChanged is called when the backend list changes.
	OnBackendChanged(backends map[string]BackendStatus, err error)
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
	curBackendInfo map[string]BackendStatus
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
		curBackendInfo:    make(map[string]BackendStatus),
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
			bo.logger.Warn("fetching backends encounters error", zap.Error(err))
			bo.eventReceiver.OnBackendChanged(nil, err)
		} else {
			backendStatus := bo.checkHealth(ctx, backendInfo)
			if ctx.Err() != nil {
				return
			}
			bo.notifyIfChanged(backendStatus)
		}
		select {
		case <-time.After(bo.healthCheckConfig.Interval):
		case <-bo.refreshChan:
		case <-ctx.Done():
			return
		}
	}
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]BackendStatus {
	curBackendStatus := make(map[string]BackendStatus, len(backends))
	for addr, info := range backends {
		if !bo.healthCheckConfig.Enable {
			curBackendStatus[addr] = StatusHealthy
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		retriedTimes := 0
		connectWithRetry := func(connect func() error) error {
			var err error
			for ; retriedTimes < bo.healthCheckConfig.MaxRetries; retriedTimes++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err = connect(); err == nil {
					return nil
				}
				if !isRetryableError(err) {
					break
				}
				if retriedTimes < bo.healthCheckConfig.MaxRetries-1 {
					time.Sleep(bo.healthCheckConfig.RetryInterval)
				}
			}
			return err
		}

		// Skip checking the status port if it's not fetched.
		if info != nil && len(info.IP) > 0 {
			// When a backend gracefully shut down, the status port returns 500 but the SQL port still accepts
			// new connections, so we must check the status port first.
			schema := "http"
			if bo.httpTLS {
				schema = "https"
			}
			url := fmt.Sprintf("%s://%s:%d%s", schema, info.IP, info.StatusPort, statusPathSuffix)
			var resp *http.Response
			err := connectWithRetry(func() error {
				var err error
				if resp, err = bo.httpCli.Get(url); err == nil {
					if err := resp.Body.Close(); err != nil {
						bo.logger.Error("close http response in health check failed", zap.Error(err))
					}
				}
				return err
			})
			if err != nil || resp.StatusCode != http.StatusOK {
				continue
			}
		}

		// Also dial the SQL port just in case that the SQL port hangs.
		err := connectWithRetry(func() error {
			conn, err := net.DialTimeout("tcp", addr, bo.healthCheckConfig.DialTimeout)
			if err == nil {
				if err := conn.Close(); err != nil && !pnet.IsDisconnectError(err) {
					bo.logger.Error("close connection in health check failed", zap.Error(err))
				}
			}
			return err
		})
		if err == nil {
			curBackendStatus[addr] = StatusHealthy
		}
	}
	return curBackendStatus
}

func (bo *BackendObserver) notifyIfChanged(backendStatus map[string]BackendStatus) {
	updatedBackends := make(map[string]BackendStatus)
	for addr, lastStatus := range bo.curBackendInfo {
		if lastStatus == StatusHealthy {
			if newStatus, ok := backendStatus[addr]; !ok {
				updatedBackends[addr] = StatusCannotConnect
				updateBackendStatusMetrics(addr, lastStatus, StatusCannotConnect)
			} else if newStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
				updateBackendStatusMetrics(addr, lastStatus, newStatus)
			}
		}
	}
	for addr, newStatus := range backendStatus {
		if newStatus == StatusHealthy {
			lastStatus, ok := bo.curBackendInfo[addr]
			if !ok {
				lastStatus = StatusCannotConnect
			}
			if lastStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
				updateBackendStatusMetrics(addr, lastStatus, newStatus)
			}
		}
	}
	// Notify it even when the updatedBackends is empty, in order to clear the last error.
	bo.eventReceiver.OnBackendChanged(updatedBackends, nil)
	bo.curBackendInfo = backendStatus
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
