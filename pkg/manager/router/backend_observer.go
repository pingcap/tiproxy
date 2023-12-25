// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

type BackendStatus int

func (bs BackendStatus) ToScore() int {
	return statusScores[bs]
}

func (bs BackendStatus) String() string {
	status, ok := statusNames[bs]
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

type BackendHealth struct {
	Status BackendStatus
	// The error occurred when backends check fails. It's used to log why the backend becomes unhealthy.
	PingErr error
	// The backend version that returned to the client during handshake.
	ServerVersion string
}

func (bh *BackendHealth) String() string {
	str := fmt.Sprintf("Status: %s", bh.Status.String())
	if bh.PingErr != nil {
		str += fmt.Sprintf(", err: %s", bh.PingErr.Error())
	}
	return str
}

// BackendEventReceiver receives the event of backend Status change.
type BackendEventReceiver interface {
	// OnBackendChanged is called when the backend list changes.
	OnBackendChanged(backends map[string]*BackendHealth, err error)
}

// BackendInfo stores the Status info of each backend.
type BackendInfo struct {
	IP         string
	StatusPort uint
}

// BackendObserver refreshes backend list and notifies BackendEventReceiver.
type BackendObserver struct {
	logger            *zap.Logger
	healthCheckConfig *config.HealthCheck
	// The current backend Status synced to the receiver.
	curBackendInfo map[string]*BackendHealth
	fetcher        BackendFetcher
	hc             HealthCheck
	eventReceiver  BackendEventReceiver
	wg             waitgroup.WaitGroup
	cancelFunc     context.CancelFunc
	refreshChan    chan struct{}
}

// StartBackendObserver creates a BackendObserver and starts watching.
func StartBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, config *config.HealthCheck,
	backendFetcher BackendFetcher, hc HealthCheck) *BackendObserver {
	bo := NewBackendObserver(logger, eventReceiver, config, backendFetcher, hc)
	bo.Start()
	return bo
}

// NewBackendObserver creates a BackendObserver.
func NewBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, config *config.HealthCheck,
	backendFetcher BackendFetcher, hc HealthCheck) *BackendObserver {
	bo := &BackendObserver{
		logger:            logger,
		healthCheckConfig: config,
		curBackendInfo:    make(map[string]*BackendHealth),
		hc:                hc,
		eventReceiver:     eventReceiver,
		refreshChan:       make(chan struct{}),
		fetcher:           backendFetcher,
	}
	return bo
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
		startTime := time.Now()
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

		cost := time.Since(startTime)
		metrics.HealthCheckCycleGauge.Set(cost.Seconds())
		wait := bo.healthCheckConfig.Interval - cost
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-bo.refreshChan:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]*BackendHealth {
	curBackendHealth := make(map[string]*BackendHealth, len(backends))
	for addr, info := range backends {
		if ctx.Err() != nil {
			return nil
		}
		curBackendHealth[addr] = bo.hc.Check(ctx, addr, info)
	}
	return curBackendHealth
}

func (bo *BackendObserver) notifyIfChanged(bhMap map[string]*BackendHealth) {
	updatedBackends := make(map[string]*BackendHealth)
	for addr, lastHealth := range bo.curBackendInfo {
		if lastHealth.Status == StatusHealthy {
			if newHealth, ok := bhMap[addr]; !ok {
				updatedBackends[addr] = &BackendHealth{
					Status:  StatusCannotConnect,
					PingErr: errors.New("removed from backend list"),
				}
				updateBackendStatusMetrics(addr, lastHealth.Status, StatusCannotConnect)
			} else if newHealth.Status != StatusHealthy {
				updatedBackends[addr] = newHealth
				updateBackendStatusMetrics(addr, lastHealth.Status, newHealth.Status)
			}
		}
	}
	for addr, newHealth := range bhMap {
		if newHealth.Status == StatusHealthy {
			lastHealth, ok := bo.curBackendInfo[addr]
			if !ok {
				lastHealth = &BackendHealth{
					Status: StatusCannotConnect,
				}
			}
			if lastHealth.Status != StatusHealthy {
				updatedBackends[addr] = newHealth
				updateBackendStatusMetrics(addr, lastHealth.Status, newHealth.Status)
			} else if lastHealth.ServerVersion != newHealth.ServerVersion {
				// Not possible here: the backend finishes upgrading between two backends checks.
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
