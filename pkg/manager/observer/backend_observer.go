// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	"go.uber.org/zap"
)

const (
	// Goroutines are created lazily, so the pool may not create so many goroutines.
	// If the pool is full, it will still create new goroutines, but not return to the pool after use.
	goPoolSize = 100
	goMaxIdle  = time.Minute
)

// BackendEventReceiver receives the event of backend status change.
type BackendEventReceiver interface {
	// OnBackendChanged is called when the backend list changes.
	OnBackendChanged(backends map[string]*BackendHealth, err error)
}

type BackendObserver interface {
	Start(ctx context.Context, eventReceiver BackendEventReceiver)
	Refresh()
	Close()
}

// DefaultBackendObserver refreshes backend list and notifies BackendEventReceiver.
type DefaultBackendObserver struct {
	logger            *zap.Logger
	healthCheckConfig *config.HealthCheck
	// The current backend status synced to the receiver.
	curBackendInfo map[string]*BackendHealth
	fetcher        BackendFetcher
	hc             HealthCheck
	wgp            *waitgroup.WaitGroupPool
	eventReceiver  BackendEventReceiver
	wg             waitgroup.WaitGroup
	cancelFunc     context.CancelFunc
	refreshChan    chan struct{}
}

// NewDefaultBackendObserver creates a BackendObserver.
func NewDefaultBackendObserver(logger *zap.Logger, config *config.HealthCheck,
	backendFetcher BackendFetcher, hc HealthCheck) *DefaultBackendObserver {
	config.Check()
	bo := &DefaultBackendObserver{
		logger:            logger,
		healthCheckConfig: config,
		curBackendInfo:    make(map[string]*BackendHealth),
		hc:                hc,
		wgp:               waitgroup.NewWaitGroupPool(goPoolSize, goMaxIdle),
		refreshChan:       make(chan struct{}),
		fetcher:           backendFetcher,
	}
	return bo
}

// Start starts watching.
func (bo *DefaultBackendObserver) Start(ctx context.Context, eventReceiver BackendEventReceiver) {
	bo.eventReceiver = eventReceiver
	childCtx, cancelFunc := context.WithCancel(ctx)
	bo.cancelFunc = cancelFunc
	// Failing to observe backends may cause even more serious problems than TiProxy reboot, so we don't recover panics.
	bo.wg.Run(func() {
		bo.observe(childCtx)
	})
}

// Refresh indicates the observer to refresh immediately.
func (bo *DefaultBackendObserver) Refresh() {
	// If the observer happens to be refreshing, skip this round.
	select {
	case bo.refreshChan <- struct{}{}:
	default:
	}
}

func (bo *DefaultBackendObserver) observe(ctx context.Context) {
	for ctx.Err() == nil {
		startTime := monotime.Now()
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

		cost := monotime.Since(startTime)
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

func (bo *DefaultBackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]*BackendHealth {
	curBackendHealth := make(map[string]*BackendHealth, len(backends))
	// Serverless tier checks health in Gateway instead of in TiProxy.
	if !bo.healthCheckConfig.Enable {
		for addr := range backends {
			curBackendHealth[addr] = &BackendHealth{
				Status: StatusHealthy,
			}
		}
		return curBackendHealth
	}

	// Each goroutine checks one backend.
	var lock sync.Mutex
	for addr, info := range backends {
		func(addr string) {
			bo.wgp.RunWithRecover(func() {
				if ctx.Err() != nil {
					return
				}
				health := bo.hc.Check(ctx, addr, info)
				lock.Lock()
				curBackendHealth[addr] = health
				lock.Unlock()
			}, nil, bo.logger)
		}(addr)
	}
	bo.wgp.Wait()
	return curBackendHealth
}

func (bo *DefaultBackendObserver) notifyIfChanged(bhMap map[string]*BackendHealth) {
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
func (bo *DefaultBackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
	bo.wg.Wait()
	bo.wgp.Close()
}
