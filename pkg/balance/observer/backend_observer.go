// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
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

var _ BackendObserver = (*DefaultBackendObserver)(nil)

type BackendObserver interface {
	Start(ctx context.Context)
	Subscribe(name string) <-chan HealthResult
	Unsubscribe(name string)
	Refresh()
	Close()
}

// DefaultBackendObserver refreshes backend list and notifies BackendEventReceiver.
type DefaultBackendObserver struct {
	sync.Mutex
	subscribers       map[string]chan HealthResult
	curBackends       map[string]*BackendHealth
	wg                waitgroup.WaitGroup
	refreshChan       chan struct{}
	fetcher           BackendFetcher
	hc                HealthCheck
	cancelFunc        context.CancelFunc
	logger            *zap.Logger
	healthCheckConfig *config.HealthCheck
	wgp               *waitgroup.WaitGroupPool
}

// NewDefaultBackendObserver creates a BackendObserver.
func NewDefaultBackendObserver(logger *zap.Logger, config *config.HealthCheck,
	backendFetcher BackendFetcher, hc HealthCheck) *DefaultBackendObserver {
	config.Check()
	bo := &DefaultBackendObserver{
		logger:            logger,
		healthCheckConfig: config,
		hc:                hc,
		wgp:               waitgroup.NewWaitGroupPool(goPoolSize, goMaxIdle),
		refreshChan:       make(chan struct{}),
		fetcher:           backendFetcher,
		subscribers:       make(map[string]chan HealthResult),
		curBackends:       make(map[string]*BackendHealth),
	}
	return bo
}

// Start starts watching.
func (bo *DefaultBackendObserver) Start(ctx context.Context) {
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
		var result HealthResult
		if err != nil {
			bo.logger.Error("fetching backends encounters error", zap.Error(err))
			result.err = err
		} else {
			result.backends = bo.checkHealth(ctx, backendInfo)
		}
		bo.updateHealthResult(result)
		bo.notifySubscribers(ctx, result)

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
		for addr, backend := range backends {
			curBackendHealth[addr] = &BackendHealth{
				BackendInfo: *backend,
				Healthy:     true,
			}
		}
		return curBackendHealth
	}

	// Each goroutine checks one backend.
	var lock sync.Mutex
	for addr, info := range backends {
		func(addr string, info *BackendInfo) {
			bo.wgp.RunWithRecover(func() {
				if ctx.Err() != nil {
					return
				}
				health := bo.hc.Check(ctx, addr, info)
				lock.Lock()
				curBackendHealth[addr] = health
				lock.Unlock()
			}, nil, bo.logger)
		}(addr, info)
	}
	bo.wgp.Wait()
	return curBackendHealth
}

func (bo *DefaultBackendObserver) Subscribe(name string) <-chan HealthResult {
	ch := make(chan HealthResult, 1)
	bo.Lock()
	bo.subscribers[name] = ch
	bo.Unlock()
	return ch
}

func (bo *DefaultBackendObserver) Unsubscribe(name string) {
	bo.Lock()
	defer bo.Unlock()
	if ch, ok := bo.subscribers[name]; ok {
		close(ch)
		delete(bo.subscribers, name)
	}
}

func (bo *DefaultBackendObserver) updateHealthResult(result HealthResult) {
	if result.err != nil {
		return
	}
	for addr, newHealth := range result.backends {
		if !newHealth.Healthy {
			continue
		}
		if oldHealth, ok := bo.curBackends[addr]; !ok || !oldHealth.Healthy {
			prev := "none"
			if oldHealth != nil {
				prev = oldHealth.String()
			}
			bo.logger.Info("update backend", zap.String("backend_addr", addr),
				zap.String("prev", prev), zap.String("cur", newHealth.String()))
			updateBackendStatusMetrics(addr, false, true)
		}
	}
	for addr, oldHealth := range bo.curBackends {
		if !oldHealth.Healthy {
			continue
		}
		if newHealth, ok := result.backends[addr]; !ok || !newHealth.Healthy {
			cur := "not in list"
			if newHealth != nil {
				cur = newHealth.String()
			}
			bo.logger.Info("update backend", zap.String("backend_addr", addr),
				zap.String("prev", oldHealth.String()), zap.String("cur", cur))
			updateBackendStatusMetrics(addr, true, false)
		}
	}
	bo.curBackends = result.backends
}

func (bo *DefaultBackendObserver) notifySubscribers(ctx context.Context, result HealthResult) {
	if ctx.Err() != nil {
		return
	}
	bo.Lock()
	defer bo.Unlock()
	for name, ch := range bo.subscribers {
		// If the subscriber is busy and doesn't read the channel, just skip and continue.
		// Now that the event may be missing, we must notify the whole backend list to the subscriber.
		select {
		case ch <- result:
		case <-time.After(50 * time.Millisecond):
			bo.logger.Warn("fails to notify health result", zap.String("receiver", name))
		case <-ctx.Done():
			return
		}
	}
}

// Close releases all resources.
func (bo *DefaultBackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
	bo.wg.Wait()
	bo.Lock()
	for _, ch := range bo.subscribers {
		close(ch)
	}
	bo.Unlock()
	bo.wgp.Close()
}
