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
	"go.uber.org/zap"
)

const (
	// Goroutines are created lazily, so the pool may not create so many goroutines.
	// If the pool is full, it will still create new goroutines, but not return to the pool after use.
	goPoolSize = 100
	goMaxIdle  = time.Minute
	// The backend metric is retained for 2 hours after it's down.
	backendMetricRetention = 2 * time.Hour
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
	downBackends      map[string]time.Time
	wg                waitgroup.WaitGroup
	refreshChan       chan struct{}
	fetcher           BackendFetcher
	hc                HealthCheck
	cfgGetter         config.ConfigGetter
	cancelFunc        context.CancelFunc
	logger            *zap.Logger
	healthCheckConfig *config.HealthCheck
	wgp               *waitgroup.WaitGroupPool
}

// NewDefaultBackendObserver creates a BackendObserver.
func NewDefaultBackendObserver(logger *zap.Logger, config *config.HealthCheck, backendFetcher BackendFetcher, hc HealthCheck,
	cfgGetter config.ConfigGetter) *DefaultBackendObserver {
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
		downBackends:      make(map[string]time.Time),
		cfgGetter:         cfgGetter,
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
		startTime := time.Now()
		backendInfo, err := bo.fetcher.GetBackendList(ctx)
		var result HealthResult
		if err != nil {
			bo.logger.Error("fetching backends encounters error", zap.Error(err))
			result.err = err
		} else {
			result.backends = bo.checkHealth(ctx, backendInfo)
		}
		bo.updateHealthResult(result)
		bo.purgeBackendMetrics()
		bo.notifySubscribers(ctx, result)

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

func (bo *DefaultBackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]*BackendHealth {
	curBackendHealth := make(map[string]*BackendHealth, len(backends))
	// Serverless tier checks health in Gateway instead of in TiProxy.
	if !bo.healthCheckConfig.Enable {
		for addr, backend := range backends {
			curBackendHealth[addr] = &BackendHealth{
				BackendInfo:        *backend,
				SupportRedirection: true,
				Healthy:            true,
			}
		}
		return curBackendHealth
	}

	// Each goroutine checks one backend.
	var lock sync.Mutex
	cfg := bo.cfgGetter.GetConfig()
	for addr, info := range backends {
		func(addr string, info *BackendInfo) {
			bo.wgp.RunWithRecover(func() {
				if ctx.Err() != nil {
					return
				}
				lastHealth := bo.curBackends[addr]
				health := bo.hc.Check(ctx, addr, info, lastHealth)
				health.setLocal(cfg)
				lock.Lock()
				curBackendHealth[addr] = health
				lock.Unlock()
			}, nil, bo.logger)
		}(addr, info)
	}
	bo.wgp.Wait()
	return curBackendHealth
}

// If a backend has been down for more than backendMetricRetention, remove it from the metrics.
// For auto-scaling case, the metrics will occupy too much volume after weeks.
func (bo *DefaultBackendObserver) purgeBackendMetrics() {
	if len(bo.downBackends) == 0 {
		return
	}
	now := time.Now()
	for addr, ts := range bo.downBackends {
		if ts.Add(backendMetricRetention).Before(now) {
			bo.logger.Info("backend is down for too long, purging backend metrics", zap.String("backend", addr))
			metrics.DelBackend(addr)
			delete(bo.downBackends, addr)
		}
	}
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
			updateBackendStatusMetrics(addr, true)
			delete(bo.downBackends, addr)
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
			updateBackendStatusMetrics(addr, false)
			bo.downBackends[addr] = time.Now()
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
