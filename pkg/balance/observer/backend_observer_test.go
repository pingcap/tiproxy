// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func newHealthCheckConfigForTest() *config.HealthCheck {
	return &config.HealthCheck{
		Enable:        true,
		Interval:      300 * time.Millisecond,
		MaxRetries:    3,
		RetryInterval: 30 * time.Millisecond,
		DialTimeout:   100 * time.Millisecond,
	}
}

// Test that the notified backend status is correct when the backend starts or shuts down.
func TestObserveBackends(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)
	ts.bo.Start(context.Background())

	backend1, info1 := ts.addBackend()
	ts.checkStatus(backend1, true, info1)
	ts.setHealth(backend1, false)
	ts.checkStatus(backend1, false, info1)
	ts.setHealth(backend1, true)
	ts.checkStatus(backend1, true, info1)

	backend2, info2 := ts.addBackend()
	ts.checkStatus(backend2, true, info2)
	ts.removeBackend(backend2)
	ts.checkStatus(backend2, false, info2)

	ts.setHealth(backend1, false)
	ts.checkStatus(backend1, false, info1)
}

func TestObserveInParallel(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	var backend string
	var info BackendInfo
	for i := 0; i < 100; i++ {
		backend, info = ts.addBackend()
	}
	ts.bo.Start(context.Background())
	result := ts.getResultFromCh()
	require.NoError(t, result.Error())
	require.Len(ts.t, result.Backends(), 100)
	// Wait for next loop.
	ts.setHealth(backend, false)
	ts.checkStatus(backend, false, info)
}

// Test that the health check can exit when the context is cancelled.
func TestCancelObserver(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	for i := 0; i < 10; i++ {
		ts.addBackend()
	}
	info, err := ts.fetcher.GetBackendList(context.Background())
	require.NoError(t, err)
	require.Len(t, info, 10)

	// Try 10 times.
	for i := 0; i < 10; i++ {
		childCtx, cancelFunc := context.WithCancel(context.Background())
		var wg waitgroup.WaitGroup
		wg.Run(func() {
			for childCtx.Err() == nil {
				ts.bo.checkHealth(childCtx, info)
			}
		})
		time.Sleep(10 * time.Millisecond)
		cancelFunc()
		wg.Wait()
	}
}

func TestDisableHealthCheck(t *testing.T) {
	ts := newObserverTestSuite(t)
	ts.bo.healthCheckConfig.Enable = false
	t.Cleanup(ts.close)

	backend1, info1 := ts.addBackend()
	ts.setHealth(backend1, false)
	ts.bo.Start(context.Background())
	ts.checkStatus(backend1, true, info1)
}

func TestMultiSubscribers(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)
	subscribers := make([]<-chan HealthResult, 0, 10)
	for i := 0; i < cap(subscribers); i++ {
		subscribers = append(subscribers, ts.bo.Subscribe(fmt.Sprintf("receiver%d", i)))
	}

	backend, info := ts.addBackend()
	ts.bo.Start(context.Background())
	ts.getResultFromCh()
	for _, subscriber := range subscribers {
		require.Eventually(t, func() bool {
			result := <-subscriber
			require.NoError(t, result.Error())
			if len(result.Backends()) == 0 {
				return false
			}
			health, ok := result.Backends()[backend]
			require.True(t, ok)
			require.True(t, health.Healthy)
			require.Equal(t, info, health.BackendInfo)
			return true
		}, 3*time.Second, time.Millisecond)
	}

	ts.setHealth(backend, false)
	ts.getResultFromCh()
	for _, subscriber := range subscribers {
		require.Eventually(t, func() bool {
			result := <-subscriber
			require.NoError(t, result.Error())
			require.Len(t, result.Backends(), 1)
			health, ok := result.Backends()[backend]
			require.True(t, ok)
			require.Equal(t, info, health.BackendInfo)
			return !health.Healthy
		}, 3*time.Second, time.Millisecond)
	}

	for i := 0; i < cap(subscribers); i++ {
		ts.bo.Unsubscribe(fmt.Sprintf("receiver%d", i))
	}
}

type observerTestSuite struct {
	t          *testing.T
	bo         *DefaultBackendObserver
	hc         *mockHealthCheck
	fetcher    *mockBackendFetcher
	subscriber <-chan HealthResult
	backendIdx int
}

func newObserverTestSuite(t *testing.T) *observerTestSuite {
	fetcher := newMockBackendFetcher()
	hc := newMockHealthCheck()
	lg, _ := logger.CreateLoggerForTest(t)
	bo := NewDefaultBackendObserver(lg, newHealthCheckConfigForTest(), fetcher, hc)
	subscriber := bo.Subscribe("receiver")
	return &observerTestSuite{
		t:          t,
		bo:         bo,
		fetcher:    fetcher,
		hc:         hc,
		subscriber: subscriber,
	}
}

func (ts *observerTestSuite) close() {
	if ts.bo != nil {
		ts.bo.Close()
		ts.bo = nil
	}
}

func (ts *observerTestSuite) checkStatus(addr string, expectedHealthy bool, info BackendInfo) {
	require.Eventually(ts.t, func() bool {
		result := ts.getResultFromCh()
		require.NoError(ts.t, result.Error())
		health, ok := result.Backends()[addr]
		if ok {
			require.Equal(ts.t, info, health.BackendInfo)
		}
		if expectedHealthy {
			return ok && health.Healthy
		} else {
			return !ok || !health.Healthy
		}
	}, 3*time.Second, 10*time.Millisecond)
	require.True(ts.t, checkBackendStatusMetrics(addr, expectedHealthy))
	cycle, err := readHealthCheckCycle()
	require.NoError(ts.t, err)
	require.Greater(ts.t, cycle.Nanoseconds(), int64(0))
	require.Less(ts.t, cycle.Nanoseconds(), 3*time.Second)
}

func (ts *observerTestSuite) getResultFromCh() HealthResult {
	select {
	case result := <-ts.subscriber:
		return result
	case <-time.After(3 * time.Second):
		ts.t.Fatal("timeout")
		return HealthResult{}
	}
}

func (ts *observerTestSuite) addBackend() (string, BackendInfo) {
	ts.backendIdx++
	addr := fmt.Sprintf("%d", ts.backendIdx)
	info := &BackendInfo{
		IP:         "127.0.0.1",
		StatusPort: uint(ts.backendIdx),
	}
	ts.fetcher.setBackend(addr, info)
	ts.hc.setBackend(addr, &BackendHealth{
		BackendInfo: *info,
		Healthy:     true,
	})
	return addr, *info
}

func (ts *observerTestSuite) setHealth(addr string, healthy bool) {
	ts.hc.setHealth(addr, healthy)
}

func (ts *observerTestSuite) removeBackend(addr string) {
	ts.fetcher.removeBackend(addr)
	ts.hc.removeBackend(addr)
}
