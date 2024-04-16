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

	backend1 := ts.addBackend()
	ts.checkStatus(backend1, StatusHealthy)
	ts.setHealth(backend1, StatusCannotConnect)
	ts.checkStatus(backend1, StatusCannotConnect)
	ts.setHealth(backend1, StatusHealthy)
	ts.checkStatus(backend1, StatusHealthy)

	backend2 := ts.addBackend()
	ts.checkStatus(backend2, StatusHealthy)
	ts.removeBackend(backend2)
	ts.checkStatus(backend2, StatusCannotConnect)

	ts.setHealth(backend1, StatusCannotConnect)
	ts.checkStatus(backend1, StatusCannotConnect)
}

func TestObserveInParallel(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	var backend string
	for i := 0; i < 100; i++ {
		backend = ts.addBackend()
	}
	ts.bo.Start(context.Background())
	result := ts.getResultFromCh()
	require.NoError(t, result.Error())
	require.Len(ts.t, result.Backends(), 100)
	// Wait for next loop.
	ts.setHealth(backend, StatusCannotConnect)
	ts.checkStatus(backend, StatusCannotConnect)
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

	backend1 := ts.addBackend()
	ts.setHealth(backend1, StatusCannotConnect)
	ts.bo.Start(context.Background())
	ts.checkStatus(backend1, StatusHealthy)
}

func TestMultiSubscribers(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)
	subscribers := make([]<-chan HealthResult, 0, 10)
	for i := 0; i < cap(subscribers); i++ {
		subscribers = append(subscribers, ts.bo.Subscribe(fmt.Sprintf("receiver%d", i)))
	}

	backend := ts.addBackend()
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
			require.Equal(t, StatusHealthy, health.Status)
			return true
		}, 3*time.Second, time.Millisecond)
	}

	ts.setHealth(backend, StatusCannotConnect)
	ts.getResultFromCh()
	for _, subscriber := range subscribers {
		require.Eventually(t, func() bool {
			result := <-subscriber
			require.NoError(t, result.Error())
			require.Len(t, result.Backends(), 1)
			health, ok := result.Backends()[backend]
			require.True(t, ok)
			return health.Status == StatusCannotConnect
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

func (ts *observerTestSuite) checkStatus(addr string, expectedStatus BackendStatus) {
	result := ts.getResultFromCh()
	require.NoError(ts.t, result.Error())
	health, ok := result.Backends()[addr]
	if expectedStatus == StatusHealthy {
		require.True(ts.t, ok)
		require.Equal(ts.t, expectedStatus, health.Status)
	} else {
		require.True(ts.t, !ok || health.Status == expectedStatus)
	}
	require.True(ts.t, checkBackendStatusMetrics(addr, expectedStatus))
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

func (ts *observerTestSuite) addBackend() string {
	ts.backendIdx++
	addr := fmt.Sprintf("%d", ts.backendIdx)
	ts.fetcher.setBackend(addr, &BackendInfo{
		IP:         "127.0.0.1",
		StatusPort: uint(ts.backendIdx),
	})
	ts.hc.setBackend(addr, &BackendHealth{
		Status: StatusHealthy,
	})
	return addr
}

func (ts *observerTestSuite) setHealth(addr string, health BackendStatus) {
	ts.hc.setBackend(addr, &BackendHealth{
		Status: health,
	})
}

func (ts *observerTestSuite) removeBackend(addr string) {
	ts.fetcher.removeBackend(addr)
	ts.hc.removeBackend(addr)
}
