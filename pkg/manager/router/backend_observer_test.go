// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

type mockEventReceiver struct {
	backendChan chan map[string]*BackendHealth
	errChan     chan error
}

func (mer *mockEventReceiver) OnBackendChanged(backends map[string]*BackendHealth, err error) {
	if err != nil {
		mer.errChan <- err
	} else if len(backends) > 0 {
		mer.backendChan <- backends
	}
}

func newMockEventReceiver(backendChan chan map[string]*BackendHealth, errChan chan error) *mockEventReceiver {
	return &mockEventReceiver{
		backendChan: backendChan,
		errChan:     errChan,
	}
}

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
	ts.bo.Start()

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

// Test that the health check can exit when the context is cancelled.
func TestCancelObserver(t *testing.T) {
	ts := newObserverTestSuite(t)
	t.Cleanup(ts.close)

	for i := 0; i < 10; i++ {
		ts.addBackend()
	}
	info, err := ts.fetcher.GetBackendList(context.Background(), false)
	require.NoError(t, err)
	require.Len(t, info, 10)

	// Try 10 times.
	for i := 0; i < 10; i++ {
		childCtx, cancelFunc := context.WithCancel(context.Background())
		var wg waitgroup.WaitGroup
		wg.Run(func() {
			for childCtx.Err() != nil {
				ts.bo.checkHealth(childCtx, info)
			}
		})
		time.Sleep(10 * time.Millisecond)
		cancelFunc()
		wg.Wait()
	}
}

type observerTestSuite struct {
	t           *testing.T
	bo          *BackendObserver
	hc          *mockHealthCheck
	fetcher     *mockBackendFetcher
	backendIdx  int
	backendChan chan map[string]*BackendHealth
}

func newObserverTestSuite(t *testing.T) *observerTestSuite {
	backendChan := make(chan map[string]*BackendHealth, 1)
	mer := newMockEventReceiver(backendChan, make(chan error, 1))
	fetcher := newMockBackendFetcher()
	hc := newMockHealthCheck()
	lg, _ := logger.CreateLoggerForTest(t)
	bo := NewBackendObserver(lg, mer, newHealthCheckConfigForTest(), fetcher, hc)
	return &observerTestSuite{
		t:           t,
		bo:          bo,
		fetcher:     fetcher,
		hc:          hc,
		backendChan: backendChan,
	}
}

func (ts *observerTestSuite) close() {
	if ts.bo != nil {
		ts.bo.Close()
		ts.bo = nil
	}
}

func (ts *observerTestSuite) checkStatus(addr string, expectedStatus BackendStatus) {
	backends := ts.getBackendsFromCh()
	require.Equal(ts.t, 1, len(backends))
	health, ok := backends[addr]
	require.True(ts.t, ok)
	require.Equal(ts.t, expectedStatus, health.Status)
	require.True(ts.t, checkBackendStatusMetrics(addr, health.Status))
	cycle, err := readHealthCheckCycle()
	require.NoError(ts.t, err)
	require.Greater(ts.t, cycle.Nanoseconds(), int64(0))
	require.Less(ts.t, cycle.Nanoseconds(), 3*time.Second)
}

func (ts *observerTestSuite) getBackendsFromCh() map[string]*BackendHealth {
	var backends map[string]*BackendHealth
	select {
	case backends = <-ts.backendChan:
	case <-time.After(3 * time.Second):
		ts.t.Fatal("timeout")
	}
	return backends
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

type mockBackendFetcher struct {
	sync.Mutex
	backends map[string]*BackendInfo
}

func newMockBackendFetcher() *mockBackendFetcher {
	return &mockBackendFetcher{
		backends: make(map[string]*BackendInfo),
	}
}

func (mbf *mockBackendFetcher) GetBackendList(context.Context, bool) (map[string]*BackendInfo, error) {
	mbf.Lock()
	defer mbf.Unlock()
	backends := make(map[string]*BackendInfo, len(mbf.backends))
	for addr, backend := range mbf.backends {
		backends[addr] = backend
	}
	return backends, nil
}

func (mbf *mockBackendFetcher) setBackend(addr string, info *BackendInfo) {
	mbf.Lock()
	defer mbf.Unlock()
	mbf.backends[addr] = info
}

func (mbf *mockBackendFetcher) removeBackend(addr string) {
	mbf.Lock()
	defer mbf.Unlock()
	delete(mbf.backends, addr)
}

// ExternalFetcher fetches backend list from a given callback.
type ExternalFetcher struct {
	backendGetter func() ([]string, error)
}

func NewExternalFetcher(backendGetter func() ([]string, error)) *ExternalFetcher {
	return &ExternalFetcher{
		backendGetter: backendGetter,
	}
}

func (ef *ExternalFetcher) GetBackendList(context.Context, bool) (map[string]*BackendInfo, error) {
	addrs, err := ef.backendGetter()
	return backendListToMap(addrs), err
}

type mockHealthCheck struct {
	sync.Mutex
	backends map[string]*BackendHealth
}

func newMockHealthCheck() *mockHealthCheck {
	return &mockHealthCheck{
		backends: make(map[string]*BackendHealth),
	}
}

func (mhc *mockHealthCheck) Check(_ context.Context, addr string, _ *BackendInfo) *BackendHealth {
	mhc.Lock()
	defer mhc.Unlock()
	return mhc.backends[addr]
}

func (mhc *mockHealthCheck) setBackend(addr string, health *BackendHealth) {
	mhc.Lock()
	defer mhc.Unlock()
	mhc.backends[addr] = health
}

func (mhc *mockHealthCheck) removeBackend(addr string) {
	mhc.Lock()
	defer mhc.Unlock()
	delete(mhc.backends, addr)
}
