// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/stretchr/testify/require"
)

func TestVIPCfgError(t *testing.T) {
	tests := []struct {
		cfg    config.HA
		hasErr bool
	}{
		{
			cfg:    config.HA{},
			hasErr: false,
		},
		{
			cfg: config.HA{
				VirtualIP: "",
				Interface: "eth0",
			},
			hasErr: false,
		},
		{
			cfg: config.HA{
				VirtualIP: "10.10.10.10/24",
				Interface: "",
			},
			hasErr: false,
		},
		{
			cfg: config.HA{
				VirtualIP: "0.0.0.0/24",
				Interface: "unknown",
			},
			// OS error for non-linux platform and parse error for linux platform.
			hasErr: true,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		cfgGetter := newMockConfigGetter(&config.Config{HA: test.cfg})
		vm, err := NewVIPManager(lg, cfgGetter)
		if test.hasErr {
			require.Error(t, err, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
		}
		require.Nil(t, vm)
	}
}

func TestNetworkOperation(t *testing.T) {
	tests := []struct {
		eventType   int
		hasIP       bool
		hasIPErr    bool
		addIPErr    bool
		delIPErr    bool
		sendArpErr  bool
		expectedLog string
	}{
		{
			eventType:   eventTypeElected,
			hasIP:       false,
			expectedLog: "adding VIP success",
		},
		{
			eventType:   eventTypeRetired,
			hasIP:       true,
			expectedLog: "deleting VIP success",
		},
		{
			eventType:   eventTypeElected,
			hasIP:       true,
			expectedLog: "do nothing",
		},
		{
			eventType:   eventTypeRetired,
			hasIP:       false,
			expectedLog: "do nothing",
		},
		{
			eventType:   eventTypeElected,
			hasIPErr:    true,
			expectedLog: "checking addresses failed",
		},
		{
			eventType:   eventTypeElected,
			hasIP:       false,
			addIPErr:    true,
			expectedLog: "adding address failed",
		},
		{
			eventType:   eventTypeElected,
			hasIP:       false,
			sendArpErr:  true,
			expectedLog: "broadcast ARP failed",
		},
		{
			eventType:   eventTypeRetired,
			hasIP:       true,
			delIPErr:    true,
			expectedLog: "deleting address failed",
		},
	}

	lg, text := logger.CreateLoggerForTest(t)
	ch := make(chan int)
	operation := newMockNetworkOperation()
	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(newMockConfig()),
		operation: operation,
	}
	vm.election = newMockElection(ch, vm)
	childCtx, cancel := context.WithCancel(context.Background())
	vm.election.Start(childCtx)
	logIdx := 0
	for i, test := range tests {
		operation.hasIP.Store(test.hasIP)
		operation.hasIPErr.Store(test.hasIPErr)
		operation.addIPErr.Store(test.addIPErr)
		operation.delIPErr.Store(test.delIPErr)
		operation.sendArpErr.Store(test.sendArpErr)
		ch <- test.eventType
		require.Eventually(t, func() bool {
			return strings.Contains(text.String()[logIdx:], test.expectedLog)
		}, 3*time.Second, 10*time.Millisecond, "case %d", i)
		logIdx = len(text.String())
	}
	cancel()
	vm.Close()
}

func TestPreCloseDeletesVIPBeforeCloseElection(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	operation := newMockNetworkOperation()
	operation.hasIP.Store(true)

	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(newMockConfig()),
		operation: operation,
	}
	vm.election = &closeHookElection{
		closeFn: func() {
			require.False(t, operation.hasIP.Load())
			require.EqualValues(t, 1, operation.delIPCnt.Load())
		},
	}

	vm.PreClose()
}

func TestPreClosePreventsReacquireDuringElectionClose(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	operation := newMockNetworkOperation()
	operation.hasIP.Store(true)

	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(newMockConfig()),
		operation: operation,
	}
	vm.election = &closeHookElection{
		closeFn: func() {
			vm.OnElected()
		},
	}

	vm.PreClose()
	require.False(t, operation.hasIP.Load())
	require.EqualValues(t, 0, operation.addIPCnt.Load())
}

func TestGARPRefresh(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newMockConfig()
	cfg.HA.GARPBurstCount = 1
	cfg.HA.GARPRefreshCount = 1
	operation := newMockNetworkOperation()
	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(cfg),
		operation: operation,
	}

	vm.OnElected()
	require.Eventually(t, func() bool {
		return operation.sendArpCnt.Load() > 0
	}, 2*garpRefreshInterval, 10*time.Millisecond)

	vm.OnRetired()
	sendArpCnt := operation.sendArpCnt.Load()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, sendArpCnt, operation.sendArpCnt.Load())
	require.False(t, operation.hasIP.Load())
}

func TestGARPRefreshNotStartedWhenVIPNotBound(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newMockConfig()
	cfg.HA.GARPBurstCount = 1
	cfg.HA.GARPRefreshCount = 1
	operation := newMockNetworkOperation()
	operation.addIPErr.Store(true)
	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(cfg),
		operation: operation,
	}

	vm.OnElected()
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 0, operation.sendArpCnt.Load())
	require.False(t, operation.hasIP.Load())
}

func TestPreCloseCancelsInFlightARP(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newMockConfig()
	cfg.HA.GARPBurstCount = 2
	cfg.HA.GARPRefreshCount = 1
	operation := newMockNetworkOperation()
	operation.sendArpDelay.Store(int64(200 * time.Millisecond))
	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(cfg),
		operation: operation,
	}

	done := make(chan struct{})
	go func() {
		vm.OnElected()
		close(done)
	}()

	require.Eventually(t, func() bool {
		return operation.addIPCnt.Load() == 1
	}, time.Second, 10*time.Millisecond)

	start := time.Now()
	vm.PreClose()
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.False(t, operation.hasIP.Load())
}

func TestStartAndClose(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	vm, err := NewVIPManager(lg, newMockConfigGetter(newMockConfig()))
	if runtime.GOOS != "linux" {
		require.Error(t, err)
	} else {
		// Maybe interface doesn't exist or lack of permission.
		if err != nil {
			return
		}
		server, err := etcd.CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
		require.NoError(t, err)
		endpoint := server.Clients[0].Addr().String()
		cfg := etcd.ConfigForEtcdTest(endpoint)

		certMgr := cert.NewCertManager()
		err = certMgr.Init(cfg, lg, nil)
		require.NoError(t, err)
		client, err := etcd.InitEtcdClient(lg, cfg, certMgr)
		require.NoError(t, err)

		err = vm.Start(context.Background(), client)
		require.NoError(t, err)
		vm.PreClose()
		vm.Close()
	}
}

func TestRejecterWatcherResignsAndRecompetes(t *testing.T) {
	// Shorten the polling interval so transitions happen within the test window.
	origInterval := servingCheckInterval
	servingCheckInterval = 10 * time.Millisecond
	defer func() { servingCheckInterval = origInterval }()

	lg, _ := logger.CreateLoggerForTest(t)
	operation := newMockNetworkOperation()
	operation.hasIP.Store(false)
	rejecter := &mockConnRejecter{}
	rejecter.reject.Store(false)

	vm := &vipManager{
		lg:        lg,
		cfgGetter: newMockConfigGetter(newMockConfig()),
		operation: operation,
		rejecter:  rejecter,
	}
	vm.newElection = func() elect.Election {
		e := &autoElection{member: vm}
		e.Start(vm.startCtx)
		return e
	}
	vm.startCtx, vm.startCancel = context.WithCancel(context.Background())
	// Start competing: the auto election wins immediately and binds the VIP.
	vm.mu.Lock()
	vm.election = vm.newElection()
	vm.mu.Unlock()
	require.Eventually(t, func() bool { return operation.hasIP.Load() }, time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, operation.addIPCnt.Load())

	// Start the rejecter watcher, driven by vm.startCtx so that PreClose's
	// cancellation of startCtx (and watchWG wait) is the shutdown path under test.
	vm.watchWG.RunWithRecover(func() { vm.watchRejecter(vm.startCtx) }, nil, vm.lg)

	// Flip to rejecting: the watcher resigns, which retires and drops the VIP.
	rejecter.reject.Store(true)
	require.Eventually(t, func() bool { return !operation.hasIP.Load() }, 3*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, operation.delIPCnt.Load())

	// Flip back to not rejecting: the watcher recompetes, wins, and rebinds the VIP.
	rejecter.reject.Store(false)
	require.Eventually(t, func() bool { return operation.hasIP.Load() }, 3*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 2, operation.addIPCnt.Load())

	// Shutdown: closing must release the VIP and stop the watcher without deadlock.
	// PreClose cancels vm.startCtx (stopping the watcher) and waits for watchWG.
	vm.PreClose()
	vm.Close()
	require.False(t, operation.hasIP.Load())
}

func TestMultiVIP(t *testing.T) {
	if runtime.GOOS != "linux" {
		return
	}
	config1, config2 := newMockConfig(), newMockConfig()
	config1.HA.VirtualIP = "127.0.0.2/24"
	config2.HA.VirtualIP = "127.0.0.3/24"
	lg, text := logger.CreateLoggerForTest(t)
	vm1, err := NewVIPManager(lg, newMockConfigGetter(config1))
	// Maybe interface doesn't exist or lack of permission.
	if err != nil {
		return
	}
	vm2, err := NewVIPManager(lg, newMockConfigGetter(config2))
	require.NoError(t, err)

	server, err := etcd.CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	endpoint := server.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)

	certMgr := cert.NewCertManager()
	err = certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	client, err := etcd.InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)

	err = vm1.Start(context.Background(), client)
	require.NoError(t, err)
	err = vm2.Start(context.Background(), client)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return strings.Count(text.String(), "adding VIP success") >= 2 ||
			strings.Count(text.String(), "ip: command not found") >= 2 ||
			strings.Count(text.String(), "executable file not found") >= 2
	}, 3*time.Second, 10*time.Millisecond)
	vm1.PreClose()
	vm2.PreClose()
	vm1.Close()
	vm2.Close()
}

type closeHookElection struct {
	closeFn func()
}

func (e *closeHookElection) Start(context.Context) {}

func (e *closeHookElection) ID() string {
	return ""
}

func (e *closeHookElection) GetOwnerID(context.Context) (string, error) {
	return "", nil
}

func (e *closeHookElection) Close() {
	if e.closeFn != nil {
		e.closeFn()
	}
}

var _ elect.Election = (*autoElection)(nil)

// autoElection calls OnElected on Start (simulating an immediate win) and
// OnRetired on Close (simulating losing ownership). OnElected is delivered
// asynchronously, like the real election, so callers that start the election
// while holding the member's lock do not deadlock.
type autoElection struct {
	member  elect.Member
	started atomic.Bool
	closed  atomic.Bool
	wg      sync.WaitGroup
}

func (e *autoElection) Start(context.Context) {
	if e.started.Swap(true) {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.member.OnElected()
	}()
}

func (e *autoElection) ID() string {
	return ""
}

func (e *autoElection) GetOwnerID(context.Context) (string, error) {
	return "", nil
}

func (e *autoElection) Close() {
	if !e.started.Load() || e.closed.Swap(true) {
		return
	}
	e.wg.Wait()
	e.member.OnRetired()
}
