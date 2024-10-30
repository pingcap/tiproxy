// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
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
	vm.delOnRetire.Store(true)
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
		return strings.Count(text.String(), "adding VIP success") >= 2
	}, 3*time.Second, 10*time.Millisecond)
	vm1.PreClose()
	vm2.PreClose()
	vm1.Close()
	vm2.Close()
}
