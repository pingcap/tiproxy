// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
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
		cfgGetter := newMockConfigGetter(test.cfg)
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
		cfgGetter: newMockConfigGetter(config.HA{VirtualIP: "10.10.10.10/24", Interface: "eth0"}),
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
	vm.election.Close()
}
