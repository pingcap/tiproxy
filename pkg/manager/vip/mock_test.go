// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
)

var _ config.ConfigGetter = (*mockConfigGetter)(nil)

type mockConfigGetter struct {
	cfg *config.Config
}

func newMockConfigGetter(cfg *config.Config) *mockConfigGetter {
	return &mockConfigGetter{
		cfg: cfg,
	}
}

func (mcg *mockConfigGetter) GetConfig() *config.Config {
	return mcg.cfg
}

var _ elect.Election = (*mockElection)(nil)

const (
	eventTypeElected = iota
	eventTypeRetired
)

type mockElection struct {
	wg        waitgroup.WaitGroup
	ch        chan int
	member    elect.Member
	closeOnce sync.Once
	closeCh   chan struct{}
}

func newMockElection(ch chan int, member elect.Member) *mockElection {
	return &mockElection{
		ch:      ch,
		member:  member,
		closeCh: make(chan struct{}),
	}
}

func (m *mockElection) ID() string {
	return ""
}

func (me *mockElection) GetOwnerID(ctx context.Context) (string, error) {
	return "", nil
}

func (me *mockElection) Start(ctx context.Context) {
	me.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-me.closeCh:
				return
			case event := <-me.ch:
				switch event {
				case eventTypeElected:
					me.member.OnElected()
				case eventTypeRetired:
					me.member.OnRetired()
				}
			}
		}
	})
}

func (me *mockElection) Close() {
	me.closeOnce.Do(func() {
		close(me.closeCh)
	})
	me.wg.Wait()
}

var _ NetworkOperation = (*mockNetworkOperation)(nil)

type mockNetworkOperation struct {
	hasIP        atomic.Bool
	hasIPErr     atomic.Bool
	addIPErr     atomic.Bool
	delIPErr     atomic.Bool
	sendArpErr   atomic.Bool
	sendArpCnt   atomic.Int32
	addIPCnt     atomic.Int32
	delIPCnt     atomic.Int32
	sendArpDelay atomic.Int64
}

func newMockNetworkOperation() *mockNetworkOperation {
	return &mockNetworkOperation{}
}

func (mno *mockNetworkOperation) HasIP() (bool, error) {
	hasIP := mno.hasIP.Load()
	if mno.hasIPErr.Load() {
		return hasIP, errors.New("mock HasIP error")
	}
	return hasIP, nil
}

func (mno *mockNetworkOperation) AddIP() error {
	if mno.addIPErr.Load() {
		return errors.New("mock AddIP error")
	}
	mno.addIPCnt.Add(1)
	mno.hasIP.Store(true)
	return nil
}

func (mno *mockNetworkOperation) DeleteIP() error {
	if mno.delIPErr.Load() {
		return errors.New("mock DeleteIP error")
	}
	mno.delIPCnt.Add(1)
	mno.hasIP.Store(false)
	return nil
}

func (mno *mockNetworkOperation) SendARP() error {
	if mno.sendArpErr.Load() {
		return errors.New("mock SendARP error")
	}
	delay := time.Duration(mno.sendArpDelay.Load())
	if delay > 0 {
		time.Sleep(delay)
	}
	mno.sendArpCnt.Add(1)
	return nil
}

func (mno *mockNetworkOperation) Addr() string {
	return ""
}

func newMockConfig() *config.Config {
	return &config.Config{
		Proxy: config.ProxyServer{Addr: "0.0.0.0:6000"},
		API:   config.API{Addr: "0.0.0.0:3080"},
		HA: config.HA{
			VirtualIP:      "127.0.0.2/24",
			Interface:      "lo",
			GARPBurstCount: 5,
		},
	}
}
