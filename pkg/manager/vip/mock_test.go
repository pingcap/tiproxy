// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
)

var _ config.ConfigGetter = (*mockConfigGetter)(nil)

type mockConfigGetter struct {
	cfg *config.Config
}

func newMockConfigGetter(cfg config.HA) *mockConfigGetter {
	return &mockConfigGetter{
		cfg: &config.Config{HA: cfg},
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
	wg     waitgroup.WaitGroup
	ch     chan int
	member elect.Member
}

func newMockElection(ch chan int, member elect.Member) *mockElection {
	return &mockElection{
		ch:     ch,
		member: member,
	}
}

func (m *mockElection) ID() string {
	return ""
}

func (me *mockElection) GetOwnerID(ctx context.Context) (string, error) {
	return "", nil
}

func (me *mockElection) IsOwner() bool {
	return true
}

func (me *mockElection) Start(ctx context.Context) {
	me.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
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
	me.wg.Wait()
}

var _ NetworkOperation = (*mockNetworkOperation)(nil)

type mockNetworkOperation struct {
	hasIP      atomic.Bool
	hasIPErr   atomic.Bool
	addIPErr   atomic.Bool
	delIPErr   atomic.Bool
	sendArpErr atomic.Bool
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
	return nil
}

func (mno *mockNetworkOperation) DeleteIP() error {
	if mno.delIPErr.Load() {
		return errors.New("mock DeleteIP error")
	}
	return nil
}

func (mno *mockNetworkOperation) SendARP() error {
	if mno.sendArpErr.Load() {
		return errors.New("mock SendARP error")
	}
	return nil
}
