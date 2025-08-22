// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/policy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mockRedirectableConn struct {
	sync.Mutex
	t        *testing.T
	kv       map[any]any
	connID   uint64
	from     BackendInst
	to       BackendInst
	receiver ConnEventReceiver
	closing  bool
}

func newMockRedirectableConn(t *testing.T, id uint64) *mockRedirectableConn {
	return &mockRedirectableConn{
		t:      t,
		connID: id,
		kv:     make(map[any]any),
	}
}

func (conn *mockRedirectableConn) SetEventReceiver(receiver ConnEventReceiver) {
	conn.Lock()
	conn.receiver = receiver
	conn.Unlock()
}

func (conn *mockRedirectableConn) SetValue(k, v any) {
	conn.Lock()
	conn.kv[k] = v
	conn.Unlock()
}

func (conn *mockRedirectableConn) Value(k any) any {
	conn.Lock()
	v := conn.kv[k]
	conn.Unlock()
	return v
}

func (conn *mockRedirectableConn) Redirect(inst BackendInst) bool {
	conn.Lock()
	defer conn.Unlock()
	if conn.closing {
		return false
	}
	require.Nil(conn.t, conn.to)
	require.True(conn.t, inst.Healthy())
	conn.to = inst
	return true
}

func (conn *mockRedirectableConn) GetRedirectingAddr() string {
	conn.Lock()
	defer conn.Unlock()
	if conn.to == nil {
		return ""
	}
	return conn.to.Addr()
}

func (conn *mockRedirectableConn) ConnectionID() uint64 {
	return conn.connID
}

func (conn *mockRedirectableConn) ConnInfo() []zap.Field {
	return nil
}

func (conn *mockRedirectableConn) getAddr() (string, string) {
	conn.Lock()
	defer conn.Unlock()
	var to string
	if conn.to != nil && !reflect.ValueOf(conn.to).IsNil() {
		to = conn.to.Addr()
	}
	return conn.from.Addr(), to
}

func (conn *mockRedirectableConn) redirectSucceed() {
	conn.Lock()
	require.True(conn.t, conn.to != nil && !reflect.ValueOf(conn.to).IsNil())
	conn.from = conn.to
	conn.to = nil
	conn.Unlock()
}

func (conn *mockRedirectableConn) redirectFail() {
	conn.Lock()
	require.True(conn.t, conn.to != nil && !reflect.ValueOf(conn.to).IsNil())
	conn.to = nil
	conn.Unlock()
}

type mockBackendObserver struct {
	healthLock     sync.Mutex
	healths        map[string]*observer.BackendHealth
	subscriberLock sync.Mutex
	subscribers    map[string]chan observer.HealthResult
}

func newMockBackendObserver() *mockBackendObserver {
	return &mockBackendObserver{
		healths:     make(map[string]*observer.BackendHealth),
		subscribers: make(map[string]chan observer.HealthResult),
	}
}

func (mbo *mockBackendObserver) toggleBackendHealth(addr string) {
	mbo.healthLock.Lock()
	defer mbo.healthLock.Unlock()
	health := mbo.healths[addr]
	health.Healthy = !health.Healthy
}

func (mbo *mockBackendObserver) addBackend(addr string, labels map[string]string) {
	mbo.healthLock.Lock()
	defer mbo.healthLock.Unlock()
	mbo.healths[addr] = &observer.BackendHealth{
		Healthy: true,
		BackendInfo: observer.BackendInfo{
			Labels: labels,
		},
	}
}

func (mbo *mockBackendObserver) Start(ctx context.Context) {
}

func (mbo *mockBackendObserver) Subscribe(name string) <-chan observer.HealthResult {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	subscriber := make(chan observer.HealthResult)
	mbo.subscribers[name] = subscriber
	return subscriber
}

func (mbo *mockBackendObserver) Unsubscribe(name string) {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	if subscriber, ok := mbo.subscribers[name]; ok {
		close(subscriber)
		delete(mbo.subscribers, name)
	}
}

func (mbo *mockBackendObserver) Refresh() {
	mbo.addBackend("0", nil)
}

func (mbo *mockBackendObserver) notify(err error) {
	mbo.healthLock.Lock()
	healths := make(map[string]*observer.BackendHealth, len(mbo.healths))
	for addr, health := range mbo.healths {
		healths[addr] = health
	}
	mbo.healthLock.Unlock()
	mbo.subscriberLock.Lock()
	for _, subscriber := range mbo.subscribers {
		subscriber <- observer.NewHealthResult(healths, err)
	}
	mbo.subscriberLock.Unlock()
}

func (mbo *mockBackendObserver) Close() {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	for _, subscriber := range mbo.subscribers {
		close(subscriber)
	}
}

var _ policy.BalancePolicy = (*mockBalancePolicy)(nil)

type mockBalancePolicy struct {
	cfg               atomic.Pointer[config.Config]
	backendsToBalance func([]policy.BackendCtx) (from policy.BackendCtx, to policy.BackendCtx, balanceCount float64, reason string, logFields []zapcore.Field)
	backendToRoute    func([]policy.BackendCtx) policy.BackendCtx
}

func (m *mockBalancePolicy) Init(cfg *config.Config) {
	m.cfg.Store(cfg)
}

func (m *mockBalancePolicy) BackendToRoute(backends []policy.BackendCtx) policy.BackendCtx {
	if m.backendToRoute != nil {
		return m.backendToRoute(backends)
	}
	return nil
}

func (m *mockBalancePolicy) BackendsToBalance(backends []policy.BackendCtx) (from policy.BackendCtx, to policy.BackendCtx, balanceCount float64, reason string, logFields []zapcore.Field) {
	if m.backendsToBalance != nil {
		return m.backendsToBalance(backends)
	}
	return nil, nil, 0, "", nil
}

func (m *mockBalancePolicy) SetConfig(cfg *config.Config) {
	m.cfg.Store(cfg)
}

func (m *mockBalancePolicy) getConfig() *config.Config {
	return m.cfg.Load()
}

var _ config.ConfigGetter = (*mockConfigGetter)(nil)

type mockConfigGetter struct {
	cfg *config.Config
}

func newMockConfigGetter(cfg *config.Config) *mockConfigGetter {
	return &mockConfigGetter{
		cfg: cfg,
	}
}

func (cfgGetter *mockConfigGetter) GetConfig() *config.Config {
	return cfgGetter.cfg
}
