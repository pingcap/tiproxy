// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/tiproxy/pkg/manager/observer"
	"github.com/stretchr/testify/require"
)

type mockRedirectableConn struct {
	sync.Mutex
	t        *testing.T
	kv       map[any]any
	connID   uint64
	from     BackendInst
	to       BackendInst
	receiver ConnEventReceiver
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
	require.Nil(conn.t, conn.to)
	require.True(conn.t, inst.Healthy())
	conn.to = inst
	conn.Unlock()
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
	sync.Mutex
	healths    map[string]*observer.BackendHealth
	subscriber chan observer.HealthResult
}

func newMockBackendObserver() *mockBackendObserver {
	return &mockBackendObserver{
		healths: make(map[string]*observer.BackendHealth),
	}
}

func (mbo *mockBackendObserver) toggleBackendHealth(addr string) {
	mbo.Lock()
	defer mbo.Unlock()
	health := mbo.healths[addr]
	if health.Status == observer.StatusHealthy {
		health.Status = observer.StatusCannotConnect
	} else {
		health.Status = observer.StatusHealthy
	}
}

func (mbo *mockBackendObserver) addBackend(addr string) {
	mbo.Lock()
	defer mbo.Unlock()
	mbo.healths[addr] = &observer.BackendHealth{
		Status: observer.StatusHealthy,
	}
}

func (mbo *mockBackendObserver) Start(ctx context.Context) {
}

func (mbo *mockBackendObserver) Subscribe(name string) <-chan observer.HealthResult {
	mbo.Lock()
	defer mbo.Unlock()
	mbo.subscriber = make(chan observer.HealthResult)
	return mbo.subscriber
}

func (mbo *mockBackendObserver) Refresh() {
	mbo.addBackend("0")
}

func (mbo *mockBackendObserver) notify(err error) {
	mbo.Lock()
	defer mbo.Unlock()
	healths := make(map[string]*observer.BackendHealth, len(mbo.healths))
	for addr, health := range mbo.healths {
		healths[addr] = health
	}
	mbo.subscriber <- observer.NewHealthResult(healths, err)
}

func (mbo *mockBackendObserver) Close() {
	mbo.Lock()
	defer mbo.Unlock()
	close(mbo.subscriber)
}
