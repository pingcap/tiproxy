// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import "github.com/pingcap/tiproxy/pkg/balance/observer"

var _ BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	healthy   bool
	connScore int
}

func newMockBackend(healthy bool, connScore int) *mockBackend {
	return &mockBackend{
		healthy:   healthy,
		connScore: connScore,
	}
}

func (mb *mockBackend) Healthy() bool {
	return mb.healthy
}

func (mb *mockBackend) ConnScore() int {
	return mb.connScore
}

func (mb *mockBackend) ConnCount() int {
	return mb.connScore
}

func (mb *mockBackend) Addr() string {
	return ""
}

func (mb *mockBackend) GetBackendInfo() observer.BackendInfo {
	return observer.BackendInfo{}
}
