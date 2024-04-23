// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimplePolicy(t *testing.T) {
	tests := []struct {
		backends []BackendCtx
		routeIdx int
		fromIdx  int
		toIdx    int
	}{
		{
			backends: []BackendCtx{newMockBackend(false, 0)},
			routeIdx: -1,
			fromIdx:  -1,
			toIdx:    -1,
		},
		{
			backends: []BackendCtx{newMockBackend(true, 0)},
			routeIdx: 0,
			fromIdx:  -1,
			toIdx:    -1,
		},
		{
			backends: []BackendCtx{newMockBackend(true, 100), newMockBackend(false, 0)},
			routeIdx: 0,
			fromIdx:  -1,
			toIdx:    -1,
		},
		{
			backends: []BackendCtx{newMockBackend(false, 100), newMockBackend(false, 0)},
			routeIdx: -1,
			fromIdx:  -1,
			toIdx:    -1,
		},
		{
			backends: []BackendCtx{newMockBackend(true, 100), newMockBackend(true, 0)},
			routeIdx: 1,
			fromIdx:  0,
			toIdx:    1,
		},
		{
			backends: []BackendCtx{newMockBackend(true, 0), newMockBackend(true, 0)},
			routeIdx: 0,
			fromIdx:  -1,
			toIdx:    -1,
		},
		{
			backends: []BackendCtx{newMockBackend(true, 100), newMockBackend(true, 95)},
			routeIdx: 1,
			fromIdx:  -1,
			toIdx:    -1,
		},
	}

	sbp := NewSimpleBalancePolicy()
	for idx, test := range tests {
		var routeBackend, fromBackend, toBackend *mockBackend
		if test.routeIdx >= 0 {
			routeBackend = newMockBackend(test.backends[test.routeIdx].Healthy(), test.backends[test.routeIdx].ConnScore())
		}
		if test.fromIdx >= 0 {
			fromBackend = newMockBackend(test.backends[test.fromIdx].Healthy(), test.backends[test.fromIdx].ConnScore())
		}
		if test.toIdx >= 0 {
			toBackend = newMockBackend(test.backends[test.toIdx].Healthy(), test.backends[test.toIdx].ConnScore())
		}
		backend := sbp.BackendToRoute(test.backends)
		if test.routeIdx >= 0 {
			require.Equal(t, routeBackend.healthy, backend.Healthy(), "test idx: %d", idx)
			require.Equal(t, routeBackend.connScore, backend.ConnScore(), "test idx: %d", idx)
		} else {
			require.Nil(t, backend)
		}
		from, to, _, _ := sbp.BackendsToBalance(test.backends)
		if test.fromIdx >= 0 {
			require.Equal(t, fromBackend.healthy, from.Healthy(), "test idx: %d", idx)
			require.Equal(t, fromBackend.connScore, from.ConnScore(), "test idx: %d", idx)
			require.Equal(t, toBackend.healthy, to.Healthy(), "test idx: %d", idx)
			require.Equal(t, toBackend.connScore, to.ConnScore(), "test idx: %d", idx)
		} else {
			require.Nil(t, from, "test idx: %d", idx)
			require.Nil(t, to, "test idx: %d", idx)
		}
	}
}
