// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package id

import (
	"sync"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func TestIDValue(t *testing.T) {
	var lock sync.Mutex
	var wg waitgroup.WaitGroup
	mgr := NewIDManager()
	m := make(map[uint64]struct{}, 1000)
	for i := 0; i < 10; i++ {
		wg.Run(func() {
			for j := 0; j < 100; j++ {
				id := mgr.NewID()
				lock.Lock()
				m[id] = struct{}{}
				lock.Unlock()
			}
		})
	}
	wg.Wait()

	require.Equal(t, 1000, len(m))
	for i := 1; i <= 1000; i++ {
		require.Contains(t, m, uint64(i), "missing id: %d", i)
	}
}
