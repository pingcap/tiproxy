// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package monotime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkGoNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}

func BenchmarkMonoNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Now()
	}
}

func BenchmarkGoSince(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Since(time.Now())
	}
}

func BenchmarkMonoSince(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Since(Now())
	}
}

func TestAfter(t *testing.T) {
	t1 := Now()
	time.Sleep(100 * time.Millisecond)
	d := Since(t1)
	require.GreaterOrEqual(t, d, 100*time.Millisecond)
	require.True(t, Now().After(t1))
	require.True(t, t1.Before(Now()))
	require.Greater(t, t1.Add(time.Millisecond), t1)
	require.Less(t, t1.Sub(time.Millisecond), t1)
}
