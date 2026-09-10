// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager(t *testing.T) {
	ready := true
	reject := false
	mgr := NewManager(
		func() bool { return ready },
		func() (bool, string) {
			if !reject {
				return false, ""
			}
			return true, "high memory usage"
		},
	)

	// Fully serving.
	ok, reason := mgr.Serving()
	require.True(t, ok)
	require.Empty(t, reason)
	rejectConn, reason := mgr.RejectConns()
	require.False(t, rejectConn)
	require.Empty(t, reason)

	// Init phase: Serving reports not ready, but the proxy still accepts.
	ready = false
	ok, reason = mgr.Serving()
	require.False(t, ok)
	require.Equal(t, "server is not ready", reason)
	rejectConn, reason = mgr.RejectConns()
	require.False(t, rejectConn)
	require.Empty(t, reason)

	// Memory pressure: both consumers see the reject reason.
	ready = true
	reject = true
	ok, reason = mgr.Serving()
	require.False(t, ok)
	require.Equal(t, "high memory usage", reason)
	rejectConn, reason = mgr.RejectConns()
	require.True(t, rejectConn)
	require.Equal(t, "high memory usage", reason)

	// Closing wins over every other signal.
	mgr.PreClose()
	ok, reason = mgr.Serving()
	require.False(t, ok)
	require.Equal(t, "server is closing", reason)
	rejectConn, reason = mgr.RejectConns()
	require.True(t, rejectConn)
	require.Equal(t, "server is closing", reason)
}

func TestManagerNilChecks(t *testing.T) {
	// Nil ready/rejectCheck must not panic and default to serving/accepting.
	mgr := NewManager(nil, nil)
	ok, reason := mgr.Serving()
	require.True(t, ok)
	require.Empty(t, reason)
	rejectConn, reason := mgr.RejectConns()
	require.False(t, rejectConn)
	require.Empty(t, reason)

	mgr.PreClose()
	ok, _ = mgr.Serving()
	require.False(t, ok)
	rejectConn, _ = mgr.RejectConns()
	require.True(t, rejectConn)
}
