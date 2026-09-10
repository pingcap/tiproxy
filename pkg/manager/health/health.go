// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package health

import "sync/atomic"

// Manager aggregates the signals that decide whether the TiProxy instance is
// serving and whether it should keep accepting new connections. Each public
// method below evaluates its own condition independently, so reordering one
// signal never silently changes another consumer's behavior.
type Manager struct {
	// shuttingDown means the instance is in graceful shutdown. It only affects
	// Healthy (so DebugHealth reports unhealthy and the LB drains); it does NOT
	// reject new connections, since the proxy keeps serving until its listeners
	// are closed.
	shuttingDown atomic.Bool
	ready        func() bool
	rejectCheck  func() (bool, string)
}

// NewManager creates a Manager.
//   - ready reports whether the namespace manager is ready (init phase when false).
//   - rejectCheck reports whether new connections should be rejected because of
//     memory pressure, together with a human-readable reason.
func NewManager(ready func() bool, rejectCheck func() (bool, string)) *Manager {
	return &Manager{ready: ready, rejectCheck: rejectCheck}
}

// PreClose marks the instance as gracefully shutting down. Idempotent; safe to
// call from PreClose paths.
func (m *Manager) PreClose() {
	m.shuttingDown.Store(true)
}

// Healthy reports whether the instance is fully serving. Used by DebugHealth.
// It returns false (with a reason) during init, rejectConns, and graceful
// shutdown.
func (m *Manager) Healthy() (bool, string) {
	if m.shuttingDown.Load() {
		return false, "server is shutting down"
	}
	if m.rejectCheck != nil {
		if reject, reason := m.rejectCheck(); reject {
			return false, reason
		}
	}
	if m.ready != nil && !m.ready() {
		return false, "server is not ready"
	}
	return true, ""
}

// RejectConns reports whether new connections should be rejected and returns a
// reason string. Used by the proxy server. It returns true only on memory
// pressure; graceful shutdown does NOT reject here, because the proxy keeps
// accepting until its listeners are closed.
func (m *Manager) RejectConns() (bool, string) {
	if m.rejectCheck != nil {
		if reject, reason := m.rejectCheck(); reject {
			return true, reason
		}
	}
	return false, ""
}
