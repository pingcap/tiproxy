// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package health

import "sync/atomic"

// Manager aggregates the signals that decide whether the TiProxy instance is
// serving and whether it should keep accepting new connections. Each public
// method below evaluates its own condition independently, so reordering one
// signal never silently changes another consumer's behavior.
type Manager struct {
	closing     atomic.Bool
	ready       func() bool
	rejectCheck func() (bool, string)
}

// NewManager creates a Manager.
//   - ready reports whether the namespace manager is ready (init phase when false).
//   - rejectCheck reports whether new connections should be rejected because of
//     memory pressure, together with a human-readable reason.
func NewManager(ready func() bool, rejectCheck func() (bool, string)) *Manager {
	return &Manager{ready: ready, rejectCheck: rejectCheck}
}

// PreClose marks the server as shutting down. Idempotent; safe to call from
// PreClose paths.
func (m *Manager) PreClose() {
	m.closing.Store(true)
}

// Serving reports whether the instance is fully serving. Used by DebugHealth.
// It returns false (with a reason) during init, rejectConns, and closing.
func (m *Manager) Serving() (bool, string) {
	if m.closing.Load() {
		return false, "server is closing"
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
// reason string. Used by the proxy server. It returns true during rejectConns
// and closing; the init phase does NOT reject because the proxy already starts
// listening before the namespace manager becomes ready.
func (m *Manager) RejectConns() (bool, string) {
	if m.closing.Load() {
		return true, "server is closing"
	}
	if m.rejectCheck != nil {
		if reject, reason := m.rejectCheck(); reject {
			return true, reason
		}
	}
	return false, ""
}
