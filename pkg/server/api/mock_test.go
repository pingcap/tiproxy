// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	mconfig "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/manager/namespace"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"go.uber.org/zap"
)

var _ namespace.NamespaceManager = (*mockNamespaceManager)(nil)

type namespaceCommit struct {
	names     []string
	delete    []bool
	deleteNil bool
}

type mockNamespaceManager struct {
	success atomic.Bool
	mu      sync.Mutex
	live    map[string]bool
	commits []namespaceCommit
}

func newMockNamespaceManager() *mockNamespaceManager {
	mgr := &mockNamespaceManager{}
	mgr.success.Store(true)
	return mgr
}

func (m *mockNamespaceManager) SetBackendNetwork(_ observer.BackendNetwork) {
}

func (m *mockNamespaceManager) Init(_ *zap.Logger, _ []*config.Namespace, _ observer.TopologyFetcher,
	_ metricsreader.PromInfoFetcher, _ *http.Client, _ *mconfig.ConfigManager, _ metricsreader.MetricsQuerier) error {
	return nil
}

func (m *mockNamespaceManager) GetNamespace(name string) (*namespace.Namespace, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.live[name] {
		return &namespace.Namespace{}, true
	}
	return nil, false
}

func (m *mockNamespaceManager) GetNamespaceByUser(_ string) (*namespace.Namespace, bool) {
	return nil, false
}

func (m *mockNamespaceManager) SetNamespace(_ context.Context, _ string, _ *config.Namespace) error {
	if m.success.Load() {
		return nil
	}
	return errors.New("mock error")
}

func (m *mockNamespaceManager) GetConfigChecksum() string {
	return ""
}

func (m *mockNamespaceManager) Ready() bool {
	return m.success.Load()
}

func (m *mockNamespaceManager) RedirectConnections() []error {
	if m.success.Load() {
		return nil
	}
	return []error{errors.New("mock error")}
}

func (m *mockNamespaceManager) Close() error {
	return nil
}

func (m *mockNamespaceManager) CommitNamespaces(nss []*config.Namespace, nssDelete []bool) error {
	call := namespaceCommit{
		names:     make([]string, len(nss)),
		deleteNil: nssDelete == nil,
	}
	for i, ns := range nss {
		if ns != nil {
			call.names[i] = ns.Namespace
		}
	}
	if nssDelete != nil {
		call.delete = append([]bool(nil), nssDelete...)
	}
	m.mu.Lock()
	m.commits = append(m.commits, call)
	m.mu.Unlock()
	if m.success.Load() {
		return nil
	}
	return errors.New("mock error")
}

func (m *mockNamespaceManager) lastCommit() namespaceCommit {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.commits) == 0 {
		return namespaceCommit{}
	}
	return m.commits[len(m.commits)-1]
}
