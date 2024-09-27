// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
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

type mockNamespaceManager struct {
	success atomic.Bool
}

func (m *mockNamespaceManager) Init(_ *zap.Logger, _ []*config.Namespace, _ observer.TopologyFetcher,
	_ metricsreader.PromInfoFetcher, _ *http.Client, _ *mconfig.ConfigManager, _ metricsreader.MetricsReader) error {
	return nil
}

func (m *mockNamespaceManager) GetNamespace(_ string) (*namespace.Namespace, bool) {
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

func (m *mockNamespaceManager) CommitNamespaces(_ []*config.Namespace, _ []bool) error {
	if m.success.Load() {
		return nil
	}
	return errors.New("mock error")
}
