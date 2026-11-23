// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockTopologyFetcher struct{}

func (*mockTopologyFetcher) GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	return nil, nil
}

func (*mockTopologyFetcher) HasBackendClusters() bool {
	return false
}

func TestReady(t *testing.T) {
	nsMgr := NewNamespaceManager()
	require.NoError(t, nsMgr.Init(zap.NewNop(), nil, &mockTopologyFetcher{}, nil, nil, nil, nil))
	require.False(t, nsMgr.Ready())

	rt := router.NewStaticRouter([]string{})
	nsMgr.nsm = map[string]*Namespace{
		"test": {
			router: rt,
		},
	}
	require.False(t, nsMgr.Ready())

	rt = router.NewStaticRouter([]string{"127.0.0.1:4000"})
	ns, ok := nsMgr.GetNamespace("test")
	require.True(t, ok)
	ns.router = rt
	require.True(t, nsMgr.Ready())
}

type trackingRouter struct {
	*router.StaticRouter
	closed *atomic.Bool
}

func (r *trackingRouter) Close() {
	if r.closed != nil {
		r.closed.Store(true)
	}
}

func newTestNamespace(name, user string, closed *atomic.Bool) *Namespace {
	return &Namespace{
		name:   name,
		user:   user,
		router: &trackingRouter{closed: closed},
	}
}

func TestCommitNamespacesFullSnapshotDropsRemovedNamespace(t *testing.T) {
	stayClosed := &atomic.Bool{}
	dropClosed := &atomic.Bool{}
	builtClosed := &atomic.Bool{}
	stay := newTestNamespace("stay", "stay-user", stayClosed)
	drop := newTestNamespace("drop", "drop-user", dropClosed)
	built := newTestNamespace("stay", "stay-user", builtClosed)

	nsMgr := NewNamespaceManager()
	nsMgr.logger = zap.NewNop()
	nsMgr.nsm = map[string]*Namespace{
		"stay": stay,
		"drop": drop,
	}
	nsMgr.newNamespace = func(cfg *config.Namespace) (*Namespace, error) {
		require.Equal(t, "stay", cfg.Namespace)
		return built, nil
	}

	require.NoError(t, nsMgr.CommitNamespaces([]*config.Namespace{{
		Namespace: "stay",
		Frontend:  config.FrontendNamespace{User: "stay-user"},
	}}, nil))

	_, ok := nsMgr.GetNamespace("drop")
	require.False(t, ok)
	_, ok = nsMgr.GetNamespaceByUser("drop-user")
	require.False(t, ok)
	got, ok := nsMgr.GetNamespaceByUser("stay-user")
	require.True(t, ok)
	require.Same(t, built, got)
	require.True(t, dropClosed.Load())
	require.True(t, stayClosed.Load())
	require.False(t, builtClosed.Load())
}

func TestCommitNamespacesDeleteFlagRemovesOnlyThatNamespace(t *testing.T) {
	keepClosed := &atomic.Bool{}
	dropClosed := &atomic.Bool{}
	keep := newTestNamespace("keep", "keep-user", keepClosed)
	drop := newTestNamespace("drop", "drop-user", dropClosed)

	nsMgr := NewNamespaceManager()
	nsMgr.logger = zap.NewNop()
	nsMgr.nsm = map[string]*Namespace{
		"keep": keep,
		"drop": drop,
	}

	require.NoError(t, nsMgr.CommitNamespaces([]*config.Namespace{{Namespace: "drop"}}, []bool{true}))

	_, ok := nsMgr.GetNamespace("drop")
	require.False(t, ok)
	_, ok = nsMgr.GetNamespaceByUser("drop-user")
	require.False(t, ok)
	got, ok := nsMgr.GetNamespace("keep")
	require.True(t, ok)
	require.Same(t, keep, got)
	require.True(t, dropClosed.Load())
	require.False(t, keepClosed.Load())
}

func TestCommitNamespacesPartialCommitKeepsUnmentionedNamespaces(t *testing.T) {
	closed := &atomic.Bool{}
	keep := newTestNamespace("keep", "keep-user", closed)
	nsMgr := NewNamespaceManager()
	nsMgr.nsm = map[string]*Namespace{"keep": keep}

	require.NoError(t, nsMgr.CommitNamespaces(nil, []bool{}))

	got, ok := nsMgr.GetNamespaceByUser("keep-user")
	require.True(t, ok)
	require.Same(t, keep, got)
	require.False(t, closed.Load())
}

func TestCommitNamespacesRejectsMismatchedDeleteFlags(t *testing.T) {
	closed := &atomic.Bool{}
	keep := newTestNamespace("keep", "keep-user", closed)
	nsMgr := NewNamespaceManager()
	nsMgr.nsm = map[string]*Namespace{"keep": keep}

	err := nsMgr.CommitNamespaces([]*config.Namespace{{Namespace: "keep"}}, []bool{})
	require.Error(t, err)
	_, ok := nsMgr.GetNamespace("keep")
	require.True(t, ok)
	require.False(t, closed.Load())
}

func TestCommitNamespacesBuildErrorLeavesRoutingTable(t *testing.T) {
	keepClosed := &atomic.Bool{}
	partialClosed := &atomic.Bool{}
	keep := newTestNamespace("keep", "keep-user", keepClosed)
	partial := newTestNamespace("new", "new-user", partialClosed)
	nsMgr := NewNamespaceManager()
	nsMgr.logger = zap.NewNop()
	nsMgr.nsm = map[string]*Namespace{"keep": keep}
	calls := 0
	nsMgr.newNamespace = func(cfg *config.Namespace) (*Namespace, error) {
		calls++
		if cfg.Namespace == "new" {
			return partial, nil
		}
		return nil, errors.New("boom")
	}

	err := nsMgr.CommitNamespaces([]*config.Namespace{
		{Namespace: "new"},
		{Namespace: "broken"},
	}, nil)
	require.Error(t, err)
	require.Equal(t, 2, calls)
	_, ok := nsMgr.GetNamespace("new")
	require.False(t, ok)
	got, ok := nsMgr.GetNamespace("keep")
	require.True(t, ok)
	require.Same(t, keep, got)
	require.True(t, partialClosed.Load())
	require.False(t, keepClosed.Load())
}
