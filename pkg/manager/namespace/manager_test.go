// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"testing"

	"github.com/pingcap/tiproxy/pkg/balance/router"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReady(t *testing.T) {
	nsMgr := NewNamespaceManager()
	nsMgr.Init(zap.NewNop(), nil, nil, nil, nil, nil, nil)
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
