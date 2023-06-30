// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"testing"

	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/pkg/manager/infosync"
	tidbinfo "github.com/pingcap/tidb/domain/infosync"
	"github.com/stretchr/testify/require"
)

func TestPDFetcher(t *testing.T) {
	tests := []struct {
		infos map[string]*infosync.TiDBInfo
		ctx   context.Context
		check func(map[string]*BackendInfo)
	}{
		{
			check: func(m map[string]*BackendInfo) {
				require.Empty(t, m)
			},
		},
		{
			infos: map[string]*infosync.TiDBInfo{
				"1.1.1.1:4000": {
					TTL: "123456789",
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Empty(t, m)
			},
		},
		{
			infos: map[string]*infosync.TiDBInfo{
				"1.1.1.1:4000": {
					TopologyInfo: &tidbinfo.TopologyInfo{
						IP:         "1.1.1.1",
						StatusPort: 10080,
					},
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Empty(t, m)
			},
		},
		{
			infos: map[string]*infosync.TiDBInfo{
				"1.1.1.1:4000": {
					TTL: "123456789",
					TopologyInfo: &tidbinfo.TopologyInfo{
						IP:         "1.1.1.1",
						StatusPort: 10080,
					},
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Len(t, m, 1)
				require.NotNil(t, m["1.1.1.1:4000"])
				require.Equal(t, "1.1.1.1", m["1.1.1.1:4000"].IP)
				require.Equal(t, uint(10080), m["1.1.1.1:4000"].StatusPort)
			},
		},
		{
			infos: map[string]*infosync.TiDBInfo{
				"1.1.1.1:4000": {
					TTL: "123456789",
					TopologyInfo: &tidbinfo.TopologyInfo{
						IP:         "1.1.1.1",
						StatusPort: 10080,
					},
				},
				"2.2.2.2:4000": {
					TTL: "123456789",
					TopologyInfo: &tidbinfo.TopologyInfo{
						IP:         "2.2.2.2",
						StatusPort: 10080,
					},
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Len(t, m, 2)
				require.NotNil(t, m["1.1.1.1:4000"])
				require.Equal(t, "1.1.1.1", m["1.1.1.1:4000"].IP)
				require.Equal(t, uint(10080), m["1.1.1.1:4000"].StatusPort)
				require.NotNil(t, m["2.2.2.2:4000"])
				require.Equal(t, "2.2.2.2", m["2.2.2.2:4000"].IP)
				require.Equal(t, uint(10080), m["2.2.2.2:4000"].StatusPort)
			},
		},
		{
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			check: func(m map[string]*BackendInfo) {
				require.Empty(t, m)
			},
		},
	}

	tpFetcher := newMockTpFetcher(t)
	pf := NewPDFetcher(tpFetcher, logger.CreateLoggerForTest(t), newHealthCheckConfigForTest())
	for _, test := range tests {
		tpFetcher.infos = test.infos
		if test.ctx == nil {
			test.ctx = context.Background()
		}
		info, err := pf.GetBackendList(test.ctx)
		test.check(info)
		require.NoError(t, err)
	}
}

type mockTpFetcher struct {
	t     *testing.T
	infos map[string]*infosync.TiDBInfo
	err   error
}

func newMockTpFetcher(t *testing.T) *mockTpFetcher {
	return &mockTpFetcher{
		t: t,
	}
}

func (ft *mockTpFetcher) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBInfo, error) {
	return ft.infos, ft.err
}
