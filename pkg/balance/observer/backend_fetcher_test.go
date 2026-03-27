// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
)

func TestPDFetcher(t *testing.T) {
	tests := []struct {
		infos map[string]*infosync.TiDBTopologyInfo
		ctx   context.Context
		check func(map[string]*BackendInfo)
	}{
		{
			check: func(m map[string]*BackendInfo) {
				require.Empty(t, m)
			},
		},
		{
			infos: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					Addr:       "1.1.1.1:4000",
					Labels:     map[string]string{"k1": "v1"},
					IP:         "1.1.1.1",
					StatusPort: 10080,
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Len(t, m, 1)
				require.NotNil(t, m["1.1.1.1:4000"])
				require.Equal(t, "1.1.1.1:4000", m["1.1.1.1:4000"].Addr)
				require.Equal(t, "1.1.1.1", m["1.1.1.1:4000"].IP)
				require.Equal(t, uint(10080), m["1.1.1.1:4000"].StatusPort)
				require.Equal(t, map[string]string{"k1": "v1"}, m["1.1.1.1:4000"].Labels)
			},
		},
		{
			infos: map[string]*infosync.TiDBTopologyInfo{
				"1.1.1.1:4000": {
					Addr:       "1.1.1.1:4000",
					IP:         "1.1.1.1",
					StatusPort: 10080,
				},
				"2.2.2.2:4000": {
					Addr:       "2.2.2.2:4000",
					IP:         "2.2.2.2",
					StatusPort: 10080,
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Len(t, m, 2)
				require.NotNil(t, m["1.1.1.1:4000"])
				require.Equal(t, "1.1.1.1:4000", m["1.1.1.1:4000"].Addr)
				require.Equal(t, "1.1.1.1", m["1.1.1.1:4000"].IP)
				require.Equal(t, uint(10080), m["1.1.1.1:4000"].StatusPort)
				require.NotNil(t, m["2.2.2.2:4000"])
				require.Equal(t, "2.2.2.2:4000", m["2.2.2.2:4000"].Addr)
				require.Equal(t, "2.2.2.2", m["2.2.2.2:4000"].IP)
				require.Equal(t, uint(10080), m["2.2.2.2:4000"].StatusPort)
			},
		},
		{
			infos: map[string]*infosync.TiDBTopologyInfo{
				"cluster-a/shared.tidb:4000": {
					Addr:       "shared.tidb:4000",
					IP:         "10.0.0.1",
					StatusPort: 10080,
				},
			},
			check: func(m map[string]*BackendInfo) {
				require.Len(t, m, 1)
				require.NotNil(t, m["cluster-a/shared.tidb:4000"])
				require.Equal(t, "shared.tidb:4000", m["cluster-a/shared.tidb:4000"].Addr)
				require.Equal(t, "10.0.0.1", m["cluster-a/shared.tidb:4000"].IP)
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
	lg, _ := logger.CreateLoggerForTest(t)
	pf := NewPDFetcher(tpFetcher, nil, lg, newHealthCheckConfigForTest())
	for _, test := range tests {
		tpFetcher.infos = test.infos
		tpFetcher.hasClusters = true
		if test.ctx == nil {
			test.ctx = context.Background()
		}
		info, err := pf.GetBackendList(test.ctx)
		test.check(info)
		require.NoError(t, err)
	}
}

func TestPDFetcherFallbackToStaticWithoutBackendClusters(t *testing.T) {
	tpFetcher := newMockTpFetcher(t)
	lg, _ := logger.CreateLoggerForTest(t)
	fetcher := NewPDFetcher(tpFetcher, []string{"127.0.0.1:4000"}, lg, newHealthCheckConfigForTest())

	backends, err := fetcher.GetBackendList(context.Background())
	require.NoError(t, err)
	require.Len(t, backends, 1)
	require.Contains(t, backends, "127.0.0.1:4000")

	tpFetcher.hasClusters = true
	tpFetcher.infos = map[string]*infosync.TiDBTopologyInfo{
		"cluster-a/10.0.0.1:4000": {
			Addr:        "10.0.0.1:4000",
			ClusterName: "cluster-a",
		},
	}
	backends, err = fetcher.GetBackendList(context.Background())
	require.NoError(t, err)
	require.Len(t, backends, 1)
	require.Equal(t, "10.0.0.1:4000", backends["cluster-a/10.0.0.1:4000"].Addr)
	require.Equal(t, "cluster-a", backends["cluster-a/10.0.0.1:4000"].ClusterName)
}
