// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"path"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBase(t *testing.T) {
	cfgmgr, _, ctx := testConfigManager(t, "", "")

	nsNum := 10
	valNum := 30
	getNs := func(i int) string {
		return fmt.Sprintf("ns-%d", i)
	}
	getKey := func(i int) string {
		return fmt.Sprintf("%02d", i)
	}

	// test .set
	for i := 0; i < nsNum; i++ {
		ns := getNs(i)
		for j := 0; j < valNum; j++ {
			k := getKey(j)
			require.NoError(t, cfgmgr.set(ctx, ns, k, []byte(k)))
		}
	}

	// test .get
	for i := 0; i < nsNum; i++ {
		ns := getNs(i)
		for j := 0; j < valNum; j++ {
			k := getKey(j)
			v, err := cfgmgr.get(ctx, ns, k)
			require.NoError(t, err)
			require.Equal(t, string(v.Key), path.Join(ns, k))
			require.Equal(t, string(v.Value), k)
		}
	}

	// test .list
	for i := 0; i < nsNum; i++ {
		ns := getNs(i)
		vals, err := cfgmgr.list(ctx, ns, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		require.NoError(t, err)
		require.Len(t, vals, valNum)
		for j := 0; j < valNum; j++ {
			k := getKey(j)
			require.Equal(t, string(vals[j].Value), k)
		}
	}

	// test .del
	for i := 0; i < nsNum; i++ {
		ns := getNs(i)
		for j := 0; j < valNum; j++ {
			k := getKey(j)
			require.NoError(t, cfgmgr.set(ctx, ns, k, nil))

			require.NoError(t, cfgmgr.del(ctx, ns, k))
		}
		vals, err := cfgmgr.list(ctx, ns)
		require.NoError(t, err)
		require.Len(t, vals, 0)
	}
}

func TestBaseConcurrency(t *testing.T) {
	cfgmgr, _, ctx := testConfigManager(t, "", "")

	var wg waitgroup.WaitGroup
	batchNum := 16
	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)
		wg.Run(func() {
			require.NoError(t, cfgmgr.set(ctx, k, "1", []byte("1")))
		})

		wg.Run(func() {
			err := cfgmgr.del(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		require.NoError(t, cfgmgr.set(ctx, k, "1", []byte("1")))
	}

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		wg.Run(func() {
			require.NoError(t, cfgmgr.set(ctx, k, "1", []byte("1")))
		})

		wg.Run(func() {
			_, err := cfgmgr.get(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()
}
