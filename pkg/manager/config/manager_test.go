// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func testConfigManager(t *testing.T, cfg *config.Config) (*ConfigManager, context.Context) {
	addr, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)

	testDir := t.TempDir()

	logger := logger.CreateLoggerForTest(t)

	etcd_cfg := embed.NewConfig()
	etcd_cfg.LCUrls = []url.URL{*addr}
	etcd_cfg.LPUrls = []url.URL{*addr}
	etcd_cfg.Dir = filepath.Join(testDir, "etcd")
	etcd_cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger.Named("etcd"))
	etcd, err := embed.StartEtcd(etcd_cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	if ddl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, ddl)
	}

	cfgmgr := NewConfigManager()
	require.NoError(t, cfgmgr.Init(ctx, etcd.Server.KV(), cfg, logger))

	t.Cleanup(func() {
		require.NoError(t, cfgmgr.Close())
		etcd.Close()
	})

	t.Cleanup(cancel)

	return cfgmgr, ctx
}

func TestBase(t *testing.T) {
	cfgmgr, ctx := testConfigManager(t, &config.Config{})

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
			require.NoError(t, cfgmgr.set(ctx, ns, k, k))
		}
	}

	// test .get
	for i := 0; i < nsNum; i++ {
		ns := getNs(i)
		for j := 0; j < valNum; j++ {
			k := getKey(j)
			v, err := cfgmgr.get(ctx, ns, k)
			require.NoError(t, err)
			require.Equal(t, string(v.Key), path.Join(DefaultEtcdPath, ns, k))
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
			require.NoError(t, cfgmgr.set(ctx, ns, k, k))

			require.NoError(t, cfgmgr.del(ctx, ns, k))
		}
		vals, err := cfgmgr.list(ctx, ns)
		require.NoError(t, err)
		require.Len(t, vals, 0)
	}
}

func TestBaseConcurrency(t *testing.T) {
	cfgmgr, ctx := testConfigManager(t, &config.Config{})

	var wg waitgroup.WaitGroup
	batchNum := 16
	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)
		wg.Run(func() {
			require.NoError(t, cfgmgr.set(ctx, k, "1", "1"))
		})

		wg.Run(func() {
			err := cfgmgr.del(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		require.NoError(t, cfgmgr.set(ctx, k, "1", "1"))
	}

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		wg.Run(func() {
			require.NoError(t, cfgmgr.set(ctx, k, "1", "1"))
		})

		wg.Run(func() {
			_, err := cfgmgr.get(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

func TestBaseWatch(t *testing.T) {
	cfgmgr, ctx := testConfigManager(t, &config.Config{})

	ch := make(chan string, 1)
	cfgmgr.watch(ctx, "test", "t", func(_ *zap.Logger, e mvccpb.Event) {
		ch <- string(e.Kv.Value)
	})

	// set it
	require.NoError(t, cfgmgr.set(ctx, "test", "t", "1"))
	// now the only way to check watch is to wait
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting chan")
	case tg := <-ch:
		for len(ch) > 0 {
			tg = <-ch
		}
		require.Equal(t, "1", tg)
	}
}
