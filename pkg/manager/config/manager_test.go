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
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type testingLog struct {
	*testing.T
}

func (t *testingLog) Write(b []byte) (int, error) {
	t.Logf("%s", b)
	return len(b), nil
}

func testConfigManager(t *testing.T, cfg config.ConfigManager) (*ConfigManager, context.Context) {
	addr, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)

	testDir := t.TempDir()

	logger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(&testingLog{t}),
		zap.InfoLevel,
	)).Named(t.Name())

	etcd_cfg := embed.NewConfig()
	etcd_cfg.LCUrls = []url.URL{*addr}
	etcd_cfg.LPUrls = []url.URL{*addr}
	etcd_cfg.Dir = filepath.Join(testDir, "etcd")
	etcd_cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger.Named("etcd"))
	etcd, err := embed.StartEtcd(etcd_cfg)
	require.NoError(t, err)

	ends := make([]string, len(etcd.Clients))
	for i := range ends {
		ends[i] = etcd.Clients[i].Addr().String()
	}

	cfgmgr := NewConfigManager()
	require.NoError(t, cfgmgr.Init(ends, cfg, logger))

	t.Cleanup(func() {
		require.NoError(t, cfgmgr.Close())
		etcd.Close()
		os.RemoveAll(testDir)
	})

	ctx := context.Background()
	if ddl, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, ddl)
		t.Cleanup(cancel)
	}

	return cfgmgr, ctx
}

func TestBase(t *testing.T) {
	cfgmgr, ctx := testConfigManager(t, config.ConfigManager{
		IgnoreWrongNamespace: true,
	})

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
			_, err := cfgmgr.set(ctx, ns, k, k)
			require.NoError(t, err)
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
			_, err := cfgmgr.set(ctx, ns, k, k)
			require.NoError(t, err)

			require.NoError(t, cfgmgr.del(ctx, ns, k))
		}
		vals, err := cfgmgr.list(ctx, ns)
		require.NoError(t, err)
		require.Len(t, vals, 0)
	}
}

func TestBaseConcurrency(t *testing.T) {
	cfgmgr, ctx := testConfigManager(t, config.ConfigManager{
		IgnoreWrongNamespace: true,
	})

	var wg waitgroup.WaitGroup
	batchNum := 16
	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)
		wg.Run(func() {
			_, err := cfgmgr.set(ctx, k, "1", "1")
			require.NoError(t, err)
		})

		wg.Run(func() {
			err := cfgmgr.del(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		_, err := cfgmgr.set(ctx, k, "1", "1")
		require.NoError(t, err)
	}

	for i := 0; i < batchNum; i++ {
		k := fmt.Sprint(i)

		wg.Run(func() {
			_, err := cfgmgr.set(ctx, k, "1", "1")
			require.NoError(t, err)
		})

		wg.Run(func() {
			_, err := cfgmgr.get(ctx, k, "1")
			require.NoError(t, err)
		})
	}
	wg.Wait()
}
