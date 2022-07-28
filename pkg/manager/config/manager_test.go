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
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/stretchr/testify/require"
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

func testConfigManager(t *testing.T) {
	addr, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)

	logger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(&testingLog{t}),
		zap.InfoLevel,
	))
	logger.With(zap.String("testName", t.Name()))

	etcd_cfg := embed.NewConfig()
	etcd_cfg.LCUrls = []url.URL{*addr}
	etcd_cfg.LPUrls = []url.URL{*addr}
	cwd, err := os.Getwd()
	require.NoError(t, err)
	etcd_cfg.WalDir = filepath.Join(cwd, "etcd")
	etcd_cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger)
	etcd, err := embed.StartEtcd(etcd_cfg)
	require.NoError(t, err)

	ends := make([]string, len(etcd.Clients))
	for i := range ends {
		ends[i] = etcd.Clients[i].Addr().String()
	}

	cfgmgr := NewConfigManager()
	require.NoError(t, cfgmgr.Init(ends, config.ConfigManager{}, logger))

	require.NoError(t, cfgmgr.Close())
}

func TestConfig(t *testing.T) {
	testConfigManager(t)
}
