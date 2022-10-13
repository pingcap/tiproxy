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

package cert

import (
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/stretchr/testify/require"
)

// Test that the certs are automatically created and reloaded.
func TestReloadCerts(t *testing.T) {
	dir := t.TempDir()
	lg := logger.CreateLoggerForTest(t)
	sqlCfg := &config.TLSConfig{AutoCerts: true}
	err := security.AutoTLS(lg, sqlCfg, true, dir, "sql", 4096, 240*time.Hour)
	require.NoError(t, err)
	clusterCfg := &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 4096, 240*time.Hour)
	require.NoError(t, err)

	cfg := &config.Config{
		Workdir: dir,
		Security: config.Security{
			ServerTLS: config.TLSConfig{
				AutoCerts: true,
			},
			PeerTLS: config.TLSConfig{
				AutoCerts: true,
			},
			SQLTLS:     *sqlCfg,
			ClusterTLS: *clusterCfg,
		},
	}
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(100 * time.Millisecond)
	certMgr.SetAutoExp(240 * time.Hour)
	err = certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	var before = [3]time.Time{
		certMgr.serverTLS.expTime,
		certMgr.sqlTLS.expTime,
		certMgr.clusterTLS.expTime,
	}

	sqlCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, sqlCfg, true, dir, "sql", 4096, 480*time.Hour)
	require.NoError(t, err)
	clusterCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 4096, 480*time.Hour)
	require.NoError(t, err)
	certMgr.SetReloadAhead(360 * time.Hour)
	certMgr.SetAutoExp(480 * time.Hour)

	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timer.C:
			t.Fatal("timeout")
		case <-time.After(100 * time.Millisecond):
			var after = [3]time.Time{
				certMgr.serverTLS.expTime,
				certMgr.sqlTLS.expTime,
				certMgr.clusterTLS.expTime,
			}
			if after[0] != before[0] && after[1] != before[1] && after[2] != before[2] {
				return
			}
		}
	}
}

// Test that configuring no certs still works.
func TestReloadEmptyCerts(t *testing.T) {
	dir := t.TempDir()
	lg := logger.CreateLoggerForTest(t)
	cfg := &config.Config{
		Workdir: dir,
		Security: config.Security{
			ServerTLS:  config.TLSConfig{},
			PeerTLS:    config.TLSConfig{},
			SQLTLS:     config.TLSConfig{},
			ClusterTLS: config.TLSConfig{},
		},
	}
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(100 * time.Millisecond)
	err := certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	require.Equal(t, certMgr.serverTLS.expTime, time.Time{})
	require.Equal(t, certMgr.sqlTLS.expTime, time.Time{})
	require.Equal(t, certMgr.clusterTLS.expTime, time.Time{})
}
