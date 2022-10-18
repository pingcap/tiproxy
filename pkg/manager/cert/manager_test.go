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
	"bytes"
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
	err := security.AutoTLS(lg, sqlCfg, true, dir, "sql", 0)
	require.NoError(t, err)
	clusterCfg := &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 0)
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
	certMgr.SetAutoCertInterval(50 * time.Millisecond)
	err = certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	areCertsDifferent := func(before, after [4][][]byte) bool {
		for i := 0; i < 4; i++ {
			if len(before[i]) != len(after[i]) {
				continue
			}
			if before[i] == nil || after[i] == nil {
				continue
			}
			different := false
			for j := 0; j < len(before[i]); j++ {
				if !bytes.Equal(before[i][j], after[i][j]) {
					different = true
					break
				}
			}
			if !different {
				return false
			}
		}
		return true
	}

	var before = getAllCertificates(t, certMgr)
	sqlCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, sqlCfg, true, dir, "sql", 0)
	require.NoError(t, err)
	clusterCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 0)
	require.NoError(t, err)

	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timer.C:
			t.Fatal("timeout")
		case <-time.After(100 * time.Millisecond):
			var after = getAllCertificates(t, certMgr)
			if areCertsDifferent(before, after) {
				return
			}
		}
	}
}

func getRawCertificate(t *testing.T, ci *certInfo) [][]byte {
	tlsConfig := ci.getTLS()
	if tlsConfig == nil {
		return nil
	}
	cert, err := tlsConfig.GetCertificate(nil)
	require.NoError(t, err)
	return cert.Certificate
}

func getAllCertificates(t *testing.T, certMgr *CertManager) [4][][]byte {
	return [4][][]byte{
		getRawCertificate(t, &certMgr.serverTLS),
		getRawCertificate(t, &certMgr.peerTLS),
		getRawCertificate(t, &certMgr.sqlTLS),
		getRawCertificate(t, &certMgr.clusterTLS),
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

	rawCerts := getAllCertificates(t, certMgr)
	for i := 0; i < len(rawCerts); i++ {
		require.Nil(t, rawCerts[i])
	}
}
