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

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cert

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func createFile(t *testing.T, src string) {
	require.NoError(t, os.MkdirAll(filepath.Dir(src), 0755))
	f, err := os.Create(src)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func copyFile(t *testing.T, src, dst string) {
	f1, err := os.Open(src)
	require.NoError(t, err)
	f2, err := os.Create(dst)
	require.NoError(t, err)
	_, err = io.Copy(f2, f1)
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	require.NoError(t, f2.Close())
}

func connectWithTLS(ctls, stls *tls.Config) (clientErr, serverErr error) {
	client, server := net.Pipe()
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		tlsConn := tls.Client(client, ctls)
		clientErr = tlsConn.Handshake()
		_ = client.Close()
	})
	wg.Run(func() {
		tlsConn := tls.Server(server, stls)
		serverErr = tlsConn.Handshake()
		_ = server.Close()
	})
	wg.Wait()
	return
}

// Test various configurations.
func TestInit(t *testing.T) {
	lg := logger.CreateLoggerForTest(t)
	tmpdir := t.TempDir()

	type testcase struct {
		name  string
		cfg   config.Config
		check func(*testing.T, *CertManager)
		err   string
	}
	// should not care much about the internal of *tls.Config,
	// which should be well-tested in pkg/lib/util/security/.
	cases := []testcase{
		{
			name: "empty",
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.ServerTLS())
				require.Nil(t, cm.ClusterTLS())
				require.Nil(t, cm.PeerTLS())
				require.Nil(t, cm.SQLTLS())
			},
		},
		{
			name: "server config",
			cfg: config.Config{
				Security: config.Security{
					ServerTLS: config.TLSConfig{AutoCerts: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.ClusterTLS())
				require.Nil(t, cm.PeerTLS())
				require.Nil(t, cm.SQLTLS())
				require.NotNil(t, cm.ServerTLS())
			},
		},
		{
			name: "client config",
			cfg: config.Config{
				Security: config.Security{
					SQLTLS: config.TLSConfig{SkipCA: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.ClusterTLS())
				require.Nil(t, cm.PeerTLS())
				require.Nil(t, cm.ServerTLS())
				require.NotNil(t, cm.SQLTLS())
			},
		},
		{
			name: "invalid config",
			cfg: config.Config{
				Security: config.Security{
					SQLTLS: config.TLSConfig{CA: filepath.Join(tmpdir, "ca")},
				},
			},
			err: "no such file or directory",
		},
	}

	for i, tc := range cases {
		t.Logf("testcase[%d] start: %+v\n", i, tc)

		certMgr := NewCertManager()
		certMgr.SetRetryInterval(100 * time.Millisecond)
		err := certMgr.Init(&tc.cfg, lg)
		if tc.err != "" {
			require.ErrorContains(t, err, tc.err, fmt.Sprintf("%+v", tc))
		} else {
			require.NoError(t, err)
		}
		if tc.check != nil {
			tc.check(t, certMgr)
		}
		certMgr.Close()
	}
}

// Test rotation works.
func TestRotate(t *testing.T) {
	tmpdir := t.TempDir()
	lg := logger.CreateLoggerForTest(t)
	caPath := filepath.Join(tmpdir, "ca")
	keyPath := filepath.Join(tmpdir, "key")
	certPath := filepath.Join(tmpdir, "cert")
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")

	createFile(t, caPath)
	createFile(t, keyPath)
	createFile(t, certPath)
	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration))

	cfg := &config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerTLS: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
			},
			SQLTLS: config.TLSConfig{
				CA: caPath,
			},
		},
	}
	type testcase struct {
		name      string
		pre       func(*testing.T)
		preErrCli string
		preErrSrv string
		reload    func(*testing.T)
		relErrCli string
		relErrSrv string
	}

	cases := []testcase{
		{
			name: "normal",
			pre: func(t *testing.T) {
				copyFile(t, caPath2, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
		},
		{
			name: "rotate ca",
			pre: func(t *testing.T) {
				copyFile(t, caPath1, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
			preErrCli: "certificate signed by unknown authority",
			preErrSrv: "bad certificate",
			reload: func(t *testing.T) {
				copyFile(t, caPath2, caPath)
			},
		},
		{
			name: "rotate certs",
			pre: func(t *testing.T) {
				copyFile(t, caPath1, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
			preErrCli: "certificate signed by unknown authority",
			preErrSrv: "bad certificate",
			reload: func(t *testing.T) {
				copyFile(t, keyPath1, keyPath)
				copyFile(t, certPath1, certPath)
			},
		},
		{
			name: "rotate key only",
			pre: func(t *testing.T) {
				copyFile(t, caPath1, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
			preErrCli: "certificate signed by unknown authority",
			preErrSrv: "bad certificate",
			reload: func(t *testing.T) {
				copyFile(t, keyPath1, keyPath)
			},
			relErrCli: "certificate signed by unknown authority",
			relErrSrv: "bad certificate",
		},
		{
			name: "rotate cert only",
			pre: func(t *testing.T) {
				copyFile(t, caPath1, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
			preErrCli: "certificate signed by unknown authority",
			preErrSrv: "bad certificate",
			reload: func(t *testing.T) {
				copyFile(t, certPath1, certPath)
			},
			relErrCli: "certificate signed by unknown authority",
			relErrSrv: "bad certificate",
		},
		{
			name: "rotate all",
			pre: func(t *testing.T) {
				copyFile(t, caPath2, caPath)
				copyFile(t, keyPath2, keyPath)
				copyFile(t, certPath2, certPath)
			},
			reload: func(t *testing.T) {
				copyFile(t, caPath1, caPath)
				copyFile(t, keyPath1, keyPath)
				copyFile(t, certPath1, certPath)
			},
		},
	}

	for i, tc := range cases {
		t.Logf("testcase[%d] start: %+v\n", i, tc)

		certMgr := NewCertManager()
		certMgr.SetRetryInterval(100 * time.Millisecond)

		if tc.pre != nil {
			tc.pre(t)
		}
		require.NoError(t, certMgr.Init(cfg, lg))

		stls := certMgr.ServerTLS()
		ctls := certMgr.SQLTLS()

		// pre reloading test
		clientErr, serverErr := connectWithTLS(ctls, stls)
		errmsg := fmt.Sprintf("client: %+v\nserver: %+v\n", clientErr, serverErr)
		if tc.preErrCli != "" {
			require.ErrorContains(t, clientErr, tc.preErrCli, errmsg)
			require.Error(t, serverErr)
		}
		if tc.preErrSrv != "" {
			require.ErrorContains(t, serverErr, tc.preErrSrv, errmsg)
			require.Error(t, clientErr)
		}
		if tc.preErrCli == "" && tc.preErrSrv == "" {
			require.NoError(t, clientErr)
			require.NoError(t, serverErr)
			t.Logf("clientErr: %+v, serverErr: %+v\n", clientErr, serverErr)
		}

		// reloading test
		if tc.reload != nil {
			tc.reload(t)
		}

		timer := time.NewTimer(time.Second)
	outer:
		for {
			select {
			case <-timer.C:
				t.Fatal("timeout on reloading")
			case <-time.After(150 * time.Millisecond):
				clientErr, serverErr := connectWithTLS(ctls, stls)
				errmsg := fmt.Sprintf("client: %+v\nserver: %+v\n", clientErr, serverErr)
				if tc.relErrCli != "" {
					require.ErrorContains(t, clientErr, tc.relErrCli, errmsg)
					require.Error(t, serverErr)
					break outer
				}
				if tc.relErrSrv != "" {
					require.ErrorContains(t, serverErr, tc.relErrSrv, errmsg)
					require.Error(t, clientErr)
					break outer
				}
				if tc.relErrCli == "" && tc.relErrSrv == "" {
					if clientErr == nil && serverErr == nil {
						break outer
					}
					t.Logf("clientErr: %+v, serverErr: %+v\n", clientErr, serverErr)
				}
			}
		}

		certMgr.Close()
	}
}
