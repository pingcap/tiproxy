// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cert

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/security"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
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
	lg, _ := logger.CreateLoggerForTest(t)
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
				require.Nil(t, cm.ServerSQLTLS())
				require.Nil(t, cm.ClusterTLS())
				require.Nil(t, cm.ServerHTTPTLS())
				require.Nil(t, cm.SQLTLS())
			},
		},
		{
			name: "server config",
			cfg: config.Config{
				Security: config.Security{
					ServerSQLTLS:  config.TLSConfig{AutoCerts: true},
					ServerHTTPTLS: config.TLSConfig{AutoCerts: true},
					ClusterTLS:    config.TLSConfig{AutoCerts: true},
					SQLTLS:        config.TLSConfig{AutoCerts: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.ClusterTLS())
				require.Nil(t, cm.SQLTLS())
				require.NotNil(t, cm.ServerHTTPTLS())
				require.NotNil(t, cm.ServerSQLTLS())
			},
		},
		{
			name: "client config",
			cfg: config.Config{
				Security: config.Security{
					ServerSQLTLS:  config.TLSConfig{SkipCA: true},
					ServerHTTPTLS: config.TLSConfig{SkipCA: true},
					ClusterTLS:    config.TLSConfig{SkipCA: true},
					SQLTLS:        config.TLSConfig{SkipCA: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.NotNil(t, cm.ClusterTLS())
				require.NotNil(t, cm.SQLTLS())
				require.Nil(t, cm.ServerHTTPTLS())
				require.Nil(t, cm.ServerSQLTLS())
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
		err := certMgr.Init(&tc.cfg, lg, nil)
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
	lg, _ := logger.CreateLoggerForTest(t)
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
	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration, ""))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration, ""))

	cfg := &config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerSQLTLS: config.TLSConfig{
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
		require.NoError(t, certMgr.Init(cfg, lg, nil))

		stls := certMgr.ServerSQLTLS()
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

		time.Sleep(150 * time.Millisecond)
		require.Eventually(t, func() bool {
			clientErr, serverErr := connectWithTLS(ctls, stls)
			if tc.relErrCli != "" {
				if !strings.Contains(clientErr.Error(), tc.relErrCli) || serverErr == nil {
					t.Logf("clientErr: %+v, serverErr: %+v\n", clientErr, serverErr)
					return false
				}
			}
			if tc.relErrSrv != "" {
				if !strings.Contains(serverErr.Error(), tc.relErrSrv) || clientErr == nil {
					t.Logf("clientErr: %+v, serverErr: %+v\n", clientErr, serverErr)
					return false
				}
			}
			if tc.relErrCli == "" && tc.relErrSrv == "" {
				return clientErr == nil && serverErr == nil
			}
			return true
		}, 5*time.Second, 100*time.Millisecond)
		certMgr.Close()
	}
}

func TestBidirectional(t *testing.T) {
	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")

	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration, ""))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration, "server"))

	cfg := &config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerSQLTLS: config.TLSConfig{
				Cert:          certPath1,
				Key:           keyPath1,
				CA:            caPath2,
				CertAllowedCN: []string{"server"},
			},
			SQLTLS: config.TLSConfig{
				CA:   caPath1,
				Key:  keyPath2,
				Cert: certPath2,
			},
		},
	}

	certMgr := NewCertManager()
	require.NoError(t, certMgr.Init(cfg, lg, nil))
	stls := certMgr.ServerSQLTLS()
	ctls := certMgr.SQLTLS()
	clientErr, serverErr := connectWithTLS(ctls, stls)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)
}

func TestWatchConfig(t *testing.T) {
	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration, ""))

	tests := []struct {
		cfg     config.TLSConfig
		checker func(*tls.Config) bool
	}{
		{
			cfg: config.TLSConfig{
				SkipCA: true,
			},
			checker: func(tlsConfig *tls.Config) bool {
				if tlsConfig == nil {
					return false
				}
				return tlsConfig.InsecureSkipVerify
			},
		},
		{
			cfg: config.TLSConfig{
				SkipCA: false,
				CA:     caPath1,
			},
			checker: func(tlsConfig *tls.Config) bool {
				return tlsConfig.GetCertificate != nil
			},
		},
		{
			cfg: config.TLSConfig{
				SkipCA:        false,
				CA:            caPath1,
				MinTLSVersion: "1.3",
			},
			checker: func(tlsConfig *tls.Config) bool {
				return tlsConfig.MinVersion == tls.VersionTLS13
			},
		},
	}

	cfgCh := make(chan *config.Config)
	certMgr := NewCertManager()
	cfg := config.Config{
		Security: config.Security{
			SQLTLS: config.TLSConfig{
				SkipCA: false,
			},
		},
	}
	require.NoError(t, certMgr.Init(&cfg, lg, cfgCh))
	for _, test := range tests {
		cfg.Security.SQLTLS = test.cfg
		cfgCh <- &cfg
		require.Eventually(t, func() bool {
			return test.checker(certMgr.SQLTLS())
		}, time.Second, 10*time.Millisecond)
	}
}
