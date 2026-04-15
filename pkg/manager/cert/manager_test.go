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
	"runtime"
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

// kubelet projects Secret files through a stable user-visible symlink that points
// to ..data/<file>, and updates the content by atomically switching ..data to a
// new timestamped directory.
func initKubeletSecret(t *testing.T, volumeDir, version string, files map[string]string) {
	require.NoError(t, os.MkdirAll(volumeDir, 0755))
	writeKubeletSecretVersion(t, volumeDir, version, files)
	require.NoError(t, os.Symlink(version, filepath.Join(volumeDir, "..data")))
	for name := range files {
		require.NoError(t, os.Symlink(filepath.Join("..data", name), filepath.Join(volumeDir, name)))
	}
}

func updateKubeletSecret(t *testing.T, volumeDir, oldVersion, newVersion string, files map[string]string) {
	writeKubeletSecretVersion(t, volumeDir, newVersion, files)
	require.NoError(t, os.Symlink(newVersion, filepath.Join(volumeDir, "..data_tmp")))
	require.NoError(t, os.Rename(filepath.Join(volumeDir, "..data_tmp"), filepath.Join(volumeDir, "..data")))
	require.NoError(t, os.RemoveAll(filepath.Join(volumeDir, oldVersion)))
}

func writeKubeletSecretVersion(t *testing.T, volumeDir, version string, files map[string]string) {
	versionDir := filepath.Join(volumeDir, version)
	require.NoError(t, os.MkdirAll(versionDir, 0755))
	for name, src := range files {
		copyFile(t, src, filepath.Join(versionDir, name))
	}
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

func TestReloadByFsnotify(t *testing.T) {
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
	copyFile(t, caPath1, caPath) // caPath1 is incorrect
	copyFile(t, keyPath2, keyPath)
	copyFile(t, certPath2, certPath)

	certMgr := NewCertManager()
	certMgr.SetRetryInterval(time.Hour)
	require.NoError(t, certMgr.Init(&config.Config{
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
	}, lg, nil))
	t.Cleanup(certMgr.Close)

	stls := certMgr.ServerSQLTLS()
	ctls := certMgr.SQLTLS()
	clientErr, serverErr := connectWithTLS(ctls, stls)
	require.ErrorContains(t, clientErr, "certificate signed by unknown authority")
	require.ErrorContains(t, serverErr, "bad certificate")

	copyFile(t, caPath2, caPath) // caPath2 is correct
	require.Eventually(t, func() bool {
		clientErr, serverErr = connectWithTLS(ctls, stls)
		return clientErr == nil && serverErr == nil
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReloadByFsnotifyForKubeletSecret(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("kubelet Secret projection watcher behavior is only verified on linux, got %s", runtime.GOOS)
	}

	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")
	secretDir := filepath.Join(tmpdir, "secret")
	mountedCAPath := filepath.Join(secretDir, "ca")

	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration, ""))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration, ""))
	version1 := "..2026_04_15_12_34_01.11111111"
	version2 := "..2026_04_15_12_34_02.22222222"
	version3 := "..2026_04_15_12_34_03.33333333"
	initKubeletSecret(t, secretDir, version1, map[string]string{"ca": caPath1})

	certMgr := NewCertManager()
	certMgr.SetRetryInterval(time.Hour)
	require.NoError(t, certMgr.Init(&config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerSQLTLS: config.TLSConfig{
				Cert: certPath2,
				Key:  keyPath2,
			},
			SQLTLS: config.TLSConfig{
				CA: mountedCAPath,
			},
		},
	}, lg, nil))
	t.Cleanup(certMgr.Close)

	clientErr, serverErr := connectWithTLS(certMgr.SQLTLS(), certMgr.ServerSQLTLS())
	require.ErrorContains(t, clientErr, "certificate signed by unknown authority")
	require.ErrorContains(t, serverErr, "bad certificate")

	updateKubeletSecret(t, secretDir, version1, version2, map[string]string{"ca": caPath2})
	require.Eventually(t, func() bool {
		clientErr, serverErr = connectWithTLS(certMgr.SQLTLS(), certMgr.ServerSQLTLS())
		return clientErr == nil && serverErr == nil
	}, 5*time.Second, 50*time.Millisecond)

	updateKubeletSecret(t, secretDir, version2, version3, map[string]string{"ca": caPath1})
	require.Eventually(t, func() bool {
		clientErr, serverErr = connectWithTLS(certMgr.SQLTLS(), certMgr.ServerSQLTLS())
		return clientErr != nil && serverErr != nil &&
			strings.Contains(clientErr.Error(), "certificate signed by unknown authority") &&
			strings.Contains(serverErr.Error(), "bad certificate")
	}, 5*time.Second, 50*time.Millisecond)
}

func TestReloadByFsnotifyAfterChangeCertFile(t *testing.T) {
	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")
	caPath3 := filepath.Join(tmpdir, "c3", "ca")

	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration, ""))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration, ""))
	createFile(t, caPath3)
	copyFile(t, caPath1, caPath3) // caPath3 is incorrect and will be added to the watcher after config update.

	cfg := config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerSQLTLS: config.TLSConfig{
				Cert: certPath2,
				Key:  keyPath2,
			},
			SQLTLS: config.TLSConfig{
				CA: caPath2,
			},
		},
	}
	cfgCh := make(chan *config.Config, 1)
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(time.Hour)
	require.NoError(t, certMgr.Init(&cfg, lg, cfgCh))
	t.Cleanup(certMgr.Close)

	stls := certMgr.ServerSQLTLS()
	ctls := certMgr.SQLTLS()
	clientErr, serverErr := connectWithTLS(ctls, stls)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)

	cfg.Security.SQLTLS.CA = caPath3
	cfgCh <- &cfg
	require.Eventually(t, func() bool {
		clientErr, serverErr = connectWithTLS(ctls, stls)
		return clientErr != nil && serverErr != nil &&
			strings.Contains(clientErr.Error(), "certificate signed by unknown authority") &&
			strings.Contains(serverErr.Error(), "bad certificate")
	}, 5*time.Second, 50*time.Millisecond)

	copyFile(t, caPath2, caPath3) // caPath3 becomes correct; success depends on the new watch entry taking effect.
	require.Eventually(t, func() bool {
		clientErr, serverErr = connectWithTLS(ctls, stls)
		return clientErr == nil && serverErr == nil
	}, 5*time.Second, 50*time.Millisecond)
}
