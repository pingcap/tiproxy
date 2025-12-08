// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func TestCertServer(t *testing.T) {
	logger, _ := logger.CreateLoggerForTest(t)
	tmpdir := t.TempDir()
	certPath := filepath.Join(tmpdir, "cert")
	keyPath := filepath.Join(tmpdir, "key")
	caPath := filepath.Join(tmpdir, "ca")

	require.NoError(t, CreateTLSCertificates(logger, certPath, keyPath, caPath, 1024, time.Hour, ""))

	type certCase struct {
		config.TLSConfig
		server  bool
		checker func(*testing.T, *tls.Config, *CertInfo)
	}

	cases := []certCase{
		{
			server: true,
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.cert.Load())
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				CA:         caPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.cert.Load())
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts:  true,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:       certPath,
				Key:        keyPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:       certPath,
				Key:        keyPath,
				CA:         caPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:          certPath,
				Key:           keyPath,
				CA:            caPath,
				MinTLSVersion: "1.1",
				RSAKeySize:    1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:       certPath,
				Key:        keyPath,
				CA:         caPath,
				SkipCA:     true,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequestClientCert, c.ClientAuth)
				require.NotNil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts:  true,
				CA:         caPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts:     true,
				CA:            caPath,
				MinTLSVersion: "1.1",
				RSAKeySize:    1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, c.ClientCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
		},
		{
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.cert.Load())
			},
		},
		{
			TLSConfig: config.TLSConfig{
				Cert:       certPath,
				Key:        keyPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.cert.Load())
			},
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA:     true,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, c.RootCAs)
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA:     true,
				Cert:       certPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, c.RootCAs)
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			TLSConfig: config.TLSConfig{
				CA:         caPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, c.RootCAs)
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			TLSConfig: config.TLSConfig{
				Cert:       certPath,
				Key:        keyPath,
				CA:         caPath,
				RSAKeySize: 1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, c.RootCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
		},
		{
			TLSConfig: config.TLSConfig{
				Cert:          certPath,
				Key:           keyPath,
				CA:            caPath,
				MinTLSVersion: "1.1",
				RSAKeySize:    1024,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, c.RootCAs)
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
		},
	}

	for i, tc := range cases {
		ci := NewCert(tc.server)
		ci.SetConfig(tc.TLSConfig)
		tcfg, err := ci.Reload(logger)
		require.NoError(t, err, "case %d", i)
		if tc.checker != nil {
			tc.checker(t, tcfg, ci)
		}
	}
}

func TestReload(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	tmpdir := t.TempDir()
	certPath := filepath.Join(tmpdir, "cert")
	keyPath := filepath.Join(tmpdir, "key")
	caPath := filepath.Join(tmpdir, "ca")
	cfg := config.TLSConfig{
		CA:   caPath,
		Cert: certPath,
		Key:  keyPath,
	}

	// Create a cert and record the expiration.
	require.NoError(t, CreateTLSCertificates(lg, certPath, keyPath, caPath, 1024, time.Hour, ""))
	ci := NewCert(true)
	ci.SetConfig(cfg)
	tcfg, err := ci.Reload(lg)
	require.NoError(t, err)
	require.NotNil(t, tcfg)
	expire1 := ci.getExpireTime()
	require.Equal(t, 1, strings.Count(text.String(), "update cert expiration"))

	// Replace the cert and then reload. Check that the expiration is different.
	err = CreateTLSCertificates(lg, certPath, keyPath, caPath, 1024, 2*time.Hour, "")
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	require.NoError(t, err)
	expire2 := ci.getExpireTime()
	require.NotEqual(t, expire1, expire2)
	require.Equal(t, 2, strings.Count(text.String(), "update cert expiration"))
}

func TestAutoCerts(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := config.TLSConfig{
		AutoCerts:  true,
		RSAKeySize: 1024,
	}

	// Create an auto cert.
	ci := NewCert(true)
	ci.SetConfig(cfg)
	tcfg, err := ci.Reload(lg)
	require.NoError(t, err)
	require.NotNil(t, tcfg)
	cert1 := ci.cert.Load()
	expire1 := ci.getExpireTime()
	require.True(t, ci.autoCertExp.Load() < expire1.Unix())

	// The cert will not be recreated now.
	ci.cfg.Load().AutoExpireDuration = (DefaultCertExpiration - time.Hour).String()
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	cert2 := ci.cert.Load()
	require.Equal(t, cert1, cert2)
	expire2 := ci.getExpireTime()
	require.Equal(t, expire1, expire2)

	// The cert will be recreated when it almost expires.
	ci.autoCertExp.Store(time.Now().Add(-time.Minute).Unix())
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	require.NoError(t, err)
	expire3 := ci.getExpireTime()
	require.NotEqual(t, expire1, expire3)
}

func TestSetConfig(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	ci := NewCert(false)
	cfg := config.TLSConfig{
		SkipCA: true,
	}
	ci.SetConfig(cfg)
	tcfg, err := ci.Reload(lg)
	require.NoError(t, err)
	require.NotNil(t, tcfg)
	require.True(t, tcfg.InsecureSkipVerify)

	cfg = config.TLSConfig{
		SkipCA: false,
	}
	ci.SetConfig(cfg)
	tcfg, err = ci.Reload(lg)
	require.NoError(t, err)
	require.Nil(t, tcfg)
}

// Test that a cert pool can store multiple CAs and every CA works.
func TestCertPool(t *testing.T) {
	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")

	require.NoError(t, CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 1024, DefaultCertExpiration, ""))
	require.NoError(t, CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 1024, DefaultCertExpiration, ""))

	serverCfg := config.TLSConfig{
		Cert: certPath1,
		Key:  keyPath1,
	}
	serverCert := NewCert(true)
	serverCert.cfg.Store(&serverCfg)
	serverTLS, err := serverCert.Reload(lg)
	require.NoError(t, err)

	clientCfg := config.TLSConfig{
		CA: caPath2,
	}
	clientCert := NewCert(false)
	clientCert.cfg.Store(&clientCfg)
	clientTLS, err := clientCert.Reload(lg)
	require.NoError(t, err)
	// caPath2 fails to verify certPath1.
	clientErr, serverErr := connectWithTLS(clientTLS, serverTLS)
	require.Error(t, clientErr)
	require.Error(t, serverErr)

	// Add both caPath1 and caPath2 to the cert pool and it succeeds to verify certPath1.
	caPEM, err := os.ReadFile(caPath1)
	require.NoError(t, err)
	require.True(t, clientTLS.RootCAs.AppendCertsFromPEM(caPEM))
	clientErr, serverErr = connectWithTLS(clientTLS, serverTLS)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)

	// The cert pool can also verify certPath2.
	serverCfg = config.TLSConfig{
		Cert: certPath2,
		Key:  keyPath2,
	}
	serverCert.cfg.Store(&serverCfg)
	serverTLS, err = serverCert.Reload(lg)
	require.NoError(t, err)
	clientErr, serverErr = connectWithTLS(clientTLS, serverTLS)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)
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

func TestCommonName(t *testing.T) {
	tests := []struct {
		commonName string
		allowedCN  []string
		success    bool
	}{
		{
			commonName: "",
			allowedCN:  []string{"server1"},
			success:    false,
		},
		{
			commonName: "server1",
			allowedCN:  []string{"server1"},
			success:    true,
		},
		{
			commonName: "server1",
			allowedCN:  []string{"server2"},
			success:    false,
		},
		{
			commonName: "server1",
			allowedCN:  []string{"server2", "server1"},
			success:    true,
		},
	}

	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	clientCertPath := filepath.Join(tmpdir, "c1", "cert")
	clientKeyPath := filepath.Join(tmpdir, "c1", "key")
	clientCAPath := filepath.Join(tmpdir, "c1", "ca")
	serverCertPath := filepath.Join(tmpdir, "c2", "cert")
	serverKeyPath := filepath.Join(tmpdir, "c2", "key")
	serverCAPath := filepath.Join(tmpdir, "c2", "ca")
	require.NoError(t, CreateTLSCertificates(lg, serverCertPath, serverKeyPath, serverCAPath, 2048, DefaultCertExpiration, ""))

	for i, test := range tests {
		require.NoError(t, CreateTLSCertificates(lg, clientCertPath, clientKeyPath, clientCAPath, 2048, DefaultCertExpiration, test.commonName))
		serverCfg := config.TLSConfig{
			CertAllowedCN: test.allowedCN,
			Cert:          serverCertPath,
			Key:           serverKeyPath,
			CA:            clientCAPath,
		}
		serverCert := NewCert(true)
		serverCert.cfg.Store(&serverCfg)
		serverTLS, err := serverCert.Reload(lg)
		require.NoError(t, err)

		clientCfg := config.TLSConfig{
			Cert: clientCertPath,
			Key:  clientKeyPath,
			CA:   serverCAPath,
		}
		clientCert := NewCert(false)
		clientCert.cfg.Store(&clientCfg)
		clientTLS, err := clientCert.Reload(lg)
		require.NoError(t, err)

		clientErr, serverErr := connectWithTLS(clientTLS, serverTLS)
		if test.success {
			require.NoError(t, clientErr, "case %d", i)
			require.NoError(t, serverErr, "case %d", i)
		} else {
			require.Error(t, serverErr, "case %d", i)
		}
	}
}
