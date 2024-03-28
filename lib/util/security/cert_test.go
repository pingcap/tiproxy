// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
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

	require.NoError(t, CreateTLSCertificates(logger, certPath, keyPath, caPath, 0, time.Hour))

	type certCase struct {
		config.TLSConfig
		server  bool
		checker func(*testing.T, *tls.Config, *CertInfo)
		err     string
	}

	cases := []certCase{
		{
			server: true,
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				CA: caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
				CA:   caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:          certPath,
				Key:           keyPath,
				CA:            caPath,
				MinTLSVersion: "1.1",
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				Cert:   certPath,
				Key:    keyPath,
				CA:     caPath,
				SkipCA: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequestClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts: true,
				CA:        caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			server: true,
			TLSConfig: config.TLSConfig{
				AutoCerts:     true,
				CA:            caPath,
				MinTLSVersion: "1.1",
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Equal(t, tls.RequireAnyClientCert, c.ClientAuth)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
			err: "",
		},
		{
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.Nil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA: true,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				SkipCA: true,
				Cert:   certPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.Nil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				CA: caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, ci.ca.Load())
				require.Nil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				Cert: certPath,
				Key:  keyPath,
				CA:   caPath,
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS12, int(c.MinVersion))
			},
			err: "",
		},
		{
			TLSConfig: config.TLSConfig{
				Cert:          certPath,
				Key:           keyPath,
				CA:            caPath,
				MinTLSVersion: "1.1",
			},
			checker: func(t *testing.T, c *tls.Config, ci *CertInfo) {
				require.NotNil(t, c)
				require.NotNil(t, ci.ca.Load())
				require.NotNil(t, ci.cert.Load())
				require.Equal(t, tls.VersionTLS11, int(c.MinVersion))
			},
			err: "",
		},
	}

	for _, tc := range cases {
		ci := NewCert(tc.server)
		ci.SetConfig(tc.TLSConfig)
		tcfg, err := ci.Reload(logger)
		if len(tc.err) > 0 {
			require.Nil(t, ci)
			require.ErrorContains(t, err, tc.err)
		} else {
			require.NotNil(t, ci)
			require.NoError(t, err)
		}
		if tc.checker != nil {
			tc.checker(t, tcfg, ci)
		}
	}
}

func TestReload(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
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
	require.NoError(t, CreateTLSCertificates(lg, certPath, keyPath, caPath, 0, time.Hour))
	ci := NewCert(true)
	ci.SetConfig(cfg)
	tcfg, err := ci.Reload(lg)
	require.NoError(t, err)
	require.NotNil(t, tcfg)
	expire1 := getExpireTime(t, ci)

	// Replace the cert and then reload. Check that the expiration is different.
	err = CreateTLSCertificates(lg, certPath, keyPath, caPath, 0, 2*time.Hour)
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	require.NoError(t, err)
	expire2 := getExpireTime(t, ci)
	require.NotEqual(t, expire1, expire2)
}

func TestAutoCerts(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := config.TLSConfig{
		AutoCerts: true,
	}

	// Create an auto cert.
	ci := NewCert(true)
	ci.SetConfig(cfg)
	tcfg, err := ci.Reload(lg)
	require.NoError(t, err)
	require.NotNil(t, tcfg)
	cert1 := ci.cert.Load()
	expire1 := getExpireTime(t, ci)
	require.True(t, ci.autoCertExp.Load() < expire1.Unix())

	// The cert will not be recreated now.
	ci.cfg.Load().AutoExpireDuration = (DefaultCertExpiration - time.Hour).String()
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	cert2 := ci.cert.Load()
	require.Equal(t, cert1, cert2)
	expire2 := getExpireTime(t, ci)
	require.Equal(t, expire1, expire2)

	// The cert will be recreated when it almost expires.
	ci.autoCertExp.Store(time.Now().Add(-time.Minute).Unix())
	require.NoError(t, err)
	_, err = ci.Reload(lg)
	require.NoError(t, err)
	expire3 := getExpireTime(t, ci)
	require.NotEqual(t, expire1, expire3)
}

func getExpireTime(t *testing.T, ci *CertInfo) time.Time {
	cert := ci.cert.Load()
	cp, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	return cp.NotAfter
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

	require.NoError(t, CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, DefaultCertExpiration))
	require.NoError(t, CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, DefaultCertExpiration))

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
	err = loadCA(caPath1, clientCert.ca.Load())
	require.NoError(t, err)
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

func loadCA(caPath string, pool *x509.CertPool) error {
	pemCerts, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		certBytes := block.Bytes
		cert, err := x509.ParseCertificate(certBytes)
		if err != nil {
			continue
		}
		pool.AddCert(cert)
	}
	return nil
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
