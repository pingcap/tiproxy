// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

const DefaultCertExpiration = 24 * 90 * time.Hour

func CreateTLSCertificates(logger *zap.Logger, certpath, keypath, capath string, rsaKeySize int, expiration time.Duration) error {
	logger = logger.With(zap.String("cert", certpath), zap.String("key", keypath), zap.String("ca", capath), zap.Int("rsaKeySize", rsaKeySize))

	_, e1 := os.Stat(certpath)
	_, e2 := os.Stat(keypath)
	if errors.Is(e1, os.ErrExist) || errors.Is(e2, os.ErrExist) {
		logger.Info("either cert or key exists")
		return nil
	}

	if capath != "" {
		_, e3 := os.Stat(capath)
		if errors.Is(e3, os.ErrExist) {
			logger.Info("ca exists")
			return nil
		}
	}

	if err := os.MkdirAll(filepath.Dir(keypath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(certpath), 0755); err != nil {
		return err
	}
	if capath != "" {
		if err := os.MkdirAll(filepath.Dir(capath), 0755); err != nil {
			return err
		}
	}

	certPEM, keyPEM, caPEM, err := createTempTLS(rsaKeySize, expiration)
	if err != nil {
		return err
	}

	if err := os.WriteFile(certpath, certPEM, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(keypath, keyPEM, 0600); err != nil {
		return err
	}
	if capath != "" {
		if err := os.WriteFile(capath, caPEM, 0600); err != nil {
			return err
		}
	}

	logger.Info("TLS Certificates created")
	return nil
}

func createTempTLS(rsaKeySize int, expiration time.Duration) ([]byte, []byte, []byte, error) {
	if rsaKeySize <= 0 {
		rsaKeySize = 4096
	} else if rsaKeySize < 1024 {
		rsaKeySize = 1024
	}

	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(expiration),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, nil, nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	if err := pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	}); err != nil {
		return nil, nil, nil, err
	}

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(expiration),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, err
	}

	certPEM := new(bytes.Buffer)
	if err := pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}); err != nil {
		return nil, nil, nil, err
	}

	keyPEM := new(bytes.Buffer)
	if err := pem.Encode(keyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	}); err != nil {
		return nil, nil, nil, err
	}

	return certPEM.Bytes(), keyPEM.Bytes(), caPEM.Bytes(), nil
}

// CreateTLSConfigForTest is from https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251.
func CreateTLSConfigForTest() (serverTLSConf *tls.Config, clientTLSConf *tls.Config, err error) {
	certPEM, keyPEM, caPEM, uerr := createTempTLS(1024, DefaultCertExpiration)
	if uerr != nil {
		err = uerr
		return
	}

	serverCert, uerr := tls.X509KeyPair(certPEM, keyPEM)
	if uerr != nil {
		err = uerr
		return
	}

	serverTLSConf = &tls.Config{
		MinVersion:   tls.VersionTLS11,
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM)
	clientTLSConf = &tls.Config{
		MinVersion:         tls.VersionTLS11,
		InsecureSkipVerify: true,
		RootCAs:            certpool,
	}

	return
}

func BuildClientTLSConfig(logger *zap.Logger, cfg config.TLSConfig) (*tls.Config, error) {
	logger = logger.With(zap.String("tls", "client"))
	if !cfg.HasCA() {
		if cfg.SkipCA {
			// still enable TLS without verify server certs
			return &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS11,
			}, nil
		}
		logger.Info("no CA to verify server connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{
		MinVersion: tls.VersionTLS11,
	}
	tcfg.RootCAs = x509.NewCertPool()
	certBytes, err := os.ReadFile(cfg.CA)
	if err != nil {
		return nil, errors.Errorf("failed to read CA: %w", err)
	}
	if !tcfg.RootCAs.AppendCertsFromPEM(certBytes) {
		return nil, errors.Errorf("failed to append CA")
	}

	if !cfg.HasCert() {
		logger.Info("no certificates, server may reject the connection")
		return tcfg, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.Errorf("failed to load certs for: %w", err)
	}
	tcfg.Certificates = append(tcfg.Certificates, cert)

	return tcfg, nil
}

// GetMinTLSVer parses the min tls version from config and reports warning if necessary.
func GetMinTLSVer(tlsVerStr string, logger *zap.Logger) uint16 {
	var minTLSVersion uint16 = tls.VersionTLS12
	switch {
	case strings.HasSuffix(tlsVerStr, "1.0"):
		minTLSVersion = tls.VersionTLS10
	case strings.HasSuffix(tlsVerStr, "1.1"):
		minTLSVersion = tls.VersionTLS11
	case strings.HasSuffix(tlsVerStr, "1.2"):
		minTLSVersion = tls.VersionTLS12
	case strings.HasSuffix(tlsVerStr, "1.3"):
		minTLSVersion = tls.VersionTLS13
	case len(tlsVerStr) == 0:
	default:
		logger.Warn("Invalid TLS version, using default instead", zap.String("tls-version", tlsVerStr))
	}
	if minTLSVersion < tls.VersionTLS12 {
		logger.Warn("Minimum TLS version allows pre-TLSv1.2 protocols, this is not recommended")
	}
	return minTLSVersion
}
