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
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"
)

const DefaultCertExpiration = 10 * 365 * 24 * time.Hour

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
	if rsaKeySize < 1024 {
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
	certPEM, keyPEM, caPEM, uerr := createTempTLS(0, DefaultCertExpiration)
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
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM)
	clientTLSConf = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		RootCAs:            certpool,
	}

	return
}

func BuildServerTLSConfig(logger *zap.Logger, cfg config.TLSConfig) (*tls.Config, error) {
	logger = logger.With(zap.String("tls", "server"))
	if !cfg.HasCert() {
		logger.Info("require certificates to secure clients connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.Errorf("failed to load certs: %w", err)
	}
	tcfg.Certificates = append(tcfg.Certificates, cert)

	if !cfg.HasCA() {
		logger.Info("no CA, server will not authenticate clients (connection is still secured)")
		return tcfg, nil
	}

	tcfg.ClientAuth = tls.RequireAndVerifyClientCert
	tcfg.ClientCAs = x509.NewCertPool()
	certBytes, err := os.ReadFile(cfg.CA)
	if err != nil {
		return nil, errors.Errorf("failed to read CA: %w", err)
	}
	if !tcfg.ClientCAs.AppendCertsFromPEM(certBytes) {
		return nil, errors.Errorf("failed to append CA")
	}
	return tcfg, nil
}

func BuildClientTLSConfig(logger *zap.Logger, cfg config.TLSConfig) (*tls.Config, error) {
	logger = logger.With(zap.String("tls", "client"))
	if !cfg.HasCA() {
		if cfg.SkipCA {
			// still enable TLS without verify server certs
			return &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			}, nil
		}
		logger.Info("no CA to verify server connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
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

func BuildEtcdTLSConfig(logger *zap.Logger, server, peer config.TLSConfig) (clientInfo, peerInfo transport.TLSInfo, err error) {
	logger = logger.With(zap.String("tls", "etcd"))
	clientInfo.Logger = logger
	peerInfo.Logger = logger

	if server.HasCert() {
		clientInfo.CertFile = server.Cert
		clientInfo.KeyFile = server.Key
		if server.HasCA() {
			clientInfo.TrustedCAFile = server.CA
			clientInfo.ClientCertAuth = true
		} else if !server.SkipCA {
			logger.Info("no CA, proxy will not authenticate etcd clients (connection is still secured)")
		}
	}

	if peer.HasCert() {
		peerInfo.CertFile = peer.Cert
		peerInfo.KeyFile = peer.Key
		if peer.HasCA() {
			peerInfo.TrustedCAFile = peer.CA
			peerInfo.ClientCertAuth = true
		} else if peer.SkipCA {
			peerInfo.InsecureSkipVerify = true
			peerInfo.ClientCertAuth = false
		} else {
			err = errors.New("need a full set of cert/key/ca or cert/key/skip-ca to secure etcd peer inter-communication")
			return
		}
	}

	return
}
