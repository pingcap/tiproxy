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
	"io/ioutil"
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

func createTLSConfigificates(logger *zap.Logger, certpath string, keypath string, rsaKeySize int) error {
	_, e1 := os.Stat(certpath)
	_, e2 := os.Stat(keypath)
	if errors.Is(e1, os.ErrExist) && errors.Is(e2, os.ErrExist) {
		return nil
	} else if errors.Is(e1, os.ErrExist) || errors.Is(e2, os.ErrExist) {
		return errors.New("cert and key should be present or not at the same time")
	}

	if err := os.MkdirAll(filepath.Dir(keypath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(certpath), 0755); err != nil {
		return err
	}

	privkey, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return err
	}

	certValidity := 90 * 24 * time.Hour // 90 days
	notBefore := time.Now()
	notAfter := notBefore.Add(certValidity)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	template := x509.Certificate{
		Subject: pkix.Name{
			CommonName: "TiDB_Server_Auto_Generated_Server_Certificate",
		},
		SerialNumber: big.NewInt(1),
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		DNSNames:     []string{hostname},
	}

	// DER: Distinguished Encoding Rules, this is the ASN.1 encoding rule of the certificate.
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privkey.PublicKey, privkey)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certpath)
	if err != nil {
		return err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return err
	}
	if err := certOut.Close(); err != nil {
		return err
	}

	keyOut, err := os.OpenFile(keypath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(privkey)
	if err != nil {
		return err
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return err
	}

	if err := keyOut.Close(); err != nil {
		return err
	}

	logger.Info("TLS Certificates created", zap.String("cert", certpath), zap.String("key", keypath),
		zap.Duration("validity", certValidity), zap.Int("rsaKeySize", rsaKeySize))
	return nil
}

func PreProcessTLSConfig(logger *zap.Logger, scfg *config.TLSConfig, workdir, mod string, keySize int) error {
	if !scfg.HasCert() && scfg.AutoCerts {
		scfg.Cert = filepath.Join(workdir, mod, "cert.pem")
		scfg.Key = filepath.Join(workdir, mod, "key.pem")
		if err := createTLSConfigificates(logger, scfg.Cert, scfg.Key, keySize); err != nil {
			return errors.WithStack(err)
		}
		return PreProcessTLSConfig(logger, scfg, workdir, mod, keySize)
	}
	return nil
}

// CreateTLSConfigForTest is from https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251.
func CreateTLSConfigForTest() (serverTLSConf *tls.Config, clientTLSConf *tls.Config, err error) {
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
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

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
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	serverCert, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	if err != nil {
		return nil, nil, err
	}

	serverTLSConf = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM.Bytes())
	clientTLSConf = &tls.Config{
		RootCAs: certpool,
	}

	return
}

func BuildServerTLSConfig(logger *zap.Logger, cfg config.TLSConfig) (*tls.Config, error) {
	logger = logger.With(zap.String("tls", "server"))
	if !cfg.HasCert() {
		logger.Warn("require certificates to secure clients connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.Errorf("failed to load certs: %w", err)
	}
	tcfg.Certificates = append(tcfg.Certificates, cert)

	if !cfg.HasCA() {
		logger.Warn("no CA, server will not authenticate clients (connection is still secured)")
		return tcfg, nil
	}

	tcfg.ClientAuth = tls.RequireAndVerifyClientCert
	tcfg.ClientCAs = x509.NewCertPool()
	certBytes, err := ioutil.ReadFile(cfg.CA)
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
			return &tls.Config{InsecureSkipVerify: true}, nil
		}
		logger.Warn("no CA to verify server connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{}
	tcfg.RootCAs = x509.NewCertPool()
	certBytes, err := ioutil.ReadFile(cfg.CA)
	if err != nil {
		return nil, errors.Errorf("failed to read CA: %w", err)
	}
	if !tcfg.RootCAs.AppendCertsFromPEM(certBytes) {
		return nil, errors.Errorf("failed to append CA")
	}

	if !cfg.HasCert() {
		logger.Warn("no certificates, server may reject the connection")
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

	if server.HasCert() {
		clientInfo.CertFile = server.Cert
		clientInfo.KeyFile = server.Key
		if server.HasCA() {
			clientInfo.TrustedCAFile = server.CA
			clientInfo.ClientCertAuth = true
		} else if !server.SkipCA {
			logger.Warn("no CA, proxy will not authenticate etcd clients (connection is still secured)")
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
			err = errors.New("need a full set of cert/key/ca or cert/key/skip-ca for secure etcd peer inter-communication")
			return
		}
	}

	return
}
