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
	"fmt"
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

func createTLSCertificates(logger *zap.Logger, certpath string, keypath string, rsaKeySize int) error {
	_, e1 := os.Stat(certpath)
	_, e2 := os.Stat(keypath)
	if errors.Is(e1, os.ErrExist) &&  errors.Is(e2, os.ErrExist) {
		return nil
	} else if errors.Is(e1, os.ErrExist) ||  errors.Is(e2, os.ErrExist) {
		return errors.New("cert and key should be present or not at the same time")
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

func BuildServerTLSConfig(logger *zap.Logger, cfg config.TLSCert, workdir, mod string, keySize int) (*tls.Config, error) {
	if !cfg.HasCert() {
		if cfg.AutoCerts {
			cfg.Cert = filepath.Join(workdir, mod, "cert.pem")
			cfg.Key = filepath.Join(workdir, mod, "key.pem")
			if err := createTLSCertificates(logger, cfg.Cert, cfg.Key, keySize); err != nil {
				return nil, err
			}
			return BuildServerTLSConfig(logger, cfg, workdir, mod, keySize)
		}

		// TODO: require certs here
		logger.Warn(fmt.Sprintf("require certificates to secure %s clients connections", mod))
		return nil, nil
	}

	tcfg := &tls.Config{}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.Errorf("failed to load certs for %s: %w", mod, err)
	}
	tcfg.Certificates = append(tcfg.Certificates, cert)

	if !cfg.HasCA() {
		logger.Warn(fmt.Sprintf("no signed certs for %s, will not authenticate %s clients (connection is still secured)", mod, mod))
		return tcfg, nil
	}

	tcfg.ClientAuth = tls.RequireAndVerifyClientCert
	tcfg.ClientCAs = x509.NewCertPool()
	certBytes, err := ioutil.ReadFile(cfg.CA)
	if err != nil {
		return nil, errors.Errorf("failed to read CA for %s: %w", mod, err)
	}
	if !tcfg.ClientCAs.AppendCertsFromPEM(certBytes) {
		return nil, errors.Errorf("failed to append CA for %s", mod)
	}
	return tcfg, nil
}

func BuildClientTLSConfig(logger *zap.Logger, cfg config.TLSCert, mod string) (*tls.Config, error) {
	if !cfg.HasCA() {
		logger.Warn(fmt.Sprintf("require CA to verify %s server connections", mod))
		if cfg.SkipCA {
			// still enable TLS without verify server certs
			return &tls.Config{InsecureSkipVerify: true}, nil
		}
		// no TLS
		return nil, nil
	}

	tcfg := &tls.Config{}
	tcfg.ClientAuth = tls.RequireAndVerifyClientCert
	tcfg.ClientCAs = x509.NewCertPool()
	certBytes, err := ioutil.ReadFile(cfg.CA)
	if err != nil {
		return nil, errors.Errorf("failed to read CA for %s: %w", mod, err)
	}
	if !tcfg.ClientCAs.AppendCertsFromPEM(certBytes) {
		return nil, errors.Errorf("failed to append CA for %s", mod)
	}

	if !cfg.HasCert() {
		logger.Warn(fmt.Sprintf("no certs for %s, server may reject the connection", mod))
		return tcfg, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.Errorf("failed to load certs for %s: %w", mod, err)
	}
	tcfg.Certificates = append(tcfg.Certificates, cert)

	return tcfg, nil
}

func BuildEtcdTLSConfig(logger *zap.Logger, client, cluster config.TLSCert, workdir, mod string, keySize int) (clientInfo, peerInfo transport.TLSInfo, err error) {
	if !client.HasCert() {
		if client.AutoCerts {
			client.Cert = filepath.Join(workdir, mod, "cert.pem")
			client.Key = filepath.Join(workdir, mod, "key.pem")
			if err = createTLSCertificates(logger, client.Cert, client.Key, keySize); err != nil {
				return
			}
			return BuildEtcdTLSConfig(logger, client, cluster, workdir, mod, keySize)
		}
	} else {
		clientInfo.CertFile = client.Cert
		clientInfo.KeyFile = client.Key
		if client.HasCA() {
			clientInfo.ClientCertAuth = true
			clientInfo.TrustedCAFile = client.CA
		} else {
			logger.Warn("no signed certs for etcd clients, proxy will not authenticate etcd clients (connection is still secured)")
		}
	}

	if cluster.HasCA() && cluster.HasCert() {
		peerInfo.CertFile = cluster.Cert
		peerInfo.KeyFile = cluster.Key
		peerInfo.TrustedCAFile = cluster.CA
		peerInfo.ClientCertAuth = true
	} else if cluster.HasCA() || cluster.HasCert() {
		err = errors.New("need a full set of cert/ca/key for secure etcd peer inter-communication")
		return
	}

	return
}
