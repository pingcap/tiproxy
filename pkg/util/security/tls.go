// Copyright 2020 Ipalfish, Inc.
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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"go.uber.org/zap"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
)

func CreateServerTLSConfig(ca, key, cert, minTLSVer, path string, rsaKeySize int) (tlsConfig *tls.Config, err error) {
	if len(cert) == 0 || len(key) == 0 {
		cert = filepath.Join(path, "/cert.pem")
		key = filepath.Join(path, "/key.pem")
		err = createTLSCertificates(cert, key, rsaKeySize)
		if err != nil {
			logutil.BgLogger().Warn("TLS Certificate creation failed", zap.Error(err))
			return
		}
	}

	var tlsCert tls.Certificate
	tlsCert, err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		logutil.BgLogger().Warn("load x509 failed", zap.Error(err))
		err = errors.Trace(err)
		return
	}

	var minTLSVersion uint16 = tls.VersionTLS11
	switch minTLSVer {
	case "TLSv1.0":
		minTLSVersion = tls.VersionTLS10
	case "TLSv1.1":
		minTLSVersion = tls.VersionTLS11
	case "TLSv1.2":
		minTLSVersion = tls.VersionTLS12
	case "TLSv1.3":
		minTLSVersion = tls.VersionTLS13
	case "":
	default:
		logutil.BgLogger().Warn(
			"Invalid TLS version, using default instead",
			zap.String("tls-version", minTLSVer),
		)
	}
	if minTLSVersion < tls.VersionTLS12 {
		logutil.BgLogger().Warn(
			"Minimum TLS version allows pre-TLSv1.2 protocols, this is not recommended",
		)
	}

	// Try loading CA cert.
	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(ca) > 0 {
		var caCert []byte
		caCert, err = os.ReadFile(ca)
		if err != nil {
			logutil.BgLogger().Warn("read file failed", zap.Error(err))
			err = errors.Trace(err)
			return
		}
		certPool = x509.NewCertPool()
		if certPool.AppendCertsFromPEM(caCert) {
			clientAuthPolicy = tls.VerifyClientCertIfGiven
		}
	}

	// This excludes ciphers listed in tls.InsecureCipherSuites() and can be used to filter out more
	var cipherSuites []uint16
	var cipherNames []string
	for _, sc := range tls.CipherSuites() {
		switch sc.ID {
		case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA, tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
			logutil.BgLogger().Info("Disabling weak cipherSuite", zap.String("cipherSuite", sc.Name))
		default:
			cipherNames = append(cipherNames, sc.Name)
			cipherSuites = append(cipherSuites, sc.ID)
		}

	}
	logutil.BgLogger().Info("Enabled ciphersuites", zap.Strings("cipherNames", cipherNames))

	/* #nosec G402 */
	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
		MinVersion:   minTLSVersion,
		CipherSuites: cipherSuites,
	}
	return
}

func createTLSCertificates(certpath string, keypath string, rsaKeySize int) error {
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

	logutil.BgLogger().Info("TLS Certificates created", zap.String("cert", certpath), zap.String("key", keypath),
		zap.Duration("validity", certValidity), zap.Int("rsaKeySize", rsaKeySize))
	return nil
}

// CreateClusterTLSConfig generates tls's config based on security section of the config.
func CreateClusterTLSConfig(sslCA, sslKey, sslCert string) (tlsConfig *tls.Config, err error) {
	if len(sslCA) != 0 {
		certPool := x509.NewCertPool()
		// Create a certificate pool from the certificate authority
		var ca []byte
		ca, err = os.ReadFile(sslCA)
		if err != nil {
			err = errors.Errorf("could not read ca certificate: %s", err)
			return
		}
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			err = errors.New("failed to append ca certs")
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:   certPool,
			ClientCAs: certPool,
		}

		if len(sslCert) != 0 && len(sslKey) != 0 {
			getCert := func() (*tls.Certificate, error) {
				// Load the client certificates from disk
				cert, err := tls.LoadX509KeyPair(sslCert, sslKey)
				if err != nil {
					return nil, errors.Errorf("could not load client key pair: %s", err)
				}
				return &cert, nil
			}
			// pre-test cert's loading.
			if _, err = getCert(); err != nil {
				return
			}
			tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
			tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
		}
	}
	return
}

func CreateClientTLSConfig(sslCA, sslKey, sslCert string) (tlsConfig *tls.Config, err error) {
	tlsConfig, err = CreateClusterTLSConfig(sslCA, sslKey, sslCert)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		return tlsConfig, nil
	}
	tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	return
}
