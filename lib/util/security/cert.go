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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// Recreate the auto certs one hour before it expires.
	// It should be longer than defaultRetryInterval.
	recreateAutoCertAdvance = 24 * time.Hour
)

var emptyCert = new(tls.Certificate)

type CertInfo struct {
	cfg         config.TLSConfig
	ca          atomic.Value
	cert        atomic.Value
	autoCertExp atomic.Int64
	server      bool
}

func NewCert(lg *zap.Logger, cfg config.TLSConfig, server bool) (*CertInfo, *tls.Config, error) {
	ci := &CertInfo{
		cfg:    cfg,
		server: server,
	}
	tlscfg, err := ci.reload(lg)
	return ci, tlscfg, err
}

func (ci *CertInfo) Reload(lg *zap.Logger) error {
	_, err := ci.reload(lg)
	return err
}

func (ci *CertInfo) reload(lg *zap.Logger) (*tls.Config, error) {
	var tlsConfig *tls.Config
	var err error
	// Some methods to rotate server config:
	// - For certs: customize GetCertificate / GetConfigForClient.
	// - For CA: customize ClientAuth + VerifyPeerCertificate / GetConfigForClient
	// Some methods to rotate client config:
	// - For certs: customize GetClientCertificate
	// - For CA: customize InsecureSkipVerify + VerifyPeerCertificate
	if ci.server {
		tlsConfig, err = ci.buildServerConfig(lg)
	} else {
		tlsConfig, err = ci.buildClientConfig(lg)
	}
	return tlsConfig, err
}

func (ci *CertInfo) getCert(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := ci.cert.Load()
	if val, ok := cert.(*tls.Certificate); ok {
		return val, nil
	}
	return nil, nil
}

func (ci *CertInfo) getClientCert(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert := ci.cert.Load()
	if val, ok := cert.(*tls.Certificate); ok {
		return val, nil
	}
	if cert == nil {
		// GetClientCertificate must return a non-nil Certificate.
		return emptyCert, nil
	}
	return nil, nil
}

func (ci *CertInfo) verifyPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return nil
	}

	certs := make([]*x509.Certificate, len(rawCerts))
	for i, asn1Data := range rawCerts {
		cert, err := x509.ParseCertificate(asn1Data)
		if err != nil {
			return errors.New("tls: failed to parse certificate from server: " + err.Error())
		}
		certs[i] = cert
	}

	cas, ok := ci.ca.Load().(*x509.CertPool)
	if !ok {
		cas = x509.NewCertPool()
	}
	opts := x509.VerifyOptions{
		Roots:         cas,
		Intermediates: x509.NewCertPool(),
	}
	if ci.server {
		opts.KeyUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		// this is the default behavior of Verify()
		// it is not necessary but explicit
		opts.KeyUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	// TODO: not implemented, maybe later
	// opts.DNSName = ci.serverName
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	_, err := certs[0].Verify(opts)
	return err
}

func (ci *CertInfo) loadCA(pemCerts []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
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
	return pool, nil
}

func (ci *CertInfo) buildServerConfig(lg *zap.Logger) (*tls.Config, error) {
	lg = lg.With(zap.String("tls", "server"), zap.Any("cfg", ci.cfg))
	autoCerts := false
	if !ci.cfg.HasCert() {
		if ci.cfg.AutoCerts {
			autoCerts = true
		} else {
			lg.Info("require certificates to secure clients connections, disable TLS")
			return nil, nil
		}
	}

	tcfg := &tls.Config{
		MinVersion:            tls.VersionTLS12,
		GetCertificate:        ci.getCert,
		GetClientCertificate:  ci.getClientCert,
		VerifyPeerCertificate: ci.verifyPeerCertificate,
	}

	var certPEM, keyPEM []byte
	var err error
	if autoCerts {
		now := time.Now()
		if time.Unix(ci.autoCertExp.Load(), 0).Before(now) {
			dur, err := time.ParseDuration(ci.cfg.AutoExpireDuration)
			if err != nil {
				dur = DefaultCertExpiration
			}
			ci.autoCertExp.Store(now.Add(DefaultCertExpiration - recreateAutoCertAdvance).Unix())
			certPEM, keyPEM, _, err = createTempTLS(ci.cfg.RSAKeySize, dur)
			if err != nil {
				return nil, err
			}
		}
	} else {
		certPEM, err = os.ReadFile(ci.cfg.Cert)
		if err != nil {
			return nil, err
		}
		keyPEM, err = os.ReadFile(ci.cfg.Key)
		if err != nil {
			return nil, err
		}
	}

	if certPEM != nil {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ci.cert.Store(&cert)
	}

	if !ci.cfg.HasCA() {
		lg.Info("no CA, server will not authenticate clients (connection is still secured)")
		return tcfg, nil
	}

	caPEM, err := os.ReadFile(ci.cfg.CA)
	if err != nil {
		return nil, err
	}

	cas, err := ci.loadCA(caPEM)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ci.ca.Store(cas)

	if ci.cfg.SkipCA {
		tcfg.ClientAuth = tls.RequestClientCert
	} else {
		tcfg.ClientAuth = tls.RequireAnyClientCert
	}

	return tcfg, nil
}

func (ci *CertInfo) buildClientConfig(lg *zap.Logger) (*tls.Config, error) {
	lg = lg.With(zap.String("tls", "client"), zap.Any("cfg", ci.cfg))
	if ci.cfg.AutoCerts {
		lg.Info("specified auto-certs in a client tls config, ignored")
	}

	if !ci.cfg.HasCA() {
		if ci.cfg.SkipCA {
			// still enable TLS without verify server certs
			return &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
			}, nil
		}
		lg.Info("no CA to verify server connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{
		MinVersion:            tls.VersionTLS12,
		GetCertificate:        ci.getCert,
		GetClientCertificate:  ci.getClientCert,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: ci.verifyPeerCertificate,
	}

	certBytes, err := os.ReadFile(ci.cfg.CA)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cas, err := ci.loadCA(certBytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ci.ca.Store(cas)

	if !ci.cfg.HasCert() {
		lg.Info("no certificates, server may reject the connection")
		return tcfg, nil
	}

	cert, err := tls.LoadX509KeyPair(ci.cfg.Cert, ci.cfg.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ci.cert.Store(&cert)

	return tcfg, nil
}
