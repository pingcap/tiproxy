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

package cert

import (
	"crypto/tls"
	"crypto/x509"
	"sync/atomic"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/security"
	"go.uber.org/zap"
)

var emptyCert = new(tls.Certificate)

// Security configurations don't support dynamically updating now.
type certInfo struct {
	cfg         config.TLSConfig
	tlsConfig   atomic.Pointer[tls.Config]
	autoCert    bool
	autoCertExp time.Time
	isServer    bool
}

func (ci *certInfo) buildTLSConfig(lg *zap.Logger) error {
	builder := security.BuildClientTLSConfig
	if ci.isServer {
		builder = security.BuildServerTLSConfig
	}
	tlsConfig, err := builder(lg, ci.cfg)
	if err == nil {
		tlsConfig = ci.customizeTLSConfig(tlsConfig)
		ci.tlsConfig.Store(tlsConfig)
	}
	return err
}

func (ci *certInfo) getCertificate() *tls.Certificate {
	tlsConfig := ci.tlsConfig.Load()
	if tlsConfig != nil && len(tlsConfig.Certificates) > 0 {
		return &tlsConfig.Certificates[0]
	}
	return nil
}

func (ci *certInfo) getCAs() *x509.CertPool {
	tlsConfig := ci.tlsConfig.Load()
	if tlsConfig != nil {
		if ci.isServer {
			return tlsConfig.ClientCAs
		}
		return tlsConfig.RootCAs
	}
	return nil
}

func (ci *certInfo) verifyPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
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

	latestConfig := ci.tlsConfig.Load()
	t := latestConfig.Time
	if t == nil {
		t = time.Now
	}
	opts := x509.VerifyOptions{
		Roots:         ci.getCAs(),
		CurrentTime:   t(),
		Intermediates: x509.NewCertPool(),
	}
	if ci.isServer {
		opts.KeyUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		opts.DNSName = latestConfig.ServerName
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	_, err := certs[0].Verify(opts)
	return err
}

// Some methods to rotate server config:
// - For certs: customize GetCertificate / GetConfigForClient.
// - For CA: customize ClientAuth + VerifyPeerCertificate / GetConfigForClient
// Some methods to rotate client config:
// - For certs: customize GetClientCertificate
// - For CA: customize InsecureSkipVerify + VerifyPeerCertificate
func (ci *certInfo) customizeTLSConfig(tlsConfig *tls.Config) *tls.Config {
	if tlsConfig == nil {
		return nil
	}
	tlsConfig = tlsConfig.Clone()
	if ci.isServer {
		tlsConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return ci.getCertificate(), nil
		}
		if tlsConfig.ClientAuth >= tls.VerifyClientCertIfGiven {
			tlsConfig.ClientAuth = tls.RequireAnyClientCert
			tlsConfig.VerifyPeerCertificate = ci.verifyPeerCertificate
		}
	} else {
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert := ci.getCertificate()
			if cert == nil {
				// GetClientCertificate must return a non-nil Certificate.
				return emptyCert, nil
			}
			return cert, nil
		}
		if !tlsConfig.InsecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.VerifyPeerCertificate = ci.verifyPeerCertificate
		}
	}
	return tlsConfig
}

func (ci *certInfo) getTLS() *tls.Config {
	return ci.tlsConfig.Load()
}

func (ci *certInfo) setAutoCertExp(exp time.Time) {
	ci.autoCertExp = exp
}

func (ci *certInfo) needRecreateCert(now time.Time) bool {
	if !ci.autoCert {
		return false
	}
	return now.After(ci.autoCertExp)
}
