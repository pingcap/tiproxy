// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package security

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

const (
	// Recreate the auto certs one hour before it expires.
	// It should be longer than defaultRetryInterval.
	recreateAutoCertAdvance = 24 * time.Hour
)

var emptyCert = new(tls.Certificate)

type CertInfo struct {
	cfg         atomic.Pointer[config.TLSConfig]
	ca          atomic.Pointer[x509.CertPool]
	cert        atomic.Pointer[tls.Certificate]
	autoCertExp atomic.Int64
	server      bool
}

func NewCert(server bool) *CertInfo {
	return &CertInfo{
		server: server,
	}
}

func (ci *CertInfo) Reload(lg *zap.Logger) (tlsConfig *tls.Config, err error) {
	// Some methods to rotate server config:
	// - For certs: customize GetCertificate / GetConfigForClient.
	// - For CA: customize ClientAuth + VerifyPeerCertificate / GetConfigForClient
	// Some methods to rotate client config:
	// - For certs: customize GetClientCertificate
	// - For CA: customize InsecureSkipVerify + VerifyPeerCertificate
	prevExpireTime := ci.getExpireTime()
	if ci.server {
		tlsConfig, err = ci.buildServerConfig(lg)
	} else {
		tlsConfig, err = ci.buildClientConfig(lg)
	}
	if err == nil {
		curExpireTime := ci.getExpireTime()
		if prevExpireTime != curExpireTime {
			lg.Info("update cert expiration", zap.Time("prev", prevExpireTime), zap.Time("cur", curExpireTime), zap.Any("cfg", ci.cfg.Load()))
		}
	}
	return tlsConfig, err
}

func (ci *CertInfo) SetConfig(cfg config.TLSConfig) {
	ci.cfg.Store(&cfg)
}

func (ci *CertInfo) getCert(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return ci.cert.Load(), nil
}

func (ci *CertInfo) getClientCert(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert := ci.cert.Load()
	if cert == nil {
		// GetClientCertificate must return a non-nil Certificate.
		return emptyCert, nil
	}
	return cert, nil
}

func (ci *CertInfo) verifyCA(rawCerts [][]byte) error {
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

	cas := ci.ca.Load()
	if cas == nil {
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

func verifyCommonName(allowedCN []string, verifiedChains [][]*x509.Certificate) error {
	if len(allowedCN) == 0 {
		return nil
	}
	checkCN := make(map[string]struct{})
	for _, cn := range allowedCN {
		cn = strings.TrimSpace(cn)
		checkCN[cn] = struct{}{}
	}
	for _, chain := range verifiedChains {
		if len(chain) != 0 {
			if _, match := checkCN[chain[0].Subject.CommonName]; match {
				return nil
			}
		}
	}
	return errors.Errorf("peer certificate authentication failed. The Common Name from the peer certificate was not found in the configuration cert-allowed-cn with value: %v", allowedCN)
}

func (ci *CertInfo) buildServerConfig(lg *zap.Logger) (*tls.Config, error) {
	lg = lg.With(zap.String("tls", "server"), zap.Any("cfg", ci.cfg.Load()))
	autoCerts := false
	cfg := ci.cfg.Load()
	if !cfg.HasCert() {
		if cfg.AutoCerts {
			autoCerts = true
		} else {
			lg.Debug("require certificates to secure clients connections, disable TLS")
			return nil, nil
		}
	}

	tcfg := &tls.Config{
		MinVersion:           GetMinTLSVer(cfg.MinTLSVersion, lg),
		GetCertificate:       ci.getCert,
		GetClientCertificate: ci.getClientCert,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if err := ci.verifyCA(rawCerts); err != nil {
				return err
			}
			return verifyCommonName(cfg.CertAllowedCN, verifiedChains)
		},
	}

	var certPEM, keyPEM []byte
	var err error
	if autoCerts {
		now := time.Now()
		if time.Unix(ci.autoCertExp.Load(), 0).Before(now) {
			dur, err := time.ParseDuration(cfg.AutoExpireDuration)
			if err != nil {
				dur = DefaultCertExpiration
			}
			ci.autoCertExp.Store(now.Add(DefaultCertExpiration - recreateAutoCertAdvance).Unix())
			certPEM, keyPEM, _, err = createTempTLS(cfg.RSAKeySize, dur, "")
			if err != nil {
				return nil, err
			}
		}
	} else {
		certPEM, err = os.ReadFile(cfg.Cert)
		if err != nil {
			return nil, err
		}
		keyPEM, err = os.ReadFile(cfg.Key)
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

	if !cfg.HasCA() {
		lg.Debug("no CA, server will not authenticate clients (connection is still secured)")
		return tcfg, nil
	}

	caPEM, err := os.ReadFile(cfg.CA)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("failed to append ca certs")
	}
	ci.ca.Store(certPool)

	// RequireAndVerifyClientCert requires ClientCAs to verify client certificates.
	// But the problem is, the ClientCAs in the returned tls.Config can't be updated after reload,
	// which results in connection failure after CA rotation and cert-allowed-cn is set.
	tcfg.ClientCAs = certPool

	if len(cfg.CertAllowedCN) > 0 {
		tcfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else if cfg.SkipCA {
		tcfg.ClientAuth = tls.RequestClientCert
	} else {
		tcfg.ClientAuth = tls.RequireAnyClientCert
	}

	return tcfg, nil
}

func (ci *CertInfo) buildClientConfig(lg *zap.Logger) (*tls.Config, error) {
	lg = lg.With(zap.String("tls", "client"), zap.Any("cfg", ci.cfg.Load()))
	cfg := ci.cfg.Load()
	if cfg.AutoCerts {
		lg.Warn("specified auto-certs in a client tls config, ignored")
	}

	if !cfg.HasCA() {
		if cfg.SkipCA {
			// still enable TLS without verify server certs
			return &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         GetMinTLSVer(cfg.MinTLSVersion, lg),
			}, nil
		}
		lg.Debug("no CA to verify server connections, disable TLS")
		return nil, nil
	}

	tcfg := &tls.Config{
		MinVersion:           GetMinTLSVer(cfg.MinTLSVersion, lg),
		GetCertificate:       ci.getCert,
		GetClientCertificate: ci.getClientCert,
		InsecureSkipVerify:   true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return ci.verifyCA(rawCerts)
		},
	}

	caPEM, err := os.ReadFile(cfg.CA)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("failed to append ca certs")
	}
	ci.ca.Store(certPool)
	tcfg.RootCAs = certPool

	if !cfg.HasCert() {
		lg.Debug("no certificates, server may reject the connection")
		return tcfg, nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ci.cert.Store(&cert)

	return tcfg, nil
}

func (ci *CertInfo) getExpireTime() time.Time {
	cert := ci.cert.Load()
	if cert == nil {
		return time.Time{}
	}
	cp, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return time.Time{}
	}
	return cp.NotAfter
}
