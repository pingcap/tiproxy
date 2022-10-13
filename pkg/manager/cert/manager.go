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
	"context"
	"crypto/tls"
	"crypto/x509"
	"sync/atomic"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"go.uber.org/zap"
)

const (
	defaultReloadAhead   = 12 * time.Hour
	defaultRetryInterval = 1 * time.Hour
)

// Security configurations don't support dynamically updating now.
type certInfo struct {
	cfg       config.TLSConfig
	tlsConfig atomic.Pointer[tls.Config]
	autoCert  bool
	expTime   time.Time
}

func (ci *certInfo) getTLS() *tls.Config {
	tlsConfig := ci.tlsConfig.Load()
	if tlsConfig != nil {
		return tlsConfig.Clone()
	}
	return nil
}

func (ci *certInfo) setTLS(tlsConfig *tls.Config) {
	ci.tlsConfig.Store(tlsConfig)
	ci.expTime = ci.getExpTime()
}

func (ci *certInfo) getExpTime() time.Time {
	var minTime time.Time
	tlsConfig := ci.tlsConfig.Load()
	if tlsConfig == nil {
		return minTime
	}
	for i := range tlsConfig.Certificates {
		var parsedCert *x509.Certificate
		if tlsConfig.Certificates[i].Leaf == nil {
			var err error
			if parsedCert, err = x509.ParseCertificate(tlsConfig.Certificates[i].Certificate[0]); err != nil {
				continue
			}
		} else {
			parsedCert = tlsConfig.Certificates[i].Leaf
		}
		t := parsedCert.NotAfter
		if minTime.Equal(time.Time{}) {
			minTime = t
		} else if minTime.After(t) {
			minTime = t
		}
	}
	return minTime
}

func (ci *certInfo) needReload(now time.Time, reloadAhead time.Duration) bool {
	if ci.expTime.Equal(time.Time{}) {
		return false
	}
	return now.Add(reloadAhead).After(ci.expTime)
}

// CertManager reloads certs and offers interfaces for fetching TLS configs.
// Currently, all the namespaces share the same certs but there might be per-namespace
// certs in the future.
type CertManager struct {
	serverTLS     certInfo // client / proxyctl -> proxy
	peerTLS       certInfo // proxy -> proxy
	clusterTLS    certInfo // proxy -> pd / tidb status port
	sqlTLS        certInfo // proxy -> tidb sql port
	autoCertDir   string
	cancel        context.CancelFunc
	wg            waitgroup.WaitGroup
	retryInterval atomic.Int64
	reloadAhead   atomic.Int64
	autoExp       atomic.Int64
	cfg           *config.Security
	logger        *zap.Logger
}

// NewCertManager creates a new CertManager.
func NewCertManager() *CertManager {
	cm := &CertManager{}
	cm.SetRetryInterval(defaultRetryInterval)
	cm.SetReloadAhead(defaultReloadAhead)
	cm.SetAutoExp(security.DefaultCertExpiration)
	return cm
}

func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger) error {
	cm.cfg = &cfg.Security
	cm.logger = logger
	cm.autoCertDir = cfg.Workdir
	cm.serverTLS = certInfo{
		cfg:      cfg.Security.ServerTLS,
		autoCert: !cfg.Security.ServerTLS.HasCert() && cfg.Security.ServerTLS.AutoCerts,
	}
	cm.peerTLS = certInfo{
		cfg:      cfg.Security.PeerTLS,
		autoCert: !cfg.Security.PeerTLS.HasCert() && cfg.Security.PeerTLS.AutoCerts,
	}
	cm.clusterTLS = certInfo{
		cfg: cfg.Security.ClusterTLS,
	}
	cm.sqlTLS = certInfo{
		cfg: cfg.Security.SQLTLS,
	}

	if err := cm.load(true); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.wg.Run(func() {
		cm.reloadLoop(ctx)
	})
	cm.cancel = cancel
	return nil
}

func (cm *CertManager) SetRetryInterval(interval time.Duration) {
	cm.retryInterval.Store(int64(interval))
}

func (cm *CertManager) SetReloadAhead(ahead time.Duration) {
	cm.reloadAhead.Store(int64(ahead))
}

func (cm *CertManager) SetAutoExp(exp time.Duration) {
	cm.autoExp.Store(int64(exp))
}

func (cm *CertManager) ServerTLS() *tls.Config {
	return cm.serverTLS.getTLS()
}

func (cm *CertManager) ClusterTLS() *tls.Config {
	return cm.clusterTLS.getTLS()
}

func (cm *CertManager) SQLTLS() *tls.Config {
	return cm.sqlTLS.getTLS()
}

// The proxy is supposed to be always online, so it should reload certs automatically,
// rather than reloading it by restarting the proxy.
// The proxy checks expiration time periodically and reloads certs in advance. If reloading
// fails or the cert is not replaced, it will retry in the next round.
func (cm *CertManager) reloadLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(cm.retryInterval.Load())):
			_ = cm.load(false)
		}
	}
}

func (cm *CertManager) load(init bool) error {
	errs := make([]error, 0, 4)
	now := time.Now()
	if init || cm.serverTLS.needReload(now, time.Duration(cm.reloadAhead.Load())) {
		var err error
		if cm.serverTLS.autoCert {
			if err = security.AutoTLS(cm.logger, &cm.serverTLS.cfg, false, cm.autoCertDir, "server",
				cm.cfg.RSAKeySize, time.Duration(cm.autoExp.Load())); err != nil {
				cm.logger.Error("creating server certs failed", zap.Error(err))
				errs = append(errs, err)
			}
		}
		if err == nil {
			var tlsConfig *tls.Config
			if tlsConfig, err = security.BuildServerTLSConfig(cm.logger, cm.serverTLS.cfg); err != nil {
				cm.logger.Error("loading server certs failed", zap.Error(err))
				errs = append(errs, err)
			} else {
				cm.serverTLS.setTLS(tlsConfig)
			}
		}
	}

	// Peer tls is only used in creating etcd config and doesn't need rotation.
	if init {
		if cm.peerTLS.autoCert {
			if err := security.AutoTLS(cm.logger, &cm.peerTLS.cfg, true, cm.autoCertDir, "peer",
				cm.cfg.RSAKeySize, time.Duration(cm.autoExp.Load())); err != nil {
				cm.logger.Error("creating peer certs failed", zap.Error(err))
				errs = append(errs, err)
			}
		}
	}

	if init || cm.sqlTLS.needReload(now, time.Duration(cm.reloadAhead.Load())) {
		if tlsConfig, err := security.BuildClientTLSConfig(cm.logger, cm.sqlTLS.cfg); err != nil {
			cm.logger.Error("loading sql certs failed", zap.Error(err))
			errs = append(errs, err)
		} else {
			cm.sqlTLS.setTLS(tlsConfig)
		}
	}

	if init || cm.clusterTLS.needReload(now, time.Duration(cm.reloadAhead.Load())) {
		if tlsConfig, err := security.BuildClientTLSConfig(cm.logger, cm.clusterTLS.cfg); err != nil {
			cm.logger.Error("loading cluster certs failed", zap.Error(err))
			errs = append(errs, err)
		} else {
			cm.clusterTLS.setTLS(tlsConfig)
		}
	}

	if len(errs) != 0 {
		return errors.Collect(errors.New("loading certs"), errs...)
	}
	return nil
}

func (cm *CertManager) Close() {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()
}
