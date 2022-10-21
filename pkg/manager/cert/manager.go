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
	"sync/atomic"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"go.uber.org/zap"
)

const (
	defaultRetryInterval    = 1 * time.Hour
	defaultAutoCertInterval = 30 * 24 * time.Hour
)

// CertManager reloads certs and offers interfaces for fetching TLS configs.
// Currently, all the namespaces share the same certs but there might be per-namespace
// certs in the future.
type CertManager struct {
	serverTLS        certInfo // client / proxyctl -> proxy
	peerTLS          certInfo // proxy -> proxy
	clusterTLS       certInfo // proxy -> pd / tidb status port
	sqlTLS           certInfo // proxy -> tidb sql port
	autoCertDir      string
	cancel           context.CancelFunc
	wg               waitgroup.WaitGroup
	retryInterval    atomic.Int64
	autoCertInterval atomic.Int64
	cfg              *config.Security
	logger           *zap.Logger
}

// NewCertManager creates a new CertManager.
func NewCertManager() *CertManager {
	cm := &CertManager{}
	cm.SetRetryInterval(defaultRetryInterval)
	cm.SetAutoCertInterval(defaultAutoCertInterval)
	return cm
}

func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger) error {
	cm.cfg = &cfg.Security
	cm.logger = logger
	cm.autoCertDir = cfg.Workdir
	cm.serverTLS = certInfo{
		cfg:      cfg.Security.ServerTLS,
		autoCert: !cfg.Security.ServerTLS.HasCert() && cfg.Security.ServerTLS.AutoCerts,
		isServer: true,
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

	if err := cm.load(); err != nil {
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

func (cm *CertManager) SetAutoCertInterval(interval time.Duration) {
	cm.autoCertInterval.Store(int64(interval))
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
			_ = cm.load()
		}
	}
}

func (cm *CertManager) load() error {
	errs := make([]error, 0, 4)
	now := time.Now()
	var err error
	needReloadServer := false
	if cm.serverTLS.autoCert && cm.serverTLS.needRecreateCert(now) {
		if err = security.AutoTLS(cm.logger, &cm.serverTLS.cfg, false, cm.autoCertDir, "server",
			cm.cfg.RSAKeySize); err != nil {
			cm.logger.Error("creating server certs failed", zap.Error(err))
			errs = append(errs, err)
		} else {
			needReloadServer = true
		}
	} else if !cm.serverTLS.autoCert {
		needReloadServer = true
	}
	if needReloadServer {
		if err = cm.serverTLS.buildTLSConfig(cm.logger); err != nil {
			cm.logger.Error("loading server certs failed", zap.Error(err))
			errs = append(errs, err)
		} else {
			cm.serverTLS.setAutoCertExp(now.Add(time.Duration(cm.autoCertInterval.Load())))
		}
	}

	if cm.peerTLS.autoCert && cm.peerTLS.needRecreateCert(now) {
		if err := security.AutoTLS(cm.logger, &cm.peerTLS.cfg, true, cm.autoCertDir, "peer",
			cm.cfg.RSAKeySize); err != nil {
			cm.logger.Error("creating peer certs failed", zap.Error(err))
			errs = append(errs, err)
		} else {
			cm.peerTLS.setAutoCertExp(now.Add(time.Duration(cm.autoCertInterval.Load())))
		}
	}

	if err = cm.sqlTLS.buildTLSConfig(cm.logger); err != nil {
		cm.logger.Error("loading sql certs failed", zap.Error(err))
		errs = append(errs, err)
	}

	if err = cm.clusterTLS.buildTLSConfig(cm.logger); err != nil {
		cm.logger.Error("loading cluster certs failed", zap.Error(err))
		errs = append(errs, err)
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
