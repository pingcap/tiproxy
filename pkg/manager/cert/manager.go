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
	defaultRetryInterval = 1 * time.Hour
)

// CertManager reloads certs and offers interfaces for fetching TLS configs.
// Currently, all the namespaces share the same certs but there might be per-namespace
// certs in the future.
type CertManager struct {
	serverTLS        *security.CertInfo // client / proxyctl -> proxy
	serverTLSConfig  *tls.Config
	peerTLS          *security.CertInfo // proxy -> proxy
	peerTLSConfig    *tls.Config
	clusterTLS       *security.CertInfo // proxy -> pd / tidb status port
	clusterTLSConfig *tls.Config
	sqlTLS           *security.CertInfo // proxy -> tidb sql port
	sqlTLSConfig     *tls.Config

	cancel        context.CancelFunc
	wg            waitgroup.WaitGroup
	retryInterval atomic.Int64
	logger        *zap.Logger
}

// NewCertManager creates a new CertManager.
func NewCertManager() *CertManager {
	cm := &CertManager{}
	cm.SetRetryInterval(defaultRetryInterval)
	return cm
}

func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger) error {
	var err error
	cm.logger = logger
	cm.serverTLS, cm.serverTLSConfig, err = security.NewCert(logger, cfg.Security.ServerTLS, true)
	if err != nil {
		return err
	}
	cm.peerTLS, cm.peerTLSConfig, err = security.NewCert(logger, cfg.Security.PeerTLS, false)
	if err != nil {
		return err
	}
	cm.clusterTLS, cm.clusterTLSConfig, err = security.NewCert(logger, cfg.Security.ClusterTLS, false)
	if err != nil {
		return err
	}
	cm.sqlTLS, cm.sqlTLSConfig, err = security.NewCert(logger, cfg.Security.SQLTLS, false)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.reloadLoop(ctx)
	cm.cancel = cancel
	return nil
}

func (cm *CertManager) SetRetryInterval(interval time.Duration) {
	cm.retryInterval.Store(int64(interval))
}

func (cm *CertManager) ServerTLS() *tls.Config {
	return cm.serverTLSConfig
}

func (cm *CertManager) ClusterTLS() *tls.Config {
	return cm.clusterTLSConfig
}

func (cm *CertManager) PeerTLS() *tls.Config {
	return cm.peerTLSConfig
}

func (cm *CertManager) SQLTLS() *tls.Config {
	return cm.sqlTLSConfig
}

// The proxy is supposed to be always online, so it should reload certs automatically,
// rather than reloading it by restarting the proxy.
// The proxy periodically reloads certs. If it fails, we will retry in the next round.
func (cm *CertManager) reloadLoop(ctx context.Context) {
	cm.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(cm.retryInterval.Load())):
				cm.reload()
			}
		}
	})
}

// If any error happens, we still continue and use the old cert.
func (cm *CertManager) reload() {
	errs := make([]error, 0, 4)
	if err := cm.serverTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	}
	if err := cm.peerTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	}
	if err := cm.clusterTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	}
	if err := cm.sqlTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	}
	err := errors.Collect(errors.New("loading certs"), errs...)
	if err != nil {
		cm.logger.Error("failed to reload some certs", zap.Error(err))
	}
}

func (cm *CertManager) Close() {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()
}
