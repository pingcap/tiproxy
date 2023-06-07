// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/pingcap/TiProxy/pkg/metrics"
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
	serverTLSConfig  atomic.Pointer[tls.Config]
	peerTLS          *security.CertInfo // proxy -> proxy
	peerTLSConfig    atomic.Pointer[tls.Config]
	clusterTLS       *security.CertInfo // proxy -> pd / tidb status port
	clusterTLSConfig atomic.Pointer[tls.Config]
	sqlTLS           *security.CertInfo // proxy -> tidb sql port
	sqlTLSConfig     atomic.Pointer[tls.Config]

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

// Init creates a CertManager and reloads certificates periodically.
// cfgch can be set to nil for the serverless tier because it has no config manager.
func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger, cfgch <-chan *config.Config) error {
	cm.logger = logger
	cm.serverTLS = security.NewCert(true)
	cm.peerTLS = security.NewCert(false)
	cm.clusterTLS = security.NewCert(false)
	cm.sqlTLS = security.NewCert(false)
	cm.setConfig(cfg)
	if err := cm.reload(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.reloadLoop(ctx, cfgch)
	cm.cancel = cancel
	return nil
}

func (cm *CertManager) setConfig(cfg *config.Config) {
	cm.serverTLS.SetConfig(cfg.Security.ServerTLS)
	cm.peerTLS.SetConfig(cfg.Security.PeerTLS)
	cm.clusterTLS.SetConfig(cfg.Security.ClusterTLS)
	cm.sqlTLS.SetConfig(cfg.Security.SQLTLS)
}

func (cm *CertManager) SetRetryInterval(interval time.Duration) {
	cm.retryInterval.Store(int64(interval))
}

func (cm *CertManager) ServerTLS() *tls.Config {
	return cm.serverTLSConfig.Load()
}

func (cm *CertManager) ClusterTLS() *tls.Config {
	return cm.clusterTLSConfig.Load()
}

func (cm *CertManager) PeerTLS() *tls.Config {
	return cm.peerTLSConfig.Load()
}

func (cm *CertManager) SQLTLS() *tls.Config {
	return cm.sqlTLSConfig.Load()
}

// The proxy is supposed to be always online, so it should reload certs automatically,
// rather than reloading it by restarting the proxy.
// The proxy periodically reloads certs. If it fails, we will retry in the next round.
// If configuration changes, it only affects new connections by returning new *tls.Config.
func (cm *CertManager) reloadLoop(ctx context.Context, cfgch <-chan *config.Config) {
	cm.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case cfg := <-cfgch:
				// If cfgch is closed, it will always come here. But if cfgch is nil, it won't come here.
				if cfg == nil {
					cm.logger.Warn("config channel is closed, stop watching channel")
					cfgch = nil
					break
				}
				cm.setConfig(cfg)
				_ = cm.reload()
			case <-time.After(time.Duration(cm.retryInterval.Load())):
				_ = cm.reload()
			}
		}
	})
}

// If any error happens, we still continue and use the old cert.
func (cm *CertManager) reload() error {
	errs := make([]error, 0, 4)
	if tlsConfig, err := cm.serverTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.serverTLSConfig.Store(tlsConfig)
	}
	if tlsConfig, err := cm.peerTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.peerTLSConfig.Store(tlsConfig)
	}
	if tlsConfig, err := cm.clusterTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.clusterTLSConfig.Store(tlsConfig)
	}
	if tlsConfig, err := cm.sqlTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.sqlTLSConfig.Store(tlsConfig)
	}
	var err error
	if len(errs) > 0 {
		metrics.ServerErrCounter.WithLabelValues("load_cert").Add(float64(len(errs)))
		err = errors.Collect(errors.New("loading certs"), errs...)
		cm.logger.Error("failed to reload some certs", zap.Error(err))
	}
	return err
}

func (cm *CertManager) Close() {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()
}
