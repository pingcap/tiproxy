// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cert

import (
	"context"
	"crypto/tls"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/security"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

const (
	defaultRetryInterval = 5 * time.Minute
)

// CertManager reloads certs and offers interfaces for fetching TLS configs.
// Currently, all the namespaces share the same certs but there might be per-namespace
// certs in the future.
type CertManager struct {
	serverSQLTLS        *security.CertInfo // client -> proxy
	serverSQLTLSConfig  atomic.Pointer[tls.Config]
	serverHTTPTLS       *security.CertInfo // proxyctl -> proxy
	serverHTTPTLSConfig atomic.Pointer[tls.Config]
	clusterTLS          *security.CertInfo // proxy -> pd / tidb status port
	clusterTLSConfig    atomic.Pointer[tls.Config]
	sqlTLS              *security.CertInfo // proxy -> tidb sql port
	sqlTLSConfig        atomic.Pointer[tls.Config]

	cancel        context.CancelFunc
	wg            waitgroup.WaitGroup
	retryInterval atomic.Int64
	logger        *zap.Logger
	watcher       *fsnotify.Watcher
}

// NewCertManager creates a new CertManager.
func NewCertManager() *CertManager {
	cm := &CertManager{}
	cm.SetRetryInterval(defaultRetryInterval)
	return cm
}

// Init creates a CertManager, reloads certificates periodically and watches cert files via fsnotify.
// cfgch can be set to nil for the serverless tier because it has no config manager.
func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger, cfgch <-chan *config.Config) error {
	cm.logger = logger
	cm.serverSQLTLS = security.NewCert(true)
	cm.serverHTTPTLS = security.NewCert(true)
	cm.clusterTLS = security.NewCert(false)
	cm.sqlTLS = security.NewCert(false)
	cm.setConfig(cfg)
	if err := cm.reload(); err != nil {
		return err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	cm.watcher = watcher
	if err = cm.resetCertFileWatches(collectCertWatchFiles(cfg)); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm.reloadLoop(ctx, cfgch)
	cm.cancel = cancel
	return nil
}

func (cm *CertManager) setConfig(cfg *config.Config) {
	cm.serverSQLTLS.SetConfig(cfg.Security.ServerSQLTLS)
	cm.serverHTTPTLS.SetConfig(cfg.Security.ServerHTTPTLS)
	cm.clusterTLS.SetConfig(cfg.Security.ClusterTLS)
	cm.sqlTLS.SetConfig(cfg.Security.SQLTLS)
}

func (cm *CertManager) SetRetryInterval(interval time.Duration) {
	cm.retryInterval.Store(int64(interval))
}

func (cm *CertManager) ServerSQLTLS() *tls.Config {
	return cm.serverSQLTLSConfig.Load()
}

func (cm *CertManager) ServerHTTPTLS() *tls.Config {
	return cm.serverHTTPTLSConfig.Load()
}

func (cm *CertManager) ClusterTLS() *tls.Config {
	return cm.clusterTLSConfig.Load()
}

func (cm *CertManager) SQLTLS() *tls.Config {
	return cm.sqlTLSConfig.Load()
}

// The proxy is supposed to be always online, so it should reload certs automatically,
// rather than reloading it by restarting the proxy.
// The proxy reloads certs on configuration changes, cert file changes, and periodic retries.
// If configuration changes, it only affects new connections by returning new *tls.Config.
func (cm *CertManager) reloadLoop(ctx context.Context, cfgch <-chan *config.Config) {
	// Failing to reload certs may cause even more serious problems than TiProxy reboot, so we don't recover panics.
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
				if err := cm.resetCertFileWatches(collectCertWatchFiles(cfg)); err != nil {
					cm.logger.Warn("failed to update cert file watcher", zap.Error(err))
				}
				_ = cm.reload()
			case event, ok := <-cm.watcher.Events:
				if !ok {
					cm.logger.Warn("cert file watcher is closed, stop watching")
					break
				}
				cm.logger.Info("cert files changed, reload certs",
					zap.String("path", event.Name),
					zap.String("op", event.Op.String()))
				if event.Op.Has(fsnotify.Chmod) || event.Op.Has(fsnotify.Remove) {
					cm.logger.Info("cert file should re-add to watcher, because it may mount by Pod Secret", zap.String("path", event.Name))
					if err := cm.watcher.Add(event.Name); err != nil {
						cm.logger.Error("failed to re-add cert file", zap.String("path", event.Name), zap.Error(err))
					}
				}
				_ = cm.reload()
			case err := <-cm.watcher.Errors:
				if err != nil {
					cm.logger.Warn("cert file watcher error, but still reload certs", zap.Error(err))
					_ = cm.reload()
				}
			case <-time.After(time.Duration(cm.retryInterval.Load())):
				_ = cm.reload()
			}
		}
	}, cm.logger)
}

func collectCertWatchFiles(cfg *config.Config) []string {
	files := make(map[string]struct{})
	addCertWatchFiles(files, cfg.Security.ServerSQLTLS)
	addCertWatchFiles(files, cfg.Security.ServerHTTPTLS)
	addCertWatchFiles(files, cfg.Security.ClusterTLS)
	addCertWatchFiles(files, cfg.Security.SQLTLS)
	watchFiles := make([]string, 0, len(files))
	for file := range files {
		watchFiles = append(watchFiles, file)
	}
	return watchFiles
}

func addCertWatchFiles(files map[string]struct{}, tlsCfg config.TLSConfig) {
	if tlsCfg.Cert != "" {
		files[filepath.Clean(tlsCfg.Cert)] = struct{}{}
	}
	if tlsCfg.Key != "" {
		files[filepath.Clean(tlsCfg.Key)] = struct{}{}
	}
	if tlsCfg.CA != "" {
		files[filepath.Clean(tlsCfg.CA)] = struct{}{}
	}
}

func (cm *CertManager) resetCertFileWatches(watchFiles []string) error {
	errs := make([]error, 0)
	for _, file := range cm.watcher.WatchList() {
		if err := cm.watcher.Remove(file); err != nil {
			errs = append(errs, errors.Wrapf(err, "unwatch cert file %s", file))
		}
	}
	for _, file := range watchFiles {
		if err := cm.watcher.Add(file); err != nil {
			errs = append(errs, errors.Wrapf(err, "watch cert file %s", file))
		}
	}
	return errors.Collect(errors.New("update cert file watcher"), errs...)
}

// If any error happens, we still continue and use the old cert.
func (cm *CertManager) reload() error {
	errs := make([]error, 0, 4)
	if tlsConfig, err := cm.serverSQLTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.serverSQLTLSConfig.Store(tlsConfig)
	}
	if tlsConfig, err := cm.serverHTTPTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.serverHTTPTLSConfig.Store(tlsConfig)
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
	if cm.watcher != nil {
		_ = cm.watcher.Close()
	}
}
