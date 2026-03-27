// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Cluster is the cluster-scoped container for one backend PD cluster.
type Cluster struct {
	cfg        config.BackendCluster
	etcdCli    *clientv3.Client
	infoSyncer *infosync.InfoSyncer
	metrics    *metricsreader.ClusterReader
}

func (c *Cluster) Config() config.BackendCluster {
	return c.cfg
}

func (c *Cluster) EtcdClient() *clientv3.Client {
	return c.etcdCli
}

func (c *Cluster) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	return c.infoSyncer.GetTiDBTopology(ctx)
}

func (c *Cluster) GetPromInfo(ctx context.Context) (*infosync.PrometheusInfo, error) {
	return c.infoSyncer.GetPromInfo(ctx)
}

type Manager struct {
	lg         *zap.Logger
	clusterTLS func() *tls.Config
	cfgGetter  config.ConfigGetter

	wg      waitgroup.WaitGroup
	cancel  context.CancelFunc
	metrics *MetricsQuerier

	mu struct {
		sync.RWMutex
		clusters map[string]*Cluster
	}
}

func NewManager(lg *zap.Logger, clusterTLS func() *tls.Config) *Manager {
	mgr := &Manager{
		lg:         lg,
		clusterTLS: clusterTLS,
	}
	mgr.mu.clusters = make(map[string]*Cluster)
	mgr.metrics = NewMetricsQuerier(mgr)
	return mgr
}

func (m *Manager) Start(ctx context.Context, cfgGetter config.ConfigGetter, cfgCh <-chan *config.Config) error {
	m.cfgGetter = cfgGetter
	if err := m.syncClusters(ctx, cfgGetter.GetConfig()); err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.Run(func() {
		m.watchConfig(childCtx, cfgCh)
	}, m.lg)
	return nil
}

func (m *Manager) watchConfig(ctx context.Context, cfgCh <-chan *config.Config) {
	if cfgCh == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case cfg, ok := <-cfgCh:
			if !ok {
				m.lg.Warn("config channel is closed, stop watching backend clusters")
				return
			}
			if cfg == nil {
				continue
			}
			if err := m.syncClusters(ctx, cfg); err != nil {
				m.lg.Error("sync backend clusters failed", zap.Error(err))
			}
		}
	}
}

func (m *Manager) syncClusters(ctx context.Context, cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	desiredClusters := cfg.GetBackendClusters()
	desiredMap := make(map[string]config.BackendCluster, len(desiredClusters))
	for _, cluster := range desiredClusters {
		desiredMap[cluster.Name] = cluster
	}

	m.mu.Lock()
	oldClusters := m.mu.clusters
	newClusters := make(map[string]*Cluster, len(desiredClusters))
	closeList := make([]*Cluster, 0, len(oldClusters))

	for _, clusterCfg := range desiredClusters {
		oldCluster, ok := oldClusters[clusterCfg.Name]
		if ok && clusterReusable(oldCluster, clusterCfg) {
			newClusters[clusterCfg.Name] = oldCluster
			delete(oldClusters, clusterCfg.Name)
			continue
		}

		cluster, err := m.buildCluster(ctx, cfg, clusterCfg)
		if err != nil {
			if ok {
				m.lg.Warn("failed to update backend cluster, keep the old one",
					zap.String("cluster", clusterCfg.Name), zap.Error(err))
				newClusters[clusterCfg.Name] = oldCluster
				delete(oldClusters, clusterCfg.Name)
				continue
			}
			m.lg.Error("failed to add backend cluster",
				zap.String("cluster", clusterCfg.Name), zap.Error(err))
			continue
		}
		newClusters[clusterCfg.Name] = cluster
		if ok {
			closeList = append(closeList, oldCluster)
			delete(oldClusters, clusterCfg.Name)
			m.lg.Info("updated backend cluster",
				zap.String("cluster", clusterCfg.Name), zap.String("pd_addrs", clusterCfg.PDAddrs))
		} else {
			m.lg.Info("added backend cluster",
				zap.String("cluster", clusterCfg.Name), zap.String("pd_addrs", clusterCfg.PDAddrs))
		}
	}

	for name, cluster := range oldClusters {
		if _, ok := desiredMap[name]; ok {
			continue
		}
		closeList = append(closeList, cluster)
		m.lg.Info("removed backend cluster",
			zap.String("cluster", name), zap.String("pd_addrs", cluster.cfg.PDAddrs))
	}

	m.mu.clusters = newClusters
	m.mu.Unlock()

	for _, cluster := range closeList {
		if err := m.closeCluster(cluster); err != nil {
			m.lg.Warn("close backend cluster failed",
				zap.String("cluster", cluster.cfg.Name), zap.Error(err))
		}
	}
	return nil
}

func normalizeCluster(cluster config.BackendCluster) config.BackendCluster {
	cluster.Name = strings.TrimSpace(cluster.Name)
	cluster.PDAddrs = strings.TrimSpace(cluster.PDAddrs)
	return cluster
}

func clusterReusable(cluster *Cluster, cfg config.BackendCluster) bool {
	if cluster == nil {
		return false
	}
	left := normalizeCluster(cluster.cfg)
	right := normalizeCluster(cfg)
	return left.Name == right.Name &&
		left.PDAddrs == right.PDAddrs &&
		slices.Equal(left.NSServers, right.NSServers)
}

func (m *Manager) buildCluster(ctx context.Context, cfg *config.Config, clusterCfg config.BackendCluster) (*Cluster, error) {
	clusterCfg = normalizeCluster(clusterCfg)
	etcdCli, err := etcd.InitEtcdClientWithAddrs(
		m.lg.With(zap.String("cluster", clusterCfg.Name)).Named("etcd"),
		clusterCfg.PDAddrs,
		m.clusterTLS(),
	)
	if err != nil {
		return nil, err
	}

	infoSyncer := infosync.NewInfoSyncer(m.lg.With(zap.String("cluster", clusterCfg.Name)).Named("infosync"), etcdCli)
	if err := infoSyncer.Init(ctx, cfg); err != nil {
		if closeErr := etcdCli.Close(); closeErr != nil {
			m.lg.Warn("close cluster etcd client failed after infosync init error",
				zap.String("cluster", clusterCfg.Name), zap.Error(closeErr))
		}
		return nil, err
	}

	cluster := &Cluster{
		cfg:        clusterCfg,
		etcdCli:    etcdCli,
		infoSyncer: infoSyncer,
	}
	cluster.metrics = metricsreader.NewClusterReader(
		m.lg.With(zap.String("cluster", clusterCfg.Name)).Named("metrics"),
		clusterCfg.Name,
		cluster,
		cluster,
		http.NewHTTPClient(m.clusterTLS),
		etcdCli,
		config.NewDefaultHealthCheckConfig(),
		m.cfgGetter,
	)
	for key, query := range m.metrics.snapshot() {
		cluster.metrics.AddQueryExpr(key, query.expr, query.rule)
	}
	if err := cluster.metrics.Start(ctx); err != nil {
		_ = infoSyncer.Close()
		if closeErr := etcdCli.Close(); closeErr != nil {
			m.lg.Warn("close cluster etcd client failed after metrics init error",
				zap.String("cluster", clusterCfg.Name), zap.Error(closeErr))
		}
		return nil, err
	}

	return cluster, nil
}

func (m *Manager) closeCluster(cluster *Cluster) error {
	if cluster == nil {
		return nil
	}
	errs := make([]error, 0, 2)
	if cluster.metrics != nil {
		cluster.metrics.Close()
	}
	if cluster.infoSyncer != nil {
		errs = append(errs, cluster.infoSyncer.Close())
	}
	if cluster.etcdCli != nil {
		errs = append(errs, cluster.etcdCli.Close())
	}
	return errors.Collect(errors.New("close backend cluster"), errs...)
}

func (m *Manager) Snapshot() map[string]*Cluster {
	m.mu.RLock()
	snapshot := make(map[string]*Cluster, len(m.mu.clusters))
	maps.Copy(snapshot, m.mu.clusters)
	m.mu.RUnlock()
	return snapshot
}

func (m *Manager) HasBackendClusters() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.mu.clusters) > 0
}

func (m *Manager) MetricsQuerier() *MetricsQuerier {
	return m.metrics
}

// PrimaryCluster returns the only configured cluster when the cluster count is exactly one.
// It exists for features that are only well-defined in the single-cluster case, such as VIP.
func (m *Manager) PrimaryCluster() *Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.mu.clusters) != 1 {
		return nil
	}
	for _, cluster := range m.mu.clusters {
		return cluster
	}
	return nil
}

func (m *Manager) PreClose() {
	for _, cluster := range m.Snapshot() {
		if cluster == nil || cluster.metrics == nil {
			continue
		}
		cluster.metrics.PreClose()
	}
}

func (m *Manager) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	clusters := m.Snapshot()
	merged := make(map[string]*infosync.TiDBTopologyInfo, 128)
	errs := make([]error, 0, len(clusters))
	for clusterName, cluster := range clusters {
		infos, err := cluster.GetTiDBTopology(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, info := range infos {
			cloned := *info
			backendID := backendID(clusterName, cloned.Addr)
			if oldInfo, ok := merged[backendID]; ok {
				m.lg.Warn("duplicate backend in cluster, keep the first one",
					zap.String("backend_id", backendID),
					zap.String("addr", cloned.Addr),
					zap.String("cluster", clusterName),
					zap.String("first_cluster", oldInfo.ClusterName))
				continue
			}
			cloned.Labels = info.Labels
			cloned.ClusterName = clusterName
			merged[backendID] = &cloned
		}
	}
	if len(merged) == 0 && len(errs) > 0 {
		return nil, errors.Collect(errors.New("fetch from backend clusters"), errs...)
	}
	return merged, nil
}

func (m *Manager) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	m.mu.Lock()
	clusters := m.mu.clusters
	m.mu.clusters = make(map[string]*Cluster)
	m.mu.Unlock()

	errs := make([]error, 0, len(clusters))
	for _, cluster := range clusters {
		if err := m.closeCluster(cluster); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Collect(errors.New("close backend cluster manager"), errs...)
}
