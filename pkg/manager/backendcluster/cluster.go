// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Cluster is the cluster-scoped container for one backend PD cluster.
type Cluster struct {
	cfg        config.BackendCluster
	etcdCli    *clientv3.Client
	infoSyncer *infosync.InfoSyncer
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

func (c *Cluster) Close() error {
	errs := []error{
		c.infoSyncer.Close(),
		c.etcdCli.Close(),
	}
	return errors.Collect(errors.New("close backend cluster"), errs...)
}

// NewCluster creates a new Cluster instance based on the given configuration.
func NewCluster(ctx context.Context, cfg *config.Config, clusterCfg config.BackendCluster, clusterTLS func() *tls.Config, logger *zap.Logger) (*Cluster, error) {
	clusterCfg = normalizeCluster(clusterCfg)
	etcdCli, err := etcd.InitEtcdClientWithAddrs(
		logger.With(zap.String("cluster", clusterCfg.Name)).Named("etcd"),
		clusterCfg.PDAddrs,
		clusterTLS(),
	)
	if err != nil {
		return nil, err
	}

	infoSyncer := infosync.NewInfoSyncer(logger.With(zap.String("cluster", clusterCfg.Name)).Named("infosync"), etcdCli)
	if err := infoSyncer.Init(ctx, cfg); err != nil {
		if closeErr := etcdCli.Close(); closeErr != nil {
			logger.Warn("close cluster etcd client failed after infosync init error",
				zap.String("cluster", clusterCfg.Name), zap.Error(closeErr))
		}
		return nil, err
	}

	return &Cluster{
		cfg:        clusterCfg,
		etcdCli:    etcdCli,
		infoSyncer: infoSyncer,
	}, nil
}
