// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/retry"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"go.uber.org/zap"
)

var _ BackendFetcher = (*PDFetcher)(nil)
var _ BackendFetcher = (*StaticFetcher)(nil)

// BackendFetcher is an interface to fetch the backend list.
type BackendFetcher interface {
	GetBackendList(context.Context) (map[string]*BackendInfo, error)
}

// TopologyFetcher is an interface to fetch the tidb topology from ETCD.
type TopologyFetcher interface {
	GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error)
	// HasBackendClusters reports whether dynamic PD-backed clusters are configured at all.
	// PDFetcher uses it to preserve the legacy behavior that static backend.instances still work
	// when TiProxy starts without any PD cluster and clusters are added later through the API.
	HasBackendClusters() bool
}

// PDFetcher fetches backend list from PD.
type PDFetcher struct {
	tpFetcher TopologyFetcher
	logger    *zap.Logger
	config    *config.HealthCheck
	static    *StaticFetcher
}

func NewPDFetcher(tpFetcher TopologyFetcher, staticAddrs []string, logger *zap.Logger, config *config.HealthCheck) *PDFetcher {
	config.Check()
	return &PDFetcher{
		tpFetcher: tpFetcher,
		logger:    logger,
		config:    config,
		static:    NewStaticFetcher(staticAddrs),
	}
}

func (pf *PDFetcher) GetBackendList(ctx context.Context) (map[string]*BackendInfo, error) {
	// Keep backward compatibility with the legacy static-namespace flow: before any backend cluster
	// is configured, backend.instances must still be routable even though namespace now always sees
	// a non-nil topology fetcher from the cluster manager.
	if !pf.tpFetcher.HasBackendClusters() {
		return pf.static.GetBackendList(ctx)
	}
	backends := pf.fetchBackendList(ctx)
	infos := make(map[string]*BackendInfo, len(backends))
	for key, backend := range backends {
		infos[key] = &BackendInfo{
			Addr:        backend.Addr,
			ClusterName: backend.ClusterName,
			Labels:      backend.Labels,
			IP:          backend.IP,
			StatusPort:  backend.StatusPort,
		}
	}
	return infos, nil
}

func (pf *PDFetcher) fetchBackendList(ctx context.Context) map[string]*infosync.TiDBTopologyInfo {
	var backends map[string]*infosync.TiDBTopologyInfo
	// The jobs of PDFetcher all rely on the topology, so we retry infinitely.
	err := retry.RetryNotify(func() error {
		var err error
		backends, err = pf.tpFetcher.GetTiDBTopology(ctx)
		return err
	}, ctx, pf.config.RetryInterval, retry.InfiniteCnt,
		func(err error, duration time.Duration) {
			// Ignore errors when TiProxy shuts down.
			if ctx.Err() != nil {
				return
			}
			pf.logger.Error("fetch backend list failed, retrying", zap.Error(err))
			metrics.ServerErrCounter.WithLabelValues("fetchBackendList").Inc()
		}, 10)

	// Must be cancelled if err != nil, we do not log errors.
	if err != nil {
		return nil
	}
	return backends
}

// StaticFetcher uses configured static addrs. This is only used for testing.
type StaticFetcher struct {
	backends map[string]*BackendInfo
}

func NewStaticFetcher(staticAddrs []string) *StaticFetcher {
	return &StaticFetcher{
		backends: backendListToMap(staticAddrs),
	}
}

func (sf *StaticFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	return sf.backends, nil
}

func backendListToMap(addrs []string) map[string]*BackendInfo {
	backends := make(map[string]*BackendInfo, len(addrs))
	for _, addr := range addrs {
		backends[addr] = &BackendInfo{Addr: addr}
	}
	return backends
}
