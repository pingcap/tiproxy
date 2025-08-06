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
}

// PDFetcher fetches backend list from PD.
type PDFetcher struct {
	tpFetcher TopologyFetcher
	logger    *zap.Logger
	config    *config.HealthCheck
}

func NewPDFetcher(tpFetcher TopologyFetcher, logger *zap.Logger, config *config.HealthCheck) *PDFetcher {
	config.Check()
	return &PDFetcher{
		tpFetcher: tpFetcher,
		logger:    logger,
		config:    config,
	}
}

func (pf *PDFetcher) GetBackendList(ctx context.Context) (map[string]*BackendInfo, error) {
	backends := pf.fetchBackendList(ctx)
	infos := make(map[string]*BackendInfo, len(backends))
	for addr, backend := range backends {
		infos[addr] = &BackendInfo{
			Labels:     backend.Labels,
			IP:         backend.IP,
			StatusPort: backend.StatusPort,
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
		backends[addr] = &BackendInfo{}
	}
	return backends
}
