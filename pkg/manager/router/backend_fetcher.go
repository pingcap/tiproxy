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

package router

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	"github.com/pingcap/tidb/domain/infosync"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// BackendFetcher is an interface to fetch the backend list.
type BackendFetcher interface {
	GetBackendList(context.Context) map[string]*BackendInfo
}

// InitEtcdClient initializes an etcd client that fetches TiDB instance topology from PD.
func InitEtcdClient(logger *zap.Logger, cfg *config.Config, certMgr *cert.CertManager) (*clientv3.Client, error) {
	pdAddr := cfg.Proxy.PDAddrs
	if len(pdAddr) == 0 {
		// use tidb server addresses directly
		return nil, nil
	}
	pdEndpoints := strings.Split(pdAddr, ",")
	logger.Info("connect PD servers", zap.Strings("addrs", pdEndpoints))
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:        pdEndpoints,
		TLS:              certMgr.ClusterTLS(),
		Logger:           logger.Named("etcdcli"),
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    10 * time.Second,
				Timeout: 3 * time.Second,
			}),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	return etcdClient, errors.Wrapf(err, "init etcd client failed")
}

type pdBackendInfo struct {
	// The TopologyInfo received from the /info path.
	*infosync.TopologyInfo
	// The TTL time in the topology info.
	ttl []byte
	// Last time the TTL time is refreshed.
	// If the TTL stays unchanged for a long time, the backend might be a tombstone.
	lastUpdate time.Time
}

// PDFetcher fetches backend list from PD.
type PDFetcher struct {
	// All the backend info in the topology, including tombstones.
	backendInfo map[string]*pdBackendInfo
	client      *clientv3.Client
	logger      *zap.Logger
	config      *HealthCheckConfig
}

func NewPDFetcher(client *clientv3.Client, logger *zap.Logger, config *HealthCheckConfig) *PDFetcher {
	return &PDFetcher{
		backendInfo: make(map[string]*pdBackendInfo),
		client:      client,
		logger:      logger,
		config:      config,
	}
}

func (pf *PDFetcher) GetBackendList(ctx context.Context) map[string]*BackendInfo {
	pf.fetchBackendList(ctx)
	backendInfo := pf.filterTombstoneBackends()
	return backendInfo
}

func (pf *PDFetcher) fetchBackendList(ctx context.Context) {
	// We query the etcd periodically instead of watching events from etcd because:
	// - When a new backend starts and writes etcd, the HTTP status port is not ready yet.
	// - When a backend shuts down, it doesn't delete itself from the etcd.
	var response *clientv3.GetResponse
	var err error
	// It's a critical problem if the proxy cannot connect to the server, so we always retry.
	for ctx.Err() == nil {
		// In case there are too many tombstone backends, the query would be slow, so no need to set a timeout here.
		if response, err = pf.client.Get(ctx, infosync.TopologyInformationPath, clientv3.WithPrefix()); err == nil {
			break
		}
		pf.logger.Error("fetch backend list failed, will retry later", zap.Error(err))
		time.Sleep(pf.config.healthCheckRetryInterval)
	}

	if ctx.Err() != nil {
		return
	}

	allBackendInfo := make(map[string]*pdBackendInfo, len(response.Kvs))
	now := time.Now()
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, ttlPathSuffix) {
			addr := key[len(infosync.TopologyInformationPath)+1 : len(key)-len(ttlPathSuffix)]
			info, ok := allBackendInfo[addr]
			if !ok {
				info, ok = pf.backendInfo[addr]
			}
			if ok {
				if slices.Compare(info.ttl, kv.Value) != 0 {
					// The TTL is updated this time.
					info.lastUpdate = now
					info.ttl = kv.Value
				}
			} else {
				// A new backend.
				info = &pdBackendInfo{
					lastUpdate: now,
					ttl:        kv.Value,
				}
			}
			allBackendInfo[addr] = info
		} else if strings.HasSuffix(key, infoPathSuffix) {
			addr := key[len(infosync.TopologyInformationPath)+1 : len(key)-len(infoPathSuffix)]
			// A backend may restart with a same address but a different status port in a short time, so
			// we still need to marshal and update the topology even if the address exists in the map.
			var topo *infosync.TopologyInfo
			if err = json.Unmarshal(kv.Value, &topo); err != nil {
				pf.logger.Error("unmarshal topology info failed", zap.String("key", string(kv.Key)),
					zap.ByteString("value", kv.Value), zap.Error(err))
				continue
			}
			info, ok := allBackendInfo[addr]
			if !ok {
				info, ok = pf.backendInfo[addr]
			}
			if ok {
				info.TopologyInfo = topo
			} else {
				info = &pdBackendInfo{
					TopologyInfo: topo,
				}
			}
			allBackendInfo[addr] = info
		}
	}
	pf.backendInfo = allBackendInfo
}

func (pf *PDFetcher) filterTombstoneBackends() map[string]*BackendInfo {
	now := time.Now()
	aliveBackends := make(map[string]*BackendInfo, len(pf.backendInfo))
	for addr, info := range pf.backendInfo {
		if info.TopologyInfo == nil || info.ttl == nil {
			continue
		}
		// After running for a long time, there might be many tombstones because failed TiDB instances
		// don't delete themselves from the Etcd. Checking their health is a waste of time, leading to
		// longer and longer checking interval. So tombstones won't be added to aliveBackends.
		if info.lastUpdate.Add(pf.config.tombstoneThreshold).Before(now) {
			continue
		}
		aliveBackends[addr] = &BackendInfo{
			IP:         info.IP,
			StatusPort: info.StatusPort,
		}
	}
	return aliveBackends
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

func (sf *StaticFetcher) GetBackendList(context.Context) map[string]*BackendInfo {
	return sf.backends
}

func backendListToMap(addrs []string) map[string]*BackendInfo {
	backends := make(map[string]*BackendInfo, len(addrs))
	for _, addr := range addrs {
		backends[addr] = &BackendInfo{}
	}
	return backends
}
