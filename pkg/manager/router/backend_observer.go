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
	"net"
	"strings"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

type BackendStatus int

func (bs *BackendStatus) ToScore() int {
	return statusScores[*bs]
}

func (bs *BackendStatus) String() string {
	status, ok := statusNames[*bs]
	if !ok {
		return "unknown"
	}
	return status
}

const (
	StatusHealthy BackendStatus = iota
	StatusCannotConnect
	StatusMemoryHigh
	StatusRunSlow
	StatusSchemaOutdated
)

var statusNames = map[BackendStatus]string{
	StatusHealthy:        "healthy",
	StatusCannotConnect:  "cannot connect",
	StatusMemoryHigh:     "memory high",
	StatusRunSlow:        "run slow",
	StatusSchemaOutdated: "schema outdated",
}

var statusScores = map[BackendStatus]int{
	StatusHealthy:        0,
	StatusCannotConnect:  10000000,
	StatusMemoryHigh:     5000,
	StatusRunSlow:        5000,
	StatusSchemaOutdated: 10000000,
}

const (
	healthCheckInterval      = 5 * time.Second
	healthCheckMaxRetries    = 3
	healthCheckRetryInterval = 2 * time.Second
	healthCheckTimeout       = 2 * time.Second
	tombstoneThreshold       = 10 * time.Minute
	topologyPathSuffix       = "/ttl"
)

// HealthCheckConfig contains some configurations for health check.
// Some general configurations of them may be exposed to users in the future.
// We can use shorter durations to speed up unit tests.
type HealthCheckConfig struct {
	healthCheckInterval      time.Duration
	healthCheckMaxRetries    int
	healthCheckRetryInterval time.Duration
	healthCheckTimeout       time.Duration
	tombstoneThreshold       time.Duration
}

func newDefaultHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		healthCheckInterval:      healthCheckInterval,
		healthCheckMaxRetries:    healthCheckMaxRetries,
		healthCheckRetryInterval: healthCheckRetryInterval,
		healthCheckTimeout:       healthCheckTimeout,
		tombstoneThreshold:       tombstoneThreshold,
	}
}

// BackendEventReceiver receives the event of backend status change.
type BackendEventReceiver interface {
	// OnBackendChanged is called when the backend list changes.
	OnBackendChanged(backends map[string]BackendStatus)
}

// BackendTTLInfo stores the ttl info of each backend.
type BackendTTLInfo struct {
	// The TTL time in the topology info.
	ttl []byte
	// Last time the TTL time is refreshed.
	// If the TTL stays unchanged for a long time, the backend might be a tombstone.
	lastUpdate time.Time
}

// BackendObserver refreshes backend list and notifies BackendEventReceiver.
type BackendObserver struct {
	config *HealthCheckConfig
	// The current backend status sent to the receiver.
	curBackendInfo map[string]BackendStatus
	// All the backend info in the topology, including tombstones.
	allBackendInfo map[string]*BackendTTLInfo
	client         *clientv3.Client
	staticAddrs    []string
	eventReceiver  BackendEventReceiver
	wg             waitgroup.WaitGroup
	cancelFunc     context.CancelFunc
}

// InitEtcdClient initializes an etcd client that fetches TiDB instance topology from PD.
func InitEtcdClient(cfg *config.Config) (*clientv3.Client, error) {
	pdAddr := cfg.Proxy.PDAddrs
	if len(pdAddr) == 0 {
		// use tidb server addresses directly
		return nil, nil
	}
	pdEndpoints := strings.Split(pdAddr, ",")
	logConfig := zap.NewProductionConfig()
	logConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	tlsConfig, err := security.CreateClusterTLSConfig(cfg.Security.Cluster.CA, cfg.Security.Cluster.Key, cfg.Security.Cluster.Cert)
	if err != nil {
		return nil, err
	}
	var etcdClient *clientv3.Client
	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:        pdEndpoints,
		TLS:              tlsConfig,
		LogConfig:        &logConfig,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    10 * time.Second,
				Timeout: 3 * time.Second,
			}),
			//grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithBlock(),
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

// StartBackendObserver creates a BackendObserver and starts watching.
func StartBackendObserver(eventReceiver BackendEventReceiver, client *clientv3.Client, config *HealthCheckConfig, staticAddrs []string) (*BackendObserver, error) {
	bo, err := NewBackendObserver(eventReceiver, client, config, staticAddrs)
	if err != nil {
		return nil, err
	}
	bo.Start()
	return bo, nil
}

// NewBackendObserver creates a BackendObserver.
func NewBackendObserver(eventReceiver BackendEventReceiver, client *clientv3.Client, config *HealthCheckConfig, staticAddrs []string) (*BackendObserver, error) {
	if client == nil && len(staticAddrs) == 0 {
		return nil, ErrNoInstanceToSelect
	}
	bo := &BackendObserver{
		config:         config,
		curBackendInfo: make(map[string]BackendStatus),
		allBackendInfo: make(map[string]*BackendTTLInfo),
		client:         client,
		staticAddrs:    staticAddrs,
		eventReceiver:  eventReceiver,
	}
	return bo, nil
}

// Start starts watching.
func (bo *BackendObserver) Start() {
	childCtx, cancelFunc := context.WithCancel(context.Background())
	bo.cancelFunc = cancelFunc
	bo.wg.Run(func() {
		bo.observe(childCtx)
	})
}

func (bo *BackendObserver) observe(ctx context.Context) {
	if bo.client == nil {
		logutil.BgLogger().Info("pd addr is not configured, use static backend instances instead.")
		bo.observeStaticAddrs(ctx)
	} else {
		bo.observeDynamicAddrs(ctx)
	}
}

// If the PD address is not configured, we use static TiDB addresses in the configuration.
// This is only for test. For a production cluster, the PD address should always be configured.
func (bo *BackendObserver) observeStaticAddrs(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-time.After(bo.config.healthCheckInterval):
		case <-ctx.Done():
			return
		}
		backendInfo := make(map[string]BackendStatus)
		for _, addr := range bo.staticAddrs {
			backendInfo[addr] = StatusHealthy
		}
		// The status port is not configured, so we skip checking health now.
		//bo.checkHealth(ctx, backendInfo)
		bo.notifyIfChanged(backendInfo)
	}
}

// If the PD address is configured, we watch the TiDB addresses on the ETCD.
func (bo *BackendObserver) observeDynamicAddrs(ctx context.Context) {
	watchCh := bo.client.Watch(ctx, infosync.TopologyInformationPath, clientv3.WithPrefix())
	// Initialize the backends after watching so that new backends started between watching and refreshing
	// will be fetched immediately.
	bo.refreshBackends(ctx)
	ticker := time.NewTicker(bo.config.healthCheckInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		needRefresh := false
		select {
		case resp, ok := <-watchCh:
			if !ok {
				// The etcdClient is closed.
				return
			}
			if resp.Canceled {
				logutil.Logger(ctx).Warn("watch backend list is canceled, will retry later")
				watchCh = bo.client.Watch(ctx, infosync.TopologyInformationPath, clientv3.WithPrefix())
				time.Sleep(bo.config.healthCheckRetryInterval)
				break
			}
			for _, ev := range resp.Events {
				if strings.HasSuffix(string(ev.Kv.Key), topologyPathSuffix) {
					needRefresh = true
					break
				}
			}
		case <-ticker.C:
			needRefresh = true
		case <-ctx.Done():
			return
		}
		if !needRefresh {
			continue
		}
		ticker.Reset(bo.config.healthCheckInterval)
		bo.refreshBackends(ctx)
	}
}

func (bo *BackendObserver) refreshBackends(ctx context.Context) {
	backendInfo, err := bo.fetchBackendList(ctx)
	if err != nil {
		return
	}
	bo.checkHealth(ctx, backendInfo)
	bo.notifyIfChanged(backendInfo)
}

func (bo *BackendObserver) fetchBackendList(ctx context.Context) (map[string]BackendStatus, error) {
	var response *clientv3.GetResponse
	var err error
	// It's a critical problem if the proxy cannot connect to the server, so we always retry.
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		childCtx, cancel := context.WithTimeout(ctx, bo.config.healthCheckTimeout)
		response, err = bo.client.Get(childCtx, infosync.TopologyInformationPath, clientv3.WithPrefix())
		cancel()
		if err == nil {
			break
		}
		logutil.Logger(ctx).Error("fetch backend list failed, will retry later", zap.Error(err))
		time.Sleep(bo.config.healthCheckRetryInterval)
	}

	curBackendInfo := make(map[string]BackendStatus, len(response.Kvs))
	allBackendInfo := make(map[string]*BackendTTLInfo, len(response.Kvs))
	now := time.Now()
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		if !strings.HasSuffix(key, topologyPathSuffix) {
			continue
		}
		addr := key[len(infosync.TopologyInformationPath)+1 : len(key)-len(topologyPathSuffix)]
		info, ok := bo.allBackendInfo[addr]
		isTombstone := false
		if ok {
			if slices.Compare(info.ttl, kv.Value) != 0 {
				// The TTL is updated this time.
				info.lastUpdate = now
				info.ttl = kv.Value
			} else if info.lastUpdate.Add(bo.config.tombstoneThreshold).Before(now) {
				// The TTL has not been updated for a long time.
				isTombstone = true
			}
		} else {
			// A new backend.
			info = &BackendTTLInfo{
				lastUpdate: now,
				ttl:        kv.Value,
			}
		}
		// After running for a long time, there might be many tombstones because failed TiDB instances
		// don't delete themselves from the Etcd. Checking their health is a waste of time, leading to
		// longer and longer checking interval. So tombstones won't be added to curBackendInfo.
		if !isTombstone {
			curBackendInfo[addr] = StatusHealthy
		}
		allBackendInfo[addr] = info
	}
	bo.allBackendInfo = allBackendInfo
	return curBackendInfo, nil
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backendInfo map[string]BackendStatus) {
	for addr := range backendInfo {
		var err error
		for i := 0; i < bo.config.healthCheckMaxRetries; i++ {
			if ctx.Err() != nil {
				return
			}
			var conn net.Conn
			conn, err = net.DialTimeout("tcp", addr, bo.config.healthCheckTimeout)
			if err == nil {
				_ = conn.Close()
				break
			}
			if i < bo.config.healthCheckMaxRetries-1 {
				time.Sleep(bo.config.healthCheckRetryInterval)
			}
		}
		if err != nil {
			backendInfo[addr] = StatusCannotConnect
		}
	}
}

func (bo *BackendObserver) notifyIfChanged(backendInfo map[string]BackendStatus) {
	updatedBackends := make(map[string]BackendStatus)
	for addr, lastStatus := range bo.curBackendInfo {
		if lastStatus == StatusHealthy {
			if newStatus, ok := backendInfo[addr]; !ok {
				updatedBackends[addr] = StatusCannotConnect
			} else if newStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
			}
		}
	}
	for addr, newStatus := range backendInfo {
		if newStatus == StatusHealthy {
			if lastStatus, ok := bo.curBackendInfo[addr]; !ok || lastStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
			}
		}
	}
	if len(updatedBackends) > 0 {
		bo.eventReceiver.OnBackendChanged(updatedBackends)
	}
	bo.curBackendInfo = backendInfo
}

// Close releases all resources.
func (bo *BackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
	bo.wg.Wait()
}
