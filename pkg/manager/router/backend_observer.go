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
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/domain/infosync"
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
	StatusCannotConnect:  "down",
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
	healthCheckInterval      = 3 * time.Second
	healthCheckMaxRetries    = 3
	healthCheckRetryInterval = 1 * time.Second
	healthCheckTimeout       = 2 * time.Second
	tombstoneThreshold       = 5 * time.Minute
	ttlPathSuffix            = "/ttl"
	infoPathSuffix           = "/info"
	statusPathSuffix         = "/status"
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

// BackendInfo stores the ttl and status info of each backend.
type BackendInfo struct {
	// The TopologyInfo received from the /info path.
	*infosync.TopologyInfo
	// The TTL time in the topology info.
	ttl []byte
	// Last time the TTL time is refreshed.
	// If the TTL stays unchanged for a long time, the backend might be a tombstone.
	lastUpdate time.Time
}

// BackendObserver refreshes backend list and notifies BackendEventReceiver.
type BackendObserver struct {
	logger *zap.Logger
	config *HealthCheckConfig
	// The current backend status synced to the receiver.
	curBackendInfo map[string]BackendStatus
	// All the backend info in the topology, including tombstones.
	allBackendInfo map[string]*BackendInfo
	client         *clientv3.Client
	httpCli        *http.Client
	httpTLS        bool
	staticAddrs    []string
	eventReceiver  BackendEventReceiver
	wg             waitgroup.WaitGroup
	cancelFunc     context.CancelFunc
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

// StartBackendObserver creates a BackendObserver and starts watching.
func StartBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, client *clientv3.Client, httpCli *http.Client, config *HealthCheckConfig, staticAddrs []string) (*BackendObserver, error) {
	bo, err := NewBackendObserver(logger, eventReceiver, client, httpCli, config, staticAddrs)
	if err != nil {
		return nil, err
	}
	bo.Start()
	return bo, nil
}

// NewBackendObserver creates a BackendObserver.
func NewBackendObserver(logger *zap.Logger, eventReceiver BackendEventReceiver, client *clientv3.Client, httpCli *http.Client,
	config *HealthCheckConfig, staticAddrs []string) (*BackendObserver, error) {
	if httpCli == nil {
		httpCli = http.DefaultClient
	}
	httpTLS := false
	if v, ok := httpCli.Transport.(*http.Transport); ok && v != nil && v.TLSClientConfig != nil {
		httpTLS = true
	}
	bo := &BackendObserver{
		logger:         logger,
		config:         config,
		curBackendInfo: make(map[string]BackendStatus),
		allBackendInfo: make(map[string]*BackendInfo),
		client:         client,
		httpCli:        httpCli,
		httpTLS:        httpTLS,
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
		bo.logger.Info("pd addr is not configured, use static backend instances instead.")
		bo.observeStaticAddrs(ctx)
	} else {
		bo.observeDynamicAddrs(ctx)
	}
}

// If the PD address is not configured, we use static TiDB addresses in the configuration.
// This is only for test. For a production cluster, the PD address should always be configured.
func (bo *BackendObserver) observeStaticAddrs(ctx context.Context) {
	for ctx.Err() == nil {
		backendInfo := make(map[string]BackendStatus)
		for _, addr := range bo.staticAddrs {
			backendInfo[addr] = StatusHealthy
		}
		// The status port is not configured, so we skip checking health now.
		//bo.checkHealth(ctx, backendInfo)
		bo.notifyIfChanged(backendInfo)
		select {
		case <-time.After(bo.config.healthCheckInterval):
		case <-ctx.Done():
			return
		}
	}
}

// If the PD address is configured, we watch the TiDB addresses on the ETCD.
func (bo *BackendObserver) observeDynamicAddrs(ctx context.Context) {
	// No need to watch the events from etcd because:
	// - When a new backend starts and writes etcd, the HTTP status port is not ready yet.
	// - When a backend shuts down, it doesn't delete itself from the etcd.
	for {
		bo.refreshBackends(ctx)
		select {
		case <-time.After(bo.config.healthCheckInterval):
		case <-ctx.Done():
			return
		}
	}
}

func (bo *BackendObserver) refreshBackends(ctx context.Context) {
	if err := bo.fetchBackendList(ctx); err != nil {
		return
	}
	backendInfo := bo.filterTombstoneBackends()
	backendStatus := bo.checkHealth(ctx, backendInfo)
	bo.notifyIfChanged(backendStatus)
}

func (bo *BackendObserver) fetchBackendList(ctx context.Context) error {
	var response *clientv3.GetResponse
	var err error
	// It's a critical problem if the proxy cannot connect to the server, so we always retry.
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// In case there are too many tombstone backends, the query would be slow, so no need to set a timeout here.
		if response, err = bo.client.Get(ctx, infosync.TopologyInformationPath, clientv3.WithPrefix()); err == nil {
			break
		}
		bo.logger.Error("fetch backend list failed, will retry later", zap.Error(err))
		time.Sleep(bo.config.healthCheckRetryInterval)
	}

	allBackendInfo := make(map[string]*BackendInfo, len(response.Kvs))
	now := time.Now()
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, ttlPathSuffix) {
			addr := key[len(infosync.TopologyInformationPath)+1 : len(key)-len(ttlPathSuffix)]
			info, ok := allBackendInfo[addr]
			if !ok {
				info, ok = bo.allBackendInfo[addr]
			}
			if ok {
				if slices.Compare(info.ttl, kv.Value) != 0 {
					// The TTL is updated this time.
					info.lastUpdate = now
					info.ttl = kv.Value
				}
			} else {
				// A new backend.
				info = &BackendInfo{
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
				bo.logger.Error("unmarshal topology info failed", zap.String("key", string(kv.Key)),
					zap.ByteString("value", kv.Value), zap.Error(err))
				continue
			}
			info, ok := allBackendInfo[addr]
			if !ok {
				info, ok = bo.allBackendInfo[addr]
			}
			if ok {
				info.TopologyInfo = topo
			} else {
				info = &BackendInfo{
					TopologyInfo: topo,
				}
			}
			allBackendInfo[addr] = info
		}
	}
	bo.allBackendInfo = allBackendInfo
	return nil
}

func (bo *BackendObserver) filterTombstoneBackends() map[string]*BackendInfo {
	now := time.Now()
	curBackendInfo := make(map[string]*BackendInfo, len(bo.allBackendInfo))
	for addr, info := range bo.allBackendInfo {
		if info.TopologyInfo == nil || info.ttl == nil {
			continue
		}
		// After running for a long time, there might be many tombstones because failed TiDB instances
		// don't delete themselves from the Etcd. Checking their health is a waste of time, leading to
		// longer and longer checking interval. So tombstones won't be added to curBackendInfo.
		if info.lastUpdate.Add(bo.config.tombstoneThreshold).Before(now) {
			continue
		}
		curBackendInfo[addr] = info
	}
	return curBackendInfo
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backends map[string]*BackendInfo) map[string]BackendStatus {
	curBackendStatus := make(map[string]BackendStatus, len(backends))
	for addr, info := range backends {
		if ctx.Err() != nil {
			return nil
		}
		retriedTimes := 0
		connectWithRetry := func(connect func() error) error {
			var err error
			for ; retriedTimes < bo.config.healthCheckMaxRetries; retriedTimes++ {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err = connect(); err == nil {
					return nil
				}
				if !isRetryableError(err) {
					break
				}
				if retriedTimes < bo.config.healthCheckMaxRetries-1 {
					time.Sleep(bo.config.healthCheckRetryInterval)
				}
			}
			return err
		}

		// When a backend gracefully shut down, the status port returns 500 but the SQL port still accepts
		// new connections, so we must check the status port first.
		schema := "http"
		if bo.httpTLS {
			schema = "https"
		}
		url := fmt.Sprintf("%s://%s:%d%s", schema, info.IP, info.StatusPort, statusPathSuffix)
		var resp *http.Response
		err := connectWithRetry(func() error {
			var err error
			if resp, err = bo.httpCli.Get(url); err == nil {
				if err := resp.Body.Close(); err != nil {
					bo.logger.Error("close http response in health check failed", zap.Error(err))
				}
			}
			return err
		})
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}

		// Also dial the SQL port just in case that the SQL port hangs.
		err = connectWithRetry(func() error {
			conn, err := net.DialTimeout("tcp", addr, bo.config.healthCheckTimeout)
			if err == nil {
				if err := conn.Close(); err != nil && !pnet.IsDisconnectError(err) {
					bo.logger.Error("close connection in health check failed", zap.Error(err))
				}
			}
			return err
		})
		if err == nil {
			curBackendStatus[addr] = StatusHealthy
		}
	}
	return curBackendStatus
}

func (bo *BackendObserver) notifyIfChanged(backendStatus map[string]BackendStatus) {
	updatedBackends := make(map[string]BackendStatus)
	for addr, lastStatus := range bo.curBackendInfo {
		if lastStatus == StatusHealthy {
			if newStatus, ok := backendStatus[addr]; !ok {
				updatedBackends[addr] = StatusCannotConnect
				updateBackendStatusMetrics(addr, lastStatus, StatusCannotConnect)
			} else if newStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
				updateBackendStatusMetrics(addr, lastStatus, newStatus)
			}
		}
	}
	for addr, newStatus := range backendStatus {
		if newStatus == StatusHealthy {
			lastStatus, ok := bo.curBackendInfo[addr]
			if !ok {
				lastStatus = StatusCannotConnect
			}
			if lastStatus != StatusHealthy {
				updatedBackends[addr] = newStatus
				updateBackendStatusMetrics(addr, lastStatus, newStatus)
			}
		}
	}
	if len(updatedBackends) > 0 {
		bo.eventReceiver.OnBackendChanged(updatedBackends)
	}
	bo.curBackendInfo = backendStatus
}

// Close releases all resources.
func (bo *BackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
	bo.wg.Wait()
}

// When the server refused to connect, the port is shut down, so no need to retry.
var notRetryableError = []string{
	"connection refused",
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, errStr := range notRetryableError {
		if strings.Contains(msg, errStr) {
			return false
		}
	}
	return true
}
