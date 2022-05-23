package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/util/security"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
	StatusServerDown
	StatusMemoryHigh
	StatusRunSlow
	StatusSchemaOutdated
)

var statusNames = map[BackendStatus]string{
	StatusHealthy:        "healthy",
	StatusCannotConnect:  "cannot connect",
	StatusServerDown:     "server down",
	StatusMemoryHigh:     "memory high",
	StatusRunSlow:        "run slow",
	StatusSchemaOutdated: "schema outdated",
}

var statusScores = map[BackendStatus]int{
	StatusHealthy:        0,
	StatusCannotConnect:  10000000,
	StatusServerDown:     10000000,
	StatusMemoryHigh:     5000,
	StatusRunSlow:        5000,
	StatusSchemaOutdated: 10000000,
}

const (
	healthCheckInterval      = 5 * time.Second
	healthCheckMaxRetries    = 3
	healthCheckRetryInterval = 100 * time.Millisecond
	healthCheckTimeout       = 1 * time.Second
)

type BackendEventReceiver interface {
	OnBackendChanged(backends map[string]*BackendInfo)
}

type BackendInfo struct {
	*infosync.TopologyInfo
	status BackendStatus
}

type BackendObserver struct {
	backendInfo   map[string]*BackendInfo
	staticAddrs   []string
	eventReceiver BackendEventReceiver
	cancelFunc    context.CancelFunc
}

var globalEtcdClient atomic.Value

func InitEtcdClient(cfg *config.Proxy) error {
	pdAddr := cfg.ProxyServer.PDAddr
	if len(pdAddr) == 0 {
		// use tidb server addresses directly
		return nil
	}
	pdEndpoints := strings.Split(pdAddr, ",")
	logConfig := zap.NewProductionConfig()
	logConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	tlsConfig, err := security.CreateClusterTLSConfig(cfg.Security.ClusterSSLCA, cfg.Security.ClusterSSLKey,
		cfg.Security.ClusterSSLCert)
	if err != nil {
		return err
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
	if err == nil {
		globalEtcdClient.Store(etcdClient)
	}
	return errors.Annotate(err, "init etcd client failed")
}

func GetEtcdClient() *clientv3.Client {
	etcdClient := globalEtcdClient.Load()
	if etcdClient == nil {
		return nil
	}
	return etcdClient.(*clientv3.Client)
}

func NewBackendObserver(eventReceiver BackendEventReceiver, staticAddrs []string) (*BackendObserver, error) {
	if GetEtcdClient() == nil {
		if len(staticAddrs) == 0 {
			return nil, ErrNoInstanceToSelect
		}
	}
	bo := &BackendObserver{
		backendInfo:   make(map[string]*BackendInfo),
		staticAddrs:   staticAddrs,
		eventReceiver: eventReceiver,
	}
	childCtx, cancelFunc := context.WithCancel(context.Background())
	bo.cancelFunc = cancelFunc
	go bo.observe(childCtx)
	return bo, nil
}

func (bo *BackendObserver) observe(ctx context.Context) {
	if GetEtcdClient() == nil {
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
		case <-time.After(healthCheckInterval):
		case <-ctx.Done():
			return
		}
		backendInfo := make(map[string]*BackendInfo)
		for _, addr := range bo.staticAddrs {
			backendInfo[addr] = &BackendInfo{
				status: StatusHealthy,
			}
		}
		bo.checkHealth(ctx, backendInfo)
		bo.notifyIfChanged(backendInfo)
	}
}

// If the PD address is configured, we watch the TiDB addresses on the ETCD.
func (bo *BackendObserver) observeDynamicAddrs(ctx context.Context) {
	watchCh := GetEtcdClient().Watch(ctx, infosync.TopologyInformationPath, clientv3.WithPrefix())
	for ctx.Err() == nil {
		select {
		case _, ok := <-watchCh:
			if !ok {
				// The etcdClient is closed.
				return
			}
		case <-time.After(healthCheckInterval):
		case <-ctx.Done():
			return
		}
		backendInfo, err := bo.fetchBackendList(ctx)
		if err != nil {
			continue
		}
		bo.checkHealth(ctx, backendInfo)
		bo.notifyIfChanged(backendInfo)
	}
}

func (bo *BackendObserver) fetchBackendList(ctx context.Context) (map[string]*BackendInfo, error) {
	var response *clientv3.GetResponse
	var err error
	// It's a critical problem if the proxy cannot connect to the server, so we always retry.
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		childCtx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
		response, err = GetEtcdClient().Get(childCtx, infosync.TopologyInformationPath, clientv3.WithPrefix())
		cancel()
		if err == nil {
			break
		}
		logutil.Logger(ctx).Error("fetch backend list failed", zap.Error(err))
		time.Sleep(healthCheckRetryInterval)
	}
	if err != nil {
		return nil, err
	}

	backendInfo := make(map[string]*BackendInfo)
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		if !strings.HasSuffix(key, "/info") {
			continue
		}
		var topo *infosync.TopologyInfo
		if err = json.Unmarshal(kv.Value, &topo); err != nil {
			logutil.Logger(ctx).Error("unmarshal topology info failed", zap.String("key", string(kv.Key)),
				zap.ByteString("value", kv.Value), zap.Error(err))
			return nil, err
		}
		addr := key[len(infosync.TopologyInformationPath)+1 : len(key)-len("/info")]
		backendInfo[addr] = &BackendInfo{
			TopologyInfo: topo,
			status:       StatusHealthy,
		}
	}
	return backendInfo, nil
}

func (bo *BackendObserver) checkHealth(ctx context.Context, backendInfo map[string]*BackendInfo) {
	for _, info := range backendInfo {
		url := fmt.Sprintf("http://%s:%d/status", info.TopologyInfo.IP, info.TopologyInfo.StatusPort)
		var resp *http.Response
		var err error
		for i := 0; i < healthCheckMaxRetries; i++ {
			if ctx.Err() != nil {
				return
			}
			if resp, err = http.Get(url); err == nil {
				break
			}
			if i < healthCheckMaxRetries-1 {
				time.Sleep(healthCheckRetryInterval)
			}
		}
		if err != nil {
			info.status = StatusCannotConnect
			continue
		} else if resp.StatusCode != http.StatusOK {
			info.status = StatusServerDown
		} else {
			info.status = StatusHealthy
		}
		if err = resp.Body.Close(); err != nil {
			logutil.Logger(ctx).Warn("close http response failed", zap.Error(err))
		}
	}
}

func (bo *BackendObserver) notifyIfChanged(backendInfo map[string]*BackendInfo) {
	backends := make(map[string]*BackendInfo)
	for addr, originalInfo := range bo.backendInfo {
		if originalInfo.status == StatusHealthy {
			newInfo, ok := backendInfo[addr]
			if !ok || newInfo.status != StatusHealthy {
				backends[addr] = newInfo
			}
		}
	}
	for addr, newInfo := range backendInfo {
		if newInfo.status == StatusHealthy {
			originalInfo, ok := bo.backendInfo[addr]
			if !ok || originalInfo.status != StatusHealthy {
				backends[addr] = newInfo
			}
		}
	}
	if len(backends) > 0 {
		bo.eventReceiver.OnBackendChanged(backends)
	}
	bo.backendInfo = backendInfo
}

func (bo *BackendObserver) Close() {
	if bo.cancelFunc != nil {
		bo.cancelFunc()
	}
}
