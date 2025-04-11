// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package infosync

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/retry"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/siddontang/go/hack"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	tiproxyTopologyPath = "/topology/tiproxy"
	promTopologyPath    = "/topology/prometheus"

	// tidbTopologyInformationPath means etcd path for storing topology info.
	tidbTopologyInformationPath = "/topology/tidb"

	topologySessionTTL    = 45
	topologyRefreshIntvl  = 30 * time.Second
	topologyPutTimeout    = 2 * time.Second
	topologyPutRetryIntvl = 1 * time.Second
	topologyPutRetryCnt   = 3
	getPromTimeout        = 2 * time.Second
	getPromRetryIntvl     = 0
	getPromRetryCnt       = 3
	logInterval           = 10

	ttlSuffix  = "ttl"
	infoSuffix = "info"
)

var (
	ErrNoProm = errors.New("no prometheus info")
)

// InfoSyncer syncs TiProxy topology to ETCD and queries TiDB topology from ETCD.
// It writes 2 items to ETCD: `/topology/tiproxy/.../info` and `/topology/tiproxy/.../ttl`.
// They are erased after TiProxy is down.
// The code is modified from github.com/pingcap/tidb/domain/infosync/info.go.
type InfoSyncer struct {
	syncConfig      syncConfig
	lg              *zap.Logger
	etcdCli         *clientv3.Client
	wg              waitgroup.WaitGroup
	cancelFunc      context.CancelFunc
	topologySession *concurrency.Session
}

type syncConfig struct {
	sessionTTL        int
	refreshIntvl      time.Duration
	putTimeout        time.Duration
	putRetryIntvl     time.Duration
	putRetryCnt       uint64
	getPromTimeout    time.Duration
	getPromRetryIntvl time.Duration
	getPromRetryCnt   uint64
}

// TopologyInfo is the info of TiProxy.
type TopologyInfo struct {
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	IP             string `json:"ip"`
	Port           string `json:"port"`
	StatusPort     string `json:"status_port"`
	DeployPath     string `json:"deploy_path"`
	StartTimestamp int64  `json:"start_timestamp"`
}

// TiDBTopologyInfo is the topology info of TiDB.
type TiDBTopologyInfo struct {
	Version        string            `json:"version"`
	GitHash        string            `json:"git_hash"`
	IP             string            `json:"ip"`
	StatusPort     uint              `json:"status_port"`
	DeployPath     string            `json:"deploy_path"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
}

// PrometheusInfo is the info of prometheus.
type PrometheusInfo struct {
	IP         string `json:"ip"`
	BinaryPath string `json:"binary_path"`
	Port       int    `json:"port"`
}

func NewInfoSyncer(lg *zap.Logger, etcdCli *clientv3.Client) *InfoSyncer {
	return &InfoSyncer{
		lg: lg,
		syncConfig: syncConfig{
			sessionTTL:        topologySessionTTL,
			refreshIntvl:      topologyRefreshIntvl,
			putTimeout:        topologyPutTimeout,
			putRetryIntvl:     topologyPutRetryIntvl,
			putRetryCnt:       topologyPutRetryCnt,
			getPromTimeout:    getPromTimeout,
			getPromRetryCnt:   getPromRetryCnt,
			getPromRetryIntvl: getPromRetryIntvl,
		},
		etcdCli: etcdCli,
	}
}

func (is *InfoSyncer) Init(ctx context.Context, cfg *config.Config) error {
	topologyInfo, err := is.getTopologyInfo(cfg)
	if err != nil {
		is.lg.Error("get topology failed", zap.Error(err))
		return err
	}

	childCtx, cancelFunc := context.WithCancel(ctx)
	is.cancelFunc = cancelFunc
	is.wg.RunWithRecover(func() {
		is.updateTopologyLivenessLoop(childCtx, topologyInfo)
	}, nil, is.lg)
	return nil
}

func (is *InfoSyncer) updateTopologyLivenessLoop(ctx context.Context, topologyInfo *TopologyInfo) {
	// We allow TiProxy to start before PD, so do not fail in the main goroutine.
	if err := is.initTopologySession(ctx); err != nil {
		return
	}
	is.syncTopology(ctx, topologyInfo)
	ticker := time.NewTicker(is.syncConfig.refreshIntvl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			is.syncTopology(ctx, topologyInfo)
		case <-is.topologySession.Done():
			is.lg.Warn("restart topology session")
			if err := is.initTopologySession(ctx); err != nil {
				return
			}
		}
	}
}

func (is *InfoSyncer) initTopologySession(ctx context.Context) error {
	// Infinitely retry until cancelled.
	return retry.RetryNotify(func() error {
		// Do not use context.WithTimeout, otherwise the session will be cancelled after timeout, even if the session is created successfully.
		topologySession, err := concurrency.NewSession(is.etcdCli, concurrency.WithTTL(is.syncConfig.sessionTTL), concurrency.WithContext(ctx))
		if err == nil {
			is.topologySession = topologySession
			is.lg.Info("topology session is initialized")
		}
		return err
	}, ctx, is.syncConfig.putRetryIntvl, retry.InfiniteCnt,
		func(err error, duration time.Duration) {
			is.lg.Error("failed to init topology session, retrying", zap.Error(err))
		}, logInterval)
}

func (is *InfoSyncer) getTopologyInfo(cfg *config.Config) (*TopologyInfo, error) {
	s, err := os.Executable()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	ip, port, statusPort, err := cfg.GetIPPort()
	if err != nil {
		return nil, err
	}
	return &TopologyInfo{
		Version:        versioninfo.TiProxyVersion,
		GitHash:        versioninfo.TiProxyGitHash,
		IP:             ip,
		Port:           port,
		StatusPort:     statusPort,
		DeployPath:     dir,
		StartTimestamp: time.Now().Unix(),
	}, nil
}

func (is *InfoSyncer) syncTopology(ctx context.Context, topologyInfo *TopologyInfo) {
	// Even if the topology is unchanged, the server may restart.
	// We don't assume the server still persists data after restart, so we always store it again.
	if err := is.storeTopologyInfo(ctx, topologyInfo); err != nil {
		is.lg.Error("failed to store topology info", zap.Error(err))
	}
	if err := is.updateTopologyAliveness(ctx, topologyInfo); err != nil {
		is.lg.Error("failed to update topology ttl", zap.Error(err))
	}
}

func (is *InfoSyncer) storeTopologyInfo(ctx context.Context, topologyInfo *TopologyInfo) error {
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	value := hack.String(infoBuf)
	key := fmt.Sprintf("%s/%s/%s", tiproxyTopologyPath, net.JoinHostPort(topologyInfo.IP, topologyInfo.Port), infoSuffix)
	return retry.Retry(func() error {
		childCtx, cancel := context.WithTimeout(ctx, is.syncConfig.putTimeout)
		_, err := is.etcdCli.Put(childCtx, key, value, clientv3.WithLease(is.topologySession.Lease()))
		cancel()
		return errors.WithStack(err)
	}, ctx, is.syncConfig.putRetryIntvl, is.syncConfig.putRetryCnt)
}

func (is *InfoSyncer) updateTopologyAliveness(ctx context.Context, topologyInfo *TopologyInfo) error {
	key := fmt.Sprintf("%s/%s/%s", tiproxyTopologyPath, net.JoinHostPort(topologyInfo.IP, topologyInfo.Port), ttlSuffix)
	// The lease may be not found and the session won't be recreated, so do not retry infinitely.
	return retry.Retry(func() error {
		value := fmt.Sprintf("%v", time.Now().UnixNano())
		childCtx, cancel := context.WithTimeout(ctx, is.syncConfig.putTimeout)
		_, err := is.etcdCli.Put(childCtx, key, value, clientv3.WithLease(is.topologySession.Lease()))
		cancel()
		return errors.WithStack(err)
	}, ctx, is.syncConfig.putRetryIntvl, is.syncConfig.putRetryCnt)
}

func (is *InfoSyncer) removeTopology(ctx context.Context) error {
	// removeTopology is called when closing TiProxy. We shouldn't make it too long, so we don't retry here.
	// It will be removed automatically after TTL expires.
	childCtx, cancel := context.WithTimeout(ctx, is.syncConfig.putTimeout)
	_, err := is.etcdCli.Delete(childCtx, tiproxyTopologyPath, clientv3.WithPrefix())
	cancel()
	return errors.WithStack(err)
}

func (is *InfoSyncer) GetTiDBTopology(ctx context.Context) (map[string]*TiDBTopologyInfo, error) {
	// etcdCli.Get will retry infinitely internally.
	res, err := is.etcdCli.Get(ctx, tidbTopologyInformationPath, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	infos := make(map[string]*TiDBTopologyInfo, len(res.Kvs)/2)
	ttls := make(map[string]struct{}, len(res.Kvs)/2)
	for _, kv := range res.Kvs {
		key := hack.String(kv.Key)
		switch {
		case strings.HasSuffix(key, ttlSuffix):
			addr := key[len(tidbTopologyInformationPath)+1 : len(key)-len(ttlSuffix)-1]
			ttls[addr] = struct{}{}
		case strings.HasSuffix(key, infoSuffix):
			var topology *TiDBTopologyInfo
			addr := key[len(tidbTopologyInformationPath)+1 : len(key)-len(infoSuffix)-1]
			if err = json.Unmarshal(kv.Value, &topology); err != nil {
				is.lg.Error("unmarshal topology info failed", zap.String("key", key),
					zap.String("value", hack.String(kv.Value)), zap.Error(err))
			} else {
				infos[addr] = topology
			}
		}
	}

	for addr := range infos {
		// If ttl is empty, maybe the backend is down.
		if _, ok := ttls[addr]; !ok {
			delete(infos, addr)
		}
	}
	return infos, nil
}

func (is *InfoSyncer) GetPromInfo(ctx context.Context) (*PrometheusInfo, error) {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	kvs, err := etcd.GetKVs(ctx, is.etcdCli, promTopologyPath, opts, is.syncConfig.getPromTimeout, is.syncConfig.getPromRetryIntvl, is.syncConfig.getPromRetryCnt)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, ErrNoProm
	}
	var info PrometheusInfo
	if err = json.Unmarshal(kvs[0].Value, &info); err != nil {
		return nil, errors.WithStack(err)
	}
	return &info, nil
}

func (is *InfoSyncer) Close() error {
	if is.cancelFunc != nil {
		is.cancelFunc()
	}
	is.wg.Wait()
	return is.removeTopology(context.Background())
}
