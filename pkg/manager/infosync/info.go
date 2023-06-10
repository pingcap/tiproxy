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
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/retry"
	"github.com/pingcap/TiProxy/lib/util/sys"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/util/versioninfo"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/hack"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	tiproxyTopologyPath = "/topology/tiproxy"

	topologySessionTTL    = 45
	topologyRefreshIntvl  = 30 * time.Second
	topologyPutTimeout    = 2 * time.Second
	topologyPutRetryIntvl = 1 * time.Second
	topologyPutRetryCnt   = 3
	logInterval           = 10

	TTLSuffix  = "ttl"
	InfoSuffix = "info"
)

// InfoSyncer syncs TiProxy topology to ETCD.
// It writes 2 items: `/topology/tiproxy/.../info` and `/topology/tiproxy/.../ttl`.
// `info` is written once and `ttl` will be erased automatically after TiProxy is down.
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
	sessionTTL    int
	refreshIntvl  time.Duration
	putTimeout    time.Duration
	putRetryIntvl time.Duration
	putRetryCnt   uint64
}

type TopologyInfo struct {
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	IP             string `json:"ip"`
	Port           string `json:"port"`
	StatusPort     string `json:"status_port"`
	DeployPath     string `json:"deploy_path"`
	StartTimestamp int64  `json:"start_timestamp"`
}

func NewInfoSyncer(etcdCli *clientv3.Client, lg *zap.Logger) *InfoSyncer {
	return &InfoSyncer{
		etcdCli: etcdCli,
		lg:      lg,
		syncConfig: syncConfig{
			sessionTTL:    topologySessionTTL,
			refreshIntvl:  topologyRefreshIntvl,
			putTimeout:    topologyPutTimeout,
			putRetryIntvl: topologyPutRetryIntvl,
			putRetryCnt:   topologyPutRetryCnt,
		},
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
	is.wg.Run(func() {
		is.updateTopologyLivenessLoop(childCtx, topologyInfo)
	})
	return nil
}

func (is *InfoSyncer) updateTopologyLivenessLoop(ctx context.Context, topologyInfo *TopologyInfo) {
	// We allow TiProxy to start before PD, so do not fail in the main goroutine.
	if err := is.initTopologySession(ctx); err != nil {
		return
	}
	_ = is.storeTopologyInfo(ctx, topologyInfo)
	_ = is.updateTopologyAliveness(ctx, topologyInfo)
	ticker := time.NewTicker(is.syncConfig.refreshIntvl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Even if the topology is unchanged, the server may restart.
			// We don't assume the server still persists data after restart, so we always store it again.
			_ = is.storeTopologyInfo(ctx, topologyInfo)
			_ = is.updateTopologyAliveness(ctx, topologyInfo)
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
		topologySession, err := concurrency.NewSession(is.etcdCli, concurrency.WithTTL(is.syncConfig.sessionTTL), concurrency.WithContext(ctx))
		if err == nil {
			is.topologySession = topologySession
			is.lg.Info("topology session is initialized", zap.Error(err))
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
	ip := sys.GetLocalIP()
	_, port, err := net.SplitHostPort(cfg.Proxy.Addr)
	if err != nil {
		return nil, err
	}
	_, statusPort, err := net.SplitHostPort(cfg.API.Addr)
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

func (is *InfoSyncer) storeTopologyInfo(ctx context.Context, topologyInfo *TopologyInfo) error {
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.Trace(err)
	}
	value := hack.String(infoBuf)
	key := fmt.Sprintf("%s/%s/%s", tiproxyTopologyPath, net.JoinHostPort(topologyInfo.IP, topologyInfo.Port), InfoSuffix)
	err = retry.Retry(func() error {
		childCtx, cancel := context.WithTimeout(ctx, is.syncConfig.putTimeout)
		_, err := is.etcdCli.Put(childCtx, key, value)
		cancel()
		return err
	}, ctx, is.syncConfig.putRetryIntvl, is.syncConfig.putRetryCnt)
	if err != nil {
		is.lg.Error("failed to store topology info", zap.Error(err))
	}
	return err
}

func (is *InfoSyncer) updateTopologyAliveness(ctx context.Context, topologyInfo *TopologyInfo) error {
	key := fmt.Sprintf("%s/%s/%s", tiproxyTopologyPath, net.JoinHostPort(topologyInfo.IP, topologyInfo.Port), TTLSuffix)
	// The lease may be not found and the session won't be recreated, so do not retry infinitely.
	err := retry.Retry(func() error {
		value := fmt.Sprintf("%v", time.Now().UnixNano())
		childCtx, cancel := context.WithTimeout(ctx, is.syncConfig.putTimeout)
		_, err := is.etcdCli.Put(childCtx, key, value, clientv3.WithLease(is.topologySession.Lease()))
		cancel()
		return err
	}, ctx, is.syncConfig.putRetryIntvl, is.syncConfig.putRetryCnt)
	if err != nil {
		is.lg.Error("failed to update topology ttl", zap.Error(err))
	}
	return err
}

func (is *InfoSyncer) Close() {
	if is.cancelFunc != nil {
		is.cancelFunc()
	}
	is.wg.Wait()
}
