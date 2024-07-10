// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package etcd

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// InitEtcdClient initializes an etcd client that connects to PD ETCD server.
func InitEtcdClient(logger *zap.Logger, cfg *config.Config, certMgr *cert.CertManager) (*clientv3.Client, error) {
	pdAddr := cfg.Proxy.PDAddrs
	if len(pdAddr) == 0 {
		// use tidb server addresses directly
		return nil, nil
	}
	pdEndpoints := strings.Split(pdAddr, ",")
	logger.Info("connect ETCD servers", zap.Strings("addrs", pdEndpoints))
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

// CreateEtcdServer creates an etcd server and is only used for testing.
func CreateEtcdServer(addr, dir string, lg *zap.Logger) (*embed.Etcd, error) {
	serverURL, err := url.Parse(fmt.Sprintf("http://%s", addr))
	if err != nil {
		return nil, err
	}
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LCUrls = []url.URL{*serverURL}
	cfg.LPUrls = []url.URL{*serverURL}
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(lg)
	cfg.LogLevel = "fatal"
	// Reuse port so that it can reboot with the same port immediately.
	cfg.SocketOpts = transport.SocketOpts{
		ReuseAddress: true,
		ReusePort:    true,
	}
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	<-etcd.Server.ReadyNotify()
	return etcd, err
}

func ConfigForEtcdTest(endpoint string) *config.Config {
	return &config.Config{
		Proxy: config.ProxyServer{
			Addr:    "0.0.0.0:6000",
			PDAddrs: endpoint,
		},
		API: config.API{
			Addr: "0.0.0.0:3080",
		},
	}
}
