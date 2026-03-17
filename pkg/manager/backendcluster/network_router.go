// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pingcap/tiproxy/lib/util/errors"
	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/netutil"
)

var ErrBackendClusterNotFound = errors.New("backend cluster not found")

// NetworkRouter is a thin dispatch view over cluster-scoped dialers and HTTP clients.
// It does not own any cluster lifecycle by itself.
type NetworkRouter struct {
	manager     *Manager
	clusterTLS  func() *tls.Config
	defaultDial *netutil.DNSDialer
	defaultHTTP *httputil.Client
}

func NewNetworkRouter(manager *Manager, clusterTLS func() *tls.Config) *NetworkRouter {
	return &NetworkRouter{
		manager:     manager,
		clusterTLS:  clusterTLS,
		defaultDial: netutil.NewDNSDialer(nil),
		defaultHTTP: httputil.NewHTTPClientWithDialContext(clusterTLS, nil),
	}
}

func (nr *NetworkRouter) missingClusterHTTPClient(clusterName string) *httputil.Client {
	return httputil.NewHTTPClientWithDialContext(nr.clusterTLS, func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.Wrapf(ErrBackendClusterNotFound, "cluster %s", clusterName)
	})
}

func (nr *NetworkRouter) HTTPClient(clusterName string) *httputil.Client {
	if clusterName != "" {
		if cluster := nr.manager.Snapshot()[clusterName]; cluster != nil {
			return cluster.HTTPClient()
		}
		return nr.missingClusterHTTPClient(clusterName)
	}
	return nr.defaultHTTP
}

func (nr *NetworkRouter) DialContext(ctx context.Context, network, addr, clusterName string) (net.Conn, error) {
	if clusterName != "" {
		if cluster := nr.manager.Snapshot()[clusterName]; cluster != nil {
			return cluster.DialContext(ctx, network, addr)
		}
		return nil, errors.Wrapf(ErrBackendClusterNotFound, "cluster %s", clusterName)
	}
	return nr.defaultDial.DialContext(ctx, network, addr)
}
