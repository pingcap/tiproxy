<!--
Copyright 2026 PingCAP, Inc.
SPDX-License-Identifier: Apache-2.0
-->

# Multi-PD Clusters with Cluster-Scoped Runtime

- Author(s): [YangKeao](https://github.com/YangKeao)

## Overview

This document describes the current design for supporting multiple backend PD clusters in TiProxy.

The design keeps the single-cluster and multi-cluster paths unified:

- TiProxy always builds a `backendcluster.Manager`
- each configured backend PD cluster is represented by one `backendcluster.Cluster`
- upper layers consume aggregated views instead of owning separate single-cluster and multi-cluster code paths

With this structure:

- zero backend clusters means the manager snapshot is empty
- one backend cluster means the manager snapshot has one entry
- multiple backend clusters means the manager snapshot has multiple entries

The code path is the same in all three modes.

## Goals

- Support `proxy.backend-clusters` as the primary multi-cluster configuration
- Keep backward compatibility with legacy `proxy.pd-addrs`
- Support dynamic add, update, and remove of backend clusters through config reload and HTTP config API
- Keep cluster-scoped services under one runtime object
- Route backend traffic and backend HTTP probes through cluster-scoped DNS settings
- Support port-based routing through TiDB label `tiproxy-port`
- Keep VIP behavior well-defined by enabling it only in the single-cluster case

## Configuration Model

### Main Config Structs

The relevant config types are defined in [lib/config/proxy.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/lib/config/proxy.go) and [lib/config/balance.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/lib/config/balance.go).

```go
type ProxyServer struct {
    Addr            string
    AdvertiseAddr   string
    PDAddrs         string
    PortRange       []int
    BackendClusters []BackendCluster
    ProxyServerOnline
}

type BackendCluster struct {
    Name      string
    PDAddrs   string
    NSServers []string
}

type Balance struct {
    LabelName     string
    RoutingRule   string
    Policy        string
    RoutingPolicy string
    ...
}
```

### Effective Backend Cluster Set

`Config.GetBackendClusters()` is the compatibility shim between the legacy and new config formats.

It behaves as follows:

- if `proxy.backend-clusters` is non-empty, return that list
- if `proxy.backend-clusters` is empty and `proxy.pd-addrs` is non-empty, synthesize one backend cluster named `default`
- if both are empty, return `nil`

This means the rest of the runtime never needs to branch on legacy config directly.

### Routing-Related Config

- `balance.routing-rule = "port"` enables port-based grouping
- `proxy.port-range = [start, end]` makes TiProxy listen on multiple SQL ports
- `tiproxy-port` is carried in TiDB labels and is used as the routing key when the routing rule is `port`

## Core Runtime Types

### `backendcluster.Cluster`

`backendcluster.Cluster` is the cluster-scoped runtime container.

Defined in [pkg/manager/backendcluster/manager.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/manager/backendcluster/manager.go).

```go
type Cluster struct {
    cfg        config.BackendCluster
    etcdCli    *clientv3.Client
    infoSyncer *infosync.InfoSyncer
    metrics    *metricsreader.ClusterReader
    httpCli    *httputil.Client
    dialer     *netutil.DNSDialer
}
```

Responsibilities:

- own the normalized backend-cluster config
- own the cluster-scoped etcd client
- own the cluster-scoped `InfoSyncer`
- own the cluster-scoped metrics reader lifecycle
- own the cluster-scoped HTTP client
- own the cluster-scoped DNS dialer

Public methods:

- `Config() config.BackendCluster`
- `EtcdClient() *clientv3.Client`
- `GetTiDBTopology(ctx)`
- `GetPromInfo(ctx)`
- `HTTPClient() *httputil.Client`
- `DialContext(ctx, network, addr)`

The cluster object is intentionally small. It is not a generic service registry. It only exposes the data and network entry points that upper layers need.

### `backendcluster.Manager`

`backendcluster.Manager` is the only owner of backend-cluster lifecycle.

```go
type Manager struct {
    lg         *zap.Logger
    clusterTLS func() *tls.Config
    cfgGetter  config.ConfigGetter

    wg      waitgroup.WaitGroup
    cancel  context.CancelFunc
    metrics *MetricsQuerier
    network *NetworkRouter

    mu struct {
        sync.RWMutex
        clusters map[string]*Cluster
    }
}
```

Responsibilities:

- build the desired backend-cluster set from config
- watch config changes and update the active cluster set
- add new clusters
- replace updated clusters
- remove deleted clusters
- expose stable snapshot-based read access for upper layers
- provide aggregate helper views through `MetricsQuerier` and `NetworkRouter`

Important methods:

- `Start(ctx, cfgGetter, cfgCh)`
- `Snapshot() map[string]*Cluster`
- `MetricsQuerier() *MetricsQuerier`
- `NetworkRouter() *NetworkRouter`
- `PrimaryCluster() *Cluster`
- `PreClose()`
- `GetTiDBTopology(ctx)`
- `Close()`

#### `PrimaryCluster()`

`PrimaryCluster()` only returns a cluster when the manager currently owns exactly one cluster.

That method exists for features that only make sense in the single-cluster case:

- VIP owner election and binding

This keeps the semantics explicit. It does not silently pick the first cluster when multiple clusters are configured.

## Cluster Lifecycle

### Build Path

`Manager.buildCluster()` creates one `Cluster` with the following steps:

1. normalize `config.BackendCluster`
2. parse `ns-servers`
3. create one `netutil.DNSDialer`
4. create one cluster-scoped HTTP client using that dialer
5. create one cluster-scoped etcd client using that dialer
6. initialize one cluster-scoped `InfoSyncer`
7. create one cluster-scoped `metricsreader.ClusterReader`
8. copy currently registered query expressions from the global `MetricsQuerier`
9. start the cluster metrics reader

### Update Path

`Manager.syncClusters()` diffs the desired config against the current `clusters` map.

Behavior:

- unchanged cluster config: reuse the existing `Cluster`
- changed cluster config: build a replacement `Cluster`, switch to it, then close the old one
- new cluster: build and add it
- removed cluster: remove it from the active snapshot, then close it
- failed replacement: keep the old cluster running
- failed new cluster: skip it and keep the rest unchanged

The active set is switched by replacing the manager snapshot map under lock. Cleanup happens after the new snapshot is published.

This minimizes lock hold time and prevents upper layers from observing half-updated state.

### Shutdown Path

`Manager.closeCluster()` closes the resources owned by one cluster in this order:

- cluster metrics reader
- cluster `InfoSyncer`
- cluster etcd client

`Manager.Close()` first stops config watching, then swaps the active snapshot to an empty map, and finally closes all cluster runtimes.

## Aggregation Layer

The upper layers do not own per-cluster lifecycle. They consume aggregation or dispatch helpers created by `backendcluster.Manager`.

### `backendcluster.MetricsQuerier`

Defined in [pkg/manager/backendcluster/metrics_querier.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/manager/backendcluster/metrics_querier.go).

```go
type MetricsQuerier struct {
    manager *Manager
    mu      sync.RWMutex
    exprs   map[string]metricsreader.QueryExpr
    rules   map[string]metricsreader.QueryRule
}
```

Implemented interface:

```go
var _ metricsreader.MetricsQuerier = (*MetricsQuerier)(nil)
```

Role:

- it is a fan-out and merge view over cluster-scoped metrics readers
- it does not own election, scraping, or Prometheus polling by itself
- query expressions are registered once on `MetricsQuerier`, then copied into each active cluster reader

Key methods:

- `AddQueryExpr(key, expr, rule)`
- `RemoveQueryExpr(key)`
- `GetQueryResult(key)`
- `GetBackendMetrics()`
- `GetBackendMetricsByCluster(clusterName)`

Lifecycle is not owned by `MetricsQuerier`.

- cluster-scoped `metricsreader.ClusterReader` instances own their own `Start`, `PreClose`, and `Close`
- `backendcluster.Manager.PreClose()` fans out `PreClose()` to all active cluster readers
- `backendcluster.Manager.Close()` closes all cluster runtimes, including cluster readers

`GetQueryResult()` merges vectors or matrices returned by all cluster readers.

`GetBackendMetrics()` preserves the old single-cluster behavior by serving metrics only when exactly one cluster is active. `GetBackendMetricsByCluster()` is the explicit multi-cluster form.

### `backendcluster.NetworkRouter`

Defined in [pkg/manager/backendcluster/network_router.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/manager/backendcluster/network_router.go).

```go
type NetworkRouter struct {
    manager     *Manager
    clusterTLS  func() *tls.Config
    defaultDial *netutil.DNSDialer
    defaultHTTP *httputil.Client
}
```

Role:

- dispatch backend HTTP traffic to the cluster-specific HTTP client
- dispatch backend TCP dials to the cluster-specific dialer
- fall back to process-default HTTP and TCP behavior only when the caller does not provide a cluster name
- fail closed with `ErrBackendClusterNotFound` when a cluster name is provided but no such cluster exists

Public methods:

- `HTTPClient(clusterName string) *httputil.Client`
- `DialContext(ctx, network, addr, clusterName string) (net.Conn, error)`

This type is deliberately a dispatcher only. It does not cache topology and it does not own cluster lifecycle.

## DNS Design

### `netutil.DNSDialer`

Defined in [pkg/util/netutil/dns.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/util/netutil/dns.go).

```go
type DNSDialer struct {
    cacheTTL   time.Duration
    nameServer []string
    resolver   *net.Resolver
    dialer     net.Dialer
    nextServer atomic.Uint64
    mu struct {
        sync.Mutex
        cacheMap map[string]dnsCacheEntry
    }
}
```

Behavior:

- if `ns-servers` is empty, use the default resolver path
- if `ns-servers` is configured, create a Go resolver with a custom `Resolver.Dial`
- rotate configured DNS servers round-robin
- cache DNS answers briefly in-process
- when dialing a hostname, resolve it through the cluster DNS server list and try the returned IPs in order

Current scope of use:

- etcd / PD client bootstrap and subsequent endpoint dialing
- TiDB SQL backend connections
- TiDB status/config HTTP probes

The current etcd design does not install a custom gRPC resolver. Instead, it relies on two properties:

- the custom dialer guarantees that PD hostnames are resolved through the cluster DNS servers
- the etcd client performs an immediate `Sync()` after creation, so it quickly switches to member URLs advertised by the cluster

This keeps the implementation closer to upstream etcd client behavior.

## Etcd Client Design

The etcd helper is implemented in [pkg/util/etcd/etcd.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/util/etcd/etcd.go).

Relevant function:

```go
func InitEtcdClientWithAddrsAndDialer(
    logger *zap.Logger,
    pdAddrs string,
    tlsConfig *tls.Config,
    dnsDialer *netutil.DNSDialer,
) (*clientv3.Client, error)
```

Behavior:

- build one `clientv3.Client` from the configured PD address list
- if a cluster DNS dialer is provided, install it through `grpc.WithContextDialer`
- after the client is created, call `Sync()` once with timeout to refresh endpoints from etcd membership
- keep etcd's own resolver and endpoint update behavior unchanged after that

The design assumption is that once the client has synced membership, etcd will use the member URLs returned by the cluster instead of staying pinned to the original bootstrap hostname list.

## Topology Aggregation

`Manager.GetTiDBTopology(ctx)` aggregates topology from all active clusters.

Merge behavior:

- call `cluster.GetTiDBTopology(ctx)` for each cluster snapshot entry
- clone each `infosync.TiDBTopologyInfo`
- set `TiDBTopologyInfo.ClusterName = clusterName`
- merge the resulting topology map by cluster-scoped backend ID

If the same backend address appears from two clusters, TiProxy keeps both entries under different backend IDs.

This explicit cluster field is the key that allows downstream routing and network dispatch to remain cluster-aware without overloading TiDB labels.

## Router and Routing Keys

### Router-Side Cluster Awareness

`router.BackendInst` exposes cluster identity:

```go
type BackendInst interface {
    ID() string
    Addr() string
    Healthy() bool
    Local() bool
    Keyspace() string
    ClusterName() string
}
```

`backendWrapper.ClusterName()` reads the explicit `BackendInfo.ClusterName` field populated by topology aggregation. Cluster identity is not represented through a synthetic TiDB label.

### Port-Based Grouping

The score-based router still owns backend grouping. Multi-cluster support does not introduce a second grouping layer.

Instead:

- backend labels carry `tiproxy-port`
- topology aggregation carries backend `ClusterName` explicitly
- `ScoreBasedRouter` sets `matchType = MatchPort` when `balance.routing-rule = "port"`
- group membership is computed from `tiproxy-port` plus the backend's owning cluster

This keeps routing logic local to `pkg/balance/router`.

Operationally, a frontend connection accepted on port `P` is only routed to backends whose `tiproxy-port` label matches `P`.

If multiple TiDB instances share the same `tiproxy-port`, they are still balanced within that group.

### Port Group Key and Conflict Handling

The router uses a cluster-scoped port key:

- `matchPortValue(clusterName, port) = "<clusterName>:<port>"`
- each `MatchPort` group stores that single composite string in `Group.values`
- `Group.Intersect(...)` remains the common grouping primitive for CIDR and port routing

This keeps the grouping representation uniform and avoids special-case tuple handling such as `[]string{clusterName, port}`.

Listener dispatch is handled separately by `matchPortView`:

- if exactly one backend cluster claims listener port `P`, `matchPortView` maps `P` to that cluster-scoped group
- if multiple backend clusters claim listener port `P`, `matchPortView` marks `P` as blocked and routing returns `ErrPortConflict`
- the conflict only blocks new routing on that listener port
- TiDB instances inside one cluster that share the same `tiproxy-port` remain in the same group and can still rebalance among themselves

## Server Integration

The wiring happens in [pkg/server/server.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/server/server.go).

Key integration points:

1. create `backendcluster.Manager`
2. start it with the config watcher
3. if `PrimaryCluster()` is non-nil, keep its etcd client in a local variable for VIP wiring
4. use `MetricsQuerier()` as the server-wide metrics query view
5. pass `NetworkRouter()` into the namespace manager for backend probes
6. pass `NetworkRouter()` into the SQL proxy for backend SQL dials
7. call `clusterManager.PreClose()` during graceful shutdown to resign all cluster-scoped metrics owners early
8. enable VIP only when `PrimaryCluster()` returns a cluster

This means the server itself remains thin. It does not maintain per-cluster maps.

## Namespace and Observer Integration

The observer-side integration is based on one small interface defined in [pkg/balance/observer/health_check.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/balance/observer/health_check.go):

```go
type BackendNetwork interface {
    HTTPClient(clusterName string) *http.Client
    DialContext(ctx context.Context, network, addr, clusterName string) (net.Conn, error)
}
```

`DefaultHealthCheck` uses the backend's explicit `ClusterName` field to choose:

- the HTTP client for `/status` and `/config`
- the TCP dial path for SQL health checks

The namespace manager accepts this through `SetBackendNetwork(...)`, so the observer stack does not need to know about `backendcluster.Manager` directly.

## Proxy Integration

The SQL proxy uses one narrow interface defined in [pkg/proxy/proxy.go](/home/yangkeao/Project/github.com/YangKeao/tiproxy/pkg/proxy/proxy.go):

```go
type BackendDialer interface {
    DialContext(ctx context.Context, network, addr, clusterName string) (net.Conn, error)
}
```

`SQLServer.SetBackendDialer(...)` installs the cluster-aware dispatch path.

When the proxy opens a backend SQL connection, it passes `backendInst.ClusterName()` into this dialer. The dialer implementation is `backendcluster.NetworkRouter`.

This is enough to keep backend SQL traffic on the DNS path of the backend's owning cluster.

## VIP Behavior

VIP remains single-cluster only.

The server starts VIP only when:

- `clusterManager.PrimaryCluster()` returns a cluster
- that cluster provides the server-wide etcd client

If there are zero or multiple backend clusters, VIP is disabled and TiProxy logs that the feature is not enabled in the current mode.

This avoids ambiguous VIP ownership across multiple PD clusters.

## Dynamic Update Semantics

`proxy.backend-clusters` is reloadable.

The active backend-cluster set can be changed by:

- config file reload
- HTTP config API update

Runtime behavior:

- a new cluster can be added after startup
- an existing cluster can be updated in place through replacement
- a cluster can be removed without restarting TiProxy
- TiProxy may start with zero backend clusters and wait for later configuration

The `Manager` snapshot model ensures that readers always see a complete cluster set and never an in-between half-built state.
