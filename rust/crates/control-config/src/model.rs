// Copyright 2026 PingCAP, Inc.
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

//! Go-compatible configuration and namespace data model.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const DEFAULT_SQL_ADDR: &str = "0.0.0.0:6000";
const DEFAULT_API_ADDR: &str = "0.0.0.0:3080";
const DEFAULT_PD_ADDRS: &str = "127.0.0.1:2379";
const DEFAULT_CLUSTER_NAME: &str = "default";
const DEFAULT_CONNECTION_BUFFER_BYTES: u32 = 32 * 1024;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLISECOND: i64 = 1_000_000;

/// A deterministic validation or projection failure.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ConfigError {
    /// A bounded field has an invalid value.
    #[error("invalid configuration field {field}: {class}")]
    InvalidField {
        /// Stable field name.
        field: &'static str,
        /// Stable error class without the candidate payload.
        class: &'static str,
    },
    /// A hot reload attempted to change a restart-required field.
    #[error("configuration field {field} requires restart")]
    RestartRequired {
        /// Stable field name.
        field: &'static str,
    },
}

/// Source lineages observed while constructing one immutable snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceRevision {
    /// Process-local accepted-file lineage.
    pub file_revision: u64,
    /// Last accepted etcd header or event revision; zero means no etcd view.
    pub etcd_revision: i64,
}

/// TLS material and policy used for client connections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientTlsConfig {
    /// Optional CA bundle path.
    pub ca_path: Option<Arc<Path>>,
    /// Optional client certificate path.
    pub certificate_path: Option<Arc<Path>>,
    /// Optional client private-key path.
    pub private_key_path: Option<Arc<Path>>,
    /// Minimum accepted TLS version (`1.2` or `1.3`).
    pub minimum_version: Arc<str>,
    /// Whether server CA verification is disabled.
    pub skip_ca_verification: bool,
    /// Sorted, de-duplicated allowed certificate common names.
    pub allowed_common_names: Arc<[Arc<str>]>,
}

/// Normalized backend-cluster input consumed by CP-TOPO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendClusterConfig {
    /// Stable cluster name.
    pub name: Arc<str>,
    /// Trimmed, non-empty PD endpoints.
    pub pd_addrs: Arc<[Arc<str>]>,
    /// Normalized DNS endpoints with explicit ports.
    pub ns_servers: Arc<[Arc<str>]>,
}

/// Restart-pinned PD/etcd transport used by Rust-owned config persistence.
///
/// This deliberately follows Go's `InitEtcdClient`: the persistence home is
/// the legacy `proxy.pd-addrs` field, not the dynamic backend-cluster list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigPersistence {
    /// Trimmed, non-empty PD endpoints.
    pub pd_addrs: Arc<[Arc<str>]>,
    /// Complete client TLS material for the PD/etcd connection.
    pub cluster_tls: ClientTlsConfig,
}

/// Inputs for the dedicated VIP election and network owner.
///
/// The decision to start is pinned at process startup, matching Go's VIP
/// manager. A missing address/interface pair or a non-singleton backend set
/// disables VIP; neither case grants ownership of a partially configured IP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VipConfig {
    /// Address as configured, including its optional CIDR prefix.
    pub address: Arc<str>,
    /// Parsed host IP used for the dedicated election key and GARP.
    pub ip: IpAddr,
    /// Prefix length to compare and bind on the Linux interface.
    pub prefix_len: u8,
    /// Linux interface that holds the address.
    pub interface: Arc<str>,
    /// The sole backend cluster's PD endpoints, which own the VIP election.
    pub pd_addrs: Arc<[Arc<str>]>,
    /// TLS material for that cluster's PD connection.
    pub cluster_tls: ClientTlsConfig,
    /// Immediate GARP packets in each burst.
    pub garp_burst_count: u64,
    /// One-second follow-up GARP bursts after takeover.
    pub garp_refresh_count: u64,
}

impl VipConfig {
    /// Returns the Go-compatible election prefix for this exact VIP host IP.
    #[must_use]
    pub fn election_name(&self) -> String {
        format!("/tiproxy/vip/{}/owner", self.ip)
    }

    /// Returns the canonical address and prefix for Linux address operations.
    #[must_use]
    pub fn cidr(&self) -> String {
        format!("{}/{}", self.ip, self.prefix_len)
    }
}

/// Health observer policy consumed by CP-TOPO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HealthCheckConfig {
    /// Whether active health probes are enabled.
    pub enabled: bool,
    /// Probe cycle in nanoseconds.
    pub interval_nanos: i64,
    /// Retry count for one probe cycle.
    pub max_retries: u32,
    /// Delay between retries in nanoseconds.
    pub retry_interval_nanos: i64,
    /// Connection deadline in nanoseconds.
    pub dial_timeout_nanos: i64,
    /// Metrics refresh cycle in nanoseconds.
    pub metrics_interval_nanos: i64,
    /// Metrics request deadline in nanoseconds.
    pub metrics_timeout_nanos: i64,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_nanos: 3 * NANOS_PER_SECOND,
            max_retries: 3,
            retry_interval_nanos: NANOS_PER_SECOND,
            dial_timeout_nanos: 2 * NANOS_PER_SECOND,
            metrics_interval_nanos: 5 * NANOS_PER_SECOND,
            metrics_timeout_nanos: 3 * NANOS_PER_SECOND,
        }
    }
}

/// Configured identity and dependency inputs consumed by CP-TOPO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyConfig {
    /// Explicit advertise-address override, when a non-empty `advertise-addr`
    /// is configured. This is deterministic raw material: resolving it (and any
    /// interface fallback) into the published host belongs to CP-TOPO, not here.
    pub advertise_host_override: Option<Arc<str>>,
    /// The bind host of the first expanded SQL serving listener.
    pub bind_sql_host: Arc<str>,
    /// Advertised SQL port, taken from that first expanded listener (so a
    /// port range contributes its first port, matching Go).
    pub sql_port: u16,
    /// HTTP status port.
    pub status_port: u16,
    /// Restart-pinned TCP port for the Rust metrics-owner endpoint. Zero asks
    /// the OS for an ephemeral port at process startup.
    pub metrics_owner_port: u16,
    /// The raw HA virtual IP string (empty when HA is disabled). Kept verbatim
    /// because the resolver excludes it with Go's `HasPrefix` compatibility.
    pub ha_virtual_ip: Arc<str>,
    /// Stable, name-sorted backend clusters.
    pub backend_clusters: Arc<[BackendClusterConfig]>,
    /// Complete client TLS material for PD/etcd connections.
    pub cluster_tls: ClientTlsConfig,
}

/// Process/build facts passed from the binary to CP-TOPO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyRuntimeIdentity {
    /// Build version.
    pub version: Arc<str>,
    /// Build commit.
    pub git_hash: Arc<str>,
    /// Executable deployment directory.
    pub deploy_path: PathBuf,
    /// Process start time in Unix seconds.
    pub start_timestamp: i64,
}

/// Backend-group selection rule consumed by CP-ROUTE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingRule {
    /// Every backend belongs to one namespace-wide group.
    MatchAll,
    /// Match a backend group's CIDR labels against the client address.
    ClientCidr,
    /// Match a backend group's CIDR labels against the proxy address.
    ProxyCidr,
    /// Match a cluster-scoped backend group against the accepting listener port.
    ListenerPort,
}

/// Ordered balance-factor family consumed by CP-ROUTE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingBalancePolicy {
    /// Prefer health/resource signals before locality and connection count.
    Resource,
    /// Prefer locality before health/resource signals and connection count.
    Location,
    /// Route only by status and connection count.
    Connection,
}

/// Candidate choice policy within the lowest-score backend set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingSelectionPolicy {
    /// Prefer an idle backend, otherwise choose within the lowest-score set.
    PreferIdle,
    /// Choose randomly within the lowest-score set.
    Random,
}

/// One factor's hot-reloadable migration rate.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RoutingFactorConfig {
    /// Maximum migrations per second; zero selects the factor's Go default.
    pub migrations_per_second: f64,
}

/// Connection-count factor policy.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RoutingConnectionFactorConfig {
    /// Maximum migrations per second; zero selects the Go default.
    pub migrations_per_second: f64,
    /// Ratio above which connection-count imbalance is actionable; zero selects
    /// the Go default.
    pub count_ratio_threshold: f64,
}

/// Immutable, normalized process routing policy projected from one exact
/// [`EffectiveConfig`] generation.
///
/// This is configuration only: topology, backend health, resource metrics, and
/// connection accounting remain separately generation-fenced CP-ROUTE inputs.
#[derive(Clone, Debug, PartialEq)]
pub struct RoutingConfig {
    /// Optional backend label used as the strict business-isolation factor.
    pub label_name: Arc<str>,
    /// How backends are partitioned into routeable groups.
    pub routing_rule: RoutingRule,
    /// Ordered balance-factor family.
    pub balance_policy: RoutingBalancePolicy,
    /// Candidate choice policy within the lowest-score set.
    pub selection_policy: RoutingSelectionPolicy,
    /// Backend status factor policy.
    pub status: RoutingFactorConfig,
    /// Backend health factor policy.
    pub health: RoutingFactorConfig,
    /// Backend memory factor policy.
    pub memory: RoutingFactorConfig,
    /// Backend CPU factor policy.
    pub cpu: RoutingFactorConfig,
    /// Proxy/backend locality factor policy.
    pub location: RoutingFactorConfig,
    /// Backend connection-count factor policy.
    pub connection: RoutingConnectionFactorConfig,
    /// Stable, sorted proxy labels used by the locality factor.
    pub proxy_labels: Arc<[(Arc<str>, Arc<str>)]>,
    /// Trimmed, de-duplicated failed backend pod names or addresses, preserving
    /// the configured first-occurrence order.
    pub failed_backends: Arc<[Arc<str>]>,
    /// Grace period before sessions left on a failed backend are force-closed.
    pub failover_timeout_seconds: u64,
}

/// CP-ROUTE's immutable projection of one namespace generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingNamespace {
    /// Namespace name.
    pub name: Arc<str>,
    /// Canonical frontend users (the current legacy shape has one).
    pub users: Arc<[Arc<str>]>,
    /// Static fallback backend endpoints, preserving configured order.
    pub backend_instances: Arc<[Arc<str>]>,
    /// Namespace-specific backend client TLS material and policy.
    pub backend_tls: ClientTlsConfig,
}

/// SQL serving configuration owned by CP-CFG. Protocol-only handshake facts
/// (advertised capability and server version) deliberately remain outside
/// this type while the legacy control bridge still owns them.
#[derive(Clone, Debug, PartialEq)]
pub struct ServingConfig {
    /// Maximum concurrent connections; zero means unlimited.
    pub max_connections: u64,
    /// Process-memory admission threshold.
    pub high_memory_reject_threshold: f64,
    /// Per-connection packet buffer reservation.
    pub connection_buffer_bytes: u32,
    /// Accepted frontend socket keepalive policy.
    pub frontend_keepalive: ServingKeepalive,
    /// Healthy backend socket keepalive policy.
    pub healthy_backend_keepalive: ServingKeepalive,
    /// Unhealthy backend socket keepalive policy.
    pub unhealthy_backend_keepalive: ServingKeepalive,
    /// Whether backend PROXY protocol v2 is enabled.
    pub proxy_protocol_v2: bool,
    /// Whether backend TLS is mandatory.
    pub require_backend_tls: bool,
    /// Delay before coordinated graceful shutdown.
    pub graceful_wait_millis: u64,
    /// Existing-session graceful close deadline.
    pub graceful_close_millis: u64,
    /// Restart-required SQL listeners.
    pub listeners: Arc<[ServingListener]>,
    /// Canonical, sorted public endpoint CIDRs.
    pub public_cidrs: Arc<[Arc<str>]>,
    /// Frontend server TLS policy.
    pub frontend_tls: ServingTlsConfig,
    /// Backend client TLS policy.
    pub backend_tls: ServingTlsConfig,
    /// Traffic capture/replay gate.
    pub traffic_replay_enabled: bool,
}

/// Millisecond socket keepalive projection used by the dataplane.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServingKeepalive {
    /// Whether keepalive is enabled.
    pub enabled: bool,
    /// Idle period before the first probe.
    pub idle_millis: u64,
    /// Maximum probe count.
    pub probe_count: u32,
    /// Delay between probes.
    pub interval_millis: u64,
    /// TCP user timeout.
    pub user_timeout_millis: u64,
}

/// One immutable SQL listener projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingListener {
    /// Stable listener name.
    pub name: Arc<str>,
    /// Host/IP without brackets.
    pub address: Arc<str>,
    /// Nonzero TCP port.
    pub port: u16,
}

/// TLS policy path projection used by the serving snapshot adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingTlsConfig {
    /// Optional server/client certificate path.
    pub certificate_path: Option<Arc<Path>>,
    /// Optional matching private-key path.
    pub private_key_path: Option<Arc<Path>>,
    /// Optional CA bundle path.
    pub ca_path: Option<Arc<Path>>,
    /// Minimum TLS version.
    pub minimum_version: Arc<str>,
    /// Sorted common-name allowlist.
    pub allowed_common_names: Arc<[Arc<str>]>,
    /// Whether CA verification may be skipped.
    pub skip_ca_verification: bool,
}

/// Namespace identity projection owned by CP-CFG while backend/keyspace
/// binding remains with CP-TOPO/CP-ROUTE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingNamespace {
    /// Namespace name.
    pub name: Arc<str>,
    /// Canonical frontend users (the current legacy shape has one).
    pub users: Arc<[Arc<str>]>,
}

/// One persisted namespace value below `/config/ns/<name>`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default)]
pub struct NamespaceConfig {
    /// Namespace name. It must equal the etcd key suffix.
    pub namespace: String,
    /// Frontend identity and TLS policy.
    pub frontend: FrontendNamespaceConfig,
    /// Static backend instances and TLS policy.
    pub backend: BackendNamespaceConfig,
}

/// Frontend namespace identity and TLS policy.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default)]
pub struct FrontendNamespaceConfig {
    /// Frontend username.
    pub user: String,
    /// Frontend TLS policy.
    pub security: TlsConfig,
}

/// Backend namespace instances and TLS policy.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default)]
pub struct BackendNamespaceConfig {
    /// Static backend endpoints.
    pub instances: Vec<String>,
    /// Backend TLS policy.
    pub security: TlsConfig,
}

/// Full effective `TiProxy` configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct EffectiveConfig {
    proxy: ProxyConfig,
    api: ApiConfig,
    workdir: String,
    security: SecurityConfig,
    log: LogConfig,
    balance: BalanceConfig,
    labels: BTreeMap<String, String>,
    ha: HaConfig,
    metering: MeteringConfig,
    enable_traffic_replay: bool,
    rust_dataplane: RustDataplaneConfig,
}

impl Default for EffectiveConfig {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig::default(),
            api: ApiConfig::default(),
            workdir: String::new(),
            security: SecurityConfig::default(),
            log: LogConfig::default(),
            balance: BalanceConfig::default(),
            labels: BTreeMap::new(),
            ha: HaConfig::default(),
            metering: MeteringConfig::default(),
            enable_traffic_replay: true,
            rust_dataplane: RustDataplaneConfig::default(),
        }
    }
}

impl EffectiveConfig {
    /// Applies the process command-line advertise-address override.
    #[must_use]
    pub fn with_advertise_override(mut self, advertise_addr: Option<&str>) -> Self {
        if let Some(advertise_addr) = advertise_addr.filter(|value| !value.is_empty()) {
            advertise_addr.clone_into(&mut self.proxy.advertise_addr);
        }
        self
    }

    /// Validates and applies the same candidate mutations as Go's
    /// `Config.Check`. Serving-only normalization is deferred to projections
    /// so the effective-config checksum remains byte-identical to Go.
    ///
    /// # Errors
    ///
    /// Returns a stable field/class error for invalid values.
    pub fn validated(mut self, current_dir: &Path) -> Result<Self, ConfigError> {
        if self.workdir.is_empty() {
            self.workdir = current_dir.join("work").to_string_lossy().into_owned();
        }
        validate_proxy(&mut self.proxy)?;
        validate_balance(&mut self.balance)?;
        validate_tls("security.server-tls", &self.security.server_tls)?;
        validate_tls("security.server-http-tls", &self.security.server_http_tls)?;
        validate_tls("security.cluster-tls", &self.security.cluster_tls)?;
        validate_tls("security.sql-tls", &self.security.sql_tls)?;
        if self.ha.garp_burst_count < 0 {
            return invalid("ha.garp-burst-count", "negative");
        }
        if self.ha.garp_burst_count == 0 {
            self.ha.garp_burst_count = 1;
        }
        if self.ha.garp_refresh_count < 0 {
            return invalid("ha.garp-refresh-count", "negative");
        }
        if self.rust_dataplane.allowed_uid < -1
            || self.rust_dataplane.allowed_uid > i64::from(u32::MAX)
        {
            return invalid("rust-dataplane.allowed-uid", "out_of_range");
        }
        if !self.rust_dataplane.control_socket.is_empty()
            && !Path::new(&self.rust_dataplane.control_socket).is_absolute()
        {
            return invalid("rust-dataplane.control-socket", "relative_path");
        }
        if !self.rust_dataplane.routing_shadow_socket.is_empty()
            && (!self.rust_dataplane.enabled
                || !Path::new(&self.rust_dataplane.routing_shadow_socket).is_absolute()
                || self.rust_dataplane.routing_shadow_socket == self.rust_dataplane.control_socket)
        {
            return invalid(
                "rust-dataplane.routing-shadow-socket",
                "requires_rust_and_distinct_absolute_path",
            );
        }
        if self.rust_dataplane.enabled && self.enable_traffic_replay {
            return invalid("enable-traffic-replay", "conflicts_with_rust_dataplane");
        }
        validate_metrics_owner_port(
            &self.proxy,
            &self.api,
            self.rust_dataplane.metrics_owner_port,
        )?;
        // These conversions used to happen only inside Go's bridge adapter.
        // Validate them before publishing a Rust-owned generation so the
        // process-local source can never get ahead of the serving projection.
        let _ = self.serving()?;
        let _ = self.routing()?;
        Ok(self)
    }

    /// Rejects a hot reload that changes any restart-required field.
    /// Routing-rule updates are accepted for future router incarnations; an
    /// existing router retains the rule it read during construction, like Go.
    ///
    /// # Errors
    ///
    /// Returns the first stable restart-required field that differs.
    pub fn check_reload_from(&self, previous: &Self) -> Result<(), ConfigError> {
        let checks = [
            (self.workdir != previous.workdir, "workdir"),
            (self.proxy.addr != previous.proxy.addr, "proxy.addr"),
            (
                self.proxy.advertise_addr != previous.proxy.advertise_addr,
                "proxy.advertise-addr",
            ),
            (
                self.proxy.pd_addrs != previous.proxy.pd_addrs,
                "proxy.pd-addrs",
            ),
            (
                self.proxy.port_range != previous.proxy.port_range,
                "proxy.port-range",
            ),
            (self.api != previous.api, "api"),
            (self.log.encoder != previous.log.encoder, "log.encoder"),
            (self.log.simple != previous.log.simple, "log.simple"),
            (self.ha != previous.ha, "ha"),
            (self.metering != previous.metering, "metering"),
            (
                self.rust_dataplane != previous.rust_dataplane,
                "rust-dataplane",
            ),
        ];
        if let Some((_, field)) = checks.into_iter().find(|(changed, _)| *changed) {
            return Err(ConfigError::RestartRequired { field });
        }
        Ok(())
    }

    /// Returns the normalized topology projection.
    ///
    /// # Errors
    ///
    /// Returns a stable error when the SQL/API addresses cannot produce the
    /// self-registration identity.
    pub fn topology(&self) -> Result<TopologyConfig, ConfigError> {
        // Reuse the SQL serving projection so the registration port is the
        // first expanded listener's port (a port range contributes its first
        // port), instead of re-splitting the raw `proxy.addr`.
        let listeners = serving_listeners(&self.proxy)?;
        let first = listeners.first().ok_or(ConfigError::InvalidField {
            field: "proxy.addr",
            class: "no_listener",
        })?;
        let bind_sql_host = Arc::clone(&first.address);
        let sql_port = first.port;
        let (_, status_port) = split_host_port(&self.api.addr, "api.addr")?;
        let advertise_trimmed = self.proxy.advertise_addr.trim();
        let advertise_host_override = if advertise_trimmed.is_empty() {
            None
        } else {
            Some(Arc::from(advertise_trimmed))
        };
        let backend_clusters = self.normalized_backend_clusters()?;
        Ok(TopologyConfig {
            advertise_host_override,
            bind_sql_host,
            sql_port,
            status_port,
            metrics_owner_port: self.rust_dataplane.metrics_owner_port,
            ha_virtual_ip: Arc::from(self.ha.virtual_ip.as_str()),
            backend_clusters: Arc::from(backend_clusters),
            cluster_tls: client_tls(&self.security.cluster_tls),
        })
    }

    /// Returns the restart-pinned PD/etcd transport used by CP-CFG.
    ///
    /// An empty legacy `proxy.pd-addrs` disables persistent config exactly as
    /// it does in Go. Explicit dynamic backend clusters remain CP-TOPO inputs
    /// and never redirect this already-running ownership session.
    #[must_use]
    pub fn config_persistence(&self) -> Option<ConfigPersistence> {
        let pd_addrs = split_addr_list(&self.proxy.pd_addrs)
            .into_iter()
            .map(Arc::<str>::from)
            .collect::<Vec<_>>();
        if pd_addrs.is_empty() {
            return None;
        }
        Some(ConfigPersistence {
            pd_addrs: Arc::from(pd_addrs),
            cluster_tls: client_tls(&self.security.cluster_tls),
        })
    }

    /// Projects an enabled VIP only for a single backend cluster. The
    /// configured address and interface must both be present. Go elects via
    /// that cluster's PD endpoints, which may differ from `proxy.pd-addrs`.
    ///
    /// # Errors
    ///
    /// Returns an invalid VIP address, backend normalization error or an
    /// unrepresentable GARP count. Config validation already rejects negative
    /// counts and gives a zero burst count the Go-compatible value of one.
    pub fn vip(&self) -> Result<Option<VipConfig>, ConfigError> {
        if self.ha.virtual_ip.is_empty() || self.ha.interface.is_empty() {
            return Ok(None);
        }
        let clusters = self.normalized_backend_clusters()?;
        let [cluster] = clusters.as_slice() else {
            return Ok(None);
        };
        let (ip, prefix_len) = parse_vip_address(&self.ha.virtual_ip)?;
        let garp_burst_count =
            u64::try_from(self.ha.garp_burst_count).map_err(|_| ConfigError::InvalidField {
                field: "ha.garp-burst-count",
                class: "out_of_range",
            })?;
        let garp_refresh_count =
            u64::try_from(self.ha.garp_refresh_count).map_err(|_| ConfigError::InvalidField {
                field: "ha.garp-refresh-count",
                class: "out_of_range",
            })?;
        Ok(Some(VipConfig {
            address: Arc::from(self.ha.virtual_ip.as_str()),
            ip,
            prefix_len,
            interface: Arc::from(self.ha.interface.as_str()),
            pd_addrs: Arc::clone(&cluster.pd_addrs),
            cluster_tls: client_tls(&self.security.cluster_tls),
            garp_burst_count,
            garp_refresh_count,
        }))
    }

    /// Returns the complete SQL-serving projection formerly built by Go.
    ///
    /// # Errors
    ///
    /// Returns a stable field/class error for a value that cannot be
    /// represented by the serving contract.
    pub fn serving(&self) -> Result<ServingConfig, ConfigError> {
        let listeners = serving_listeners(&self.proxy)?;
        let public_cidrs = normalize_public_endpoints(&self.proxy.online.public_endpoints)?;
        Ok(ServingConfig {
            max_connections: self.proxy.online.max_connections,
            high_memory_reject_threshold: self.proxy.online.high_memory_usage_reject_threshold,
            connection_buffer_bytes: if self.proxy.online.conn_buffer_size == 0 {
                DEFAULT_CONNECTION_BUFFER_BYTES
            } else {
                u32::try_from(self.proxy.online.conn_buffer_size).map_err(|_| {
                    ConfigError::InvalidField {
                        field: "proxy.conn-buffer-size",
                        class: "out_of_range",
                    }
                })?
            },
            frontend_keepalive: serving_keepalive(
                "proxy.frontend-keepalive",
                &self.proxy.online.frontend_keepalive,
            )?,
            healthy_backend_keepalive: serving_keepalive(
                "proxy.backend-healthy-keepalive",
                &self.proxy.online.backend_healthy_keepalive,
            )?,
            unhealthy_backend_keepalive: serving_keepalive(
                "proxy.backend-unhealthy-keepalive",
                &self.proxy.online.backend_unhealthy_keepalive,
            )?,
            proxy_protocol_v2: self.proxy.online.proxy_protocol == "v2",
            require_backend_tls: self.security.require_backend_tls,
            graceful_wait_millis: seconds_to_millis(
                "proxy.graceful-wait-before-shutdown",
                self.proxy.online.graceful_wait_before_shutdown,
            )?,
            graceful_close_millis: seconds_to_millis(
                "proxy.graceful-close-conn-timeout",
                self.proxy.online.graceful_close_conn_timeout,
            )?,
            listeners: Arc::from(listeners),
            public_cidrs: Arc::from(public_cidrs),
            frontend_tls: serving_tls(&self.security.server_tls),
            backend_tls: serving_tls(&self.security.sql_tls),
            traffic_replay_enabled: self.enable_traffic_replay,
        })
    }

    /// Returns the complete normalized CP-ROUTE policy projection.
    ///
    /// The returned value contains no live authority. A routing consumer must
    /// retain and revalidate the exact
    /// [`ConfigNamespaceSnapshot`](crate::ConfigNamespaceSnapshot) from which
    /// this configuration was read before committing any reservation or
    /// accounting side effect.
    ///
    /// # Errors
    ///
    /// Returns a stable field/class error if a raw, unvalidated value cannot be
    /// represented by the routing contract. Normal config-store snapshots have
    /// already passed this projection during candidate validation.
    pub fn routing(&self) -> Result<RoutingConfig, ConfigError> {
        let routing_rule = match self.balance.routing_rule.as_str() {
            "" => RoutingRule::MatchAll,
            "client_cidr" => RoutingRule::ClientCidr,
            "proxy_cidr" => RoutingRule::ProxyCidr,
            "port" => RoutingRule::ListenerPort,
            _ => return invalid("balance.routing-rule", "unsupported"),
        };
        let balance_policy = match self.balance.policy.as_str() {
            "resource" => RoutingBalancePolicy::Resource,
            "location" => RoutingBalancePolicy::Location,
            "connection" => RoutingBalancePolicy::Connection,
            _ => return invalid("balance.policy", "unsupported"),
        };
        let selection_policy = match self.balance.routing_policy.as_str() {
            "prefer-idle" => RoutingSelectionPolicy::PreferIdle,
            "random" => RoutingSelectionPolicy::Random,
            _ => return invalid("balance.routing-policy", "unsupported"),
        };
        let failover_timeout_seconds =
            u64::try_from(self.proxy.online.failover_timeout).map_err(|_| {
                ConfigError::InvalidField {
                    field: "proxy.failover-timeout",
                    class: "out_of_range",
                }
            })?;
        let factor = |value: &FactorConfig| RoutingFactorConfig {
            migrations_per_second: value.migrations_per_second,
        };
        Ok(RoutingConfig {
            label_name: Arc::from(self.balance.label_name.as_str()),
            routing_rule,
            balance_policy,
            selection_policy,
            status: factor(&self.balance.status),
            health: factor(&self.balance.health),
            memory: factor(&self.balance.memory),
            cpu: factor(&self.balance.cpu),
            location: factor(&self.balance.location),
            connection: RoutingConnectionFactorConfig {
                migrations_per_second: self.balance.conn_count.factor.migrations_per_second,
                count_ratio_threshold: self.balance.conn_count.count_ratio_threshold,
            },
            proxy_labels: Arc::from(
                self.labels
                    .iter()
                    .map(|(name, value)| {
                        (
                            Arc::<str>::from(name.as_str()),
                            Arc::<str>::from(value.as_str()),
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            failed_backends: Arc::from(
                self.proxy
                    .online
                    .fail_backend_list
                    .iter()
                    .map(|backend| Arc::<str>::from(backend.as_str()))
                    .collect::<Vec<_>>(),
            ),
            failover_timeout_seconds,
        })
    }

    /// Returns the explicit, restart-pinned routing observation socket.
    #[must_use]
    pub fn routing_shadow_socket(&self) -> Option<&Path> {
        (!self.rust_dataplane.routing_shadow_socket.is_empty())
            .then(|| Path::new(&self.rust_dataplane.routing_shadow_socket))
    }

    /// Returns the process TLS allowlist configured for the Rust dataplane.
    #[must_use]
    pub fn rust_tls_allowed_roots(&self) -> &[String] {
        &self.rust_dataplane.tls_allowed_roots
    }

    /// Returns the dynamic log level spelling.
    #[must_use]
    pub fn log_level(&self) -> &str {
        &self.log.online.level
    }

    /// Returns the `log.encoder` spelling (restart-required; Go selects
    /// `json`/`console` case-sensitively and treats anything else as `tidb`).
    #[must_use]
    pub fn log_encoder(&self) -> &str {
        &self.log.encoder
    }

    /// Returns `log.simple` (restart-required): the Go encoders drop the
    /// time, level, caller and message keys.
    #[must_use]
    pub const fn log_simple(&self) -> bool {
        self.log.simple
    }

    /// Returns restart-pinned metering settings without copying credentials.
    #[must_use]
    pub const fn metering(&self) -> &MeteringConfig {
        &self.metering
    }

    /// Returns the canonical effective work directory.
    #[must_use]
    pub fn workdir(&self) -> &str {
        &self.workdir
    }

    /// Returns the management API listen address (`api.addr`). Under
    /// `RUST_API_OWNER` the Rust process binds it itself.
    #[must_use]
    pub fn api_addr(&self) -> &str {
        &self.api.addr
    }

    /// Returns whether traffic replay is enabled.
    #[must_use]
    pub const fn traffic_replay_enabled(&self) -> bool {
        self.enable_traffic_replay
    }

    /// Returns the full frontend SQL TLS policy.
    #[must_use]
    pub const fn frontend_tls(&self) -> &TlsConfig {
        &self.security.server_tls
    }

    /// Returns the full backend SQL TLS policy.
    #[must_use]
    pub const fn backend_tls(&self) -> &TlsConfig {
        &self.security.sql_tls
    }

    /// Returns the HTTP server TLS policy consumed by CP-ADMIN.
    #[must_use]
    pub const fn server_http_tls(&self) -> &TlsConfig {
        &self.security.server_http_tls
    }

    /// Returns the control-plane client TLS policy consumed by CP-TOPO.
    #[must_use]
    pub const fn cluster_tls(&self) -> &TlsConfig {
        &self.security.cluster_tls
    }

    /// Returns every configured TLS material path owned by this process.
    ///
    /// The returned paths preserve the effective configuration spelling. The
    /// module uses this view only to detect material changes; candidate
    /// validation remains responsible for loading and parsing the files.
    #[must_use]
    pub fn tls_material_paths(&self) -> Vec<&Path> {
        let mut paths = Vec::new();
        self.security.server_tls.append_material_paths(&mut paths);
        self.security
            .server_http_tls
            .append_material_paths(&mut paths);
        self.security.cluster_tls.append_material_paths(&mut paths);
        self.security.sql_tls.append_material_paths(&mut paths);
        paths
    }

    /// Returns whether frontend SQL TLS requests Go's process-local
    /// auto-certificate generator. Client-side `sql-tls.auto-certs` is
    /// intentionally ignored, matching Go's `CertInfo::buildClientConfig`.
    /// The Rust dataplane rejects only the unsupported server generator.
    #[must_use]
    pub const fn serving_auto_certs_enabled(&self) -> bool {
        self.security.server_tls.auto_certs
    }

    /// Replaces the dynamic proxy subset decoded from `/config/proxy`.
    pub fn apply_proxy_online(&mut self, value: ProxyOnlineConfig) {
        self.proxy.online = value;
    }

    /// Replaces the dynamic log subset decoded from `/config/log`.
    pub fn apply_log_online(&mut self, value: LogOnlineConfig) {
        self.log.online = value;
    }

    /// Returns the legacy persistent proxy subset.
    #[must_use]
    pub fn proxy_online(&self) -> &ProxyOnlineConfig {
        &self.proxy.online
    }

    /// Returns the legacy persistent log subset.
    #[must_use]
    pub fn log_online(&self) -> &LogOnlineConfig {
        &self.log.online
    }

    fn normalized_backend_clusters(&self) -> Result<Vec<BackendClusterConfig>, ConfigError> {
        let clusters = if self.proxy.online.backend_clusters.is_empty() {
            if self.proxy.pd_addrs.trim().is_empty() {
                Vec::new()
            } else {
                vec![BackendCluster {
                    name: DEFAULT_CLUSTER_NAME.to_owned(),
                    pd_addrs: self.proxy.pd_addrs.clone(),
                    ns_servers: Vec::new(),
                }]
            }
        } else {
            self.proxy.online.backend_clusters.clone()
        };
        let mut normalized = Vec::with_capacity(clusters.len());
        for cluster in clusters {
            let pd_addrs = split_addr_list(&cluster.pd_addrs)
                .into_iter()
                .map(Arc::<str>::from)
                .collect::<Vec<_>>();
            if pd_addrs.is_empty() {
                return invalid("proxy.backend-clusters.pd-addrs", "empty");
            }
            // Mirror Go `normalizeCluster` exactly: it runs
            // `sort.Strings(cluster.NSServers)` on the RAW input strings FIRST,
            // and only THEN normalizes each entry. Sorting the raw strings (not
            // the normalized ones) matters because a bare IPv6 literal normalizes
            // to a bracketed `[..]:53` whose sort key differs from its raw key, so
            // sort-after-normalize would reorder relative to Go and shift the
            // resolver's round-robin start. Duplicates are preserved (Go does not
            // de-duplicate).
            let mut raw_ns_servers: Vec<&str> =
                cluster.ns_servers.iter().map(String::as_str).collect();
            raw_ns_servers.sort_unstable();
            let ns_servers = raw_ns_servers
                .into_iter()
                .map(|server| normalize_ns_server(server).map(Arc::<str>::from))
                .collect::<Result<Vec<_>, _>>()?;
            normalized.push(BackendClusterConfig {
                name: Arc::from(cluster.name.trim()),
                pd_addrs: Arc::from(pd_addrs),
                ns_servers: Arc::from(ns_servers),
            });
        }
        normalized.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(normalized)
    }

    /// Renders the configuration as a TOML document for
    /// `GET /api/admin/config/`: the same document shape `SetTOMLConfig`
    /// accepts. Bytes follow this crate's encoder, not gin's.
    ///
    /// # Errors
    ///
    /// Returns the encoder error for an unrepresentable value.
    pub fn to_toml_string(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }

    /// Renders the configuration as JSON for `GET /api/admin/config/?format=json`.
    ///
    /// # Errors
    ///
    /// Returns the encoder error for an unrepresentable value.
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// CRC32-IEEE over the Go-encoded TOML, the value Go's `ConfigManager`
    /// reports as the config checksum. CP-ADMIN serves it from `/api/debug/health`.
    #[must_use]
    pub fn go_checksum(&self) -> u32 {
        crc32fast::hash(self.encode_go_toml().as_bytes())
    }

    /// Encodes the exact byte representation used by Go's `BurntSushi` TOML
    /// encoder before `ConfigManager` computes its CRC32 checksum.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn encode_go_toml(&self) -> String {
        let mut output = GoToml::default();
        output.string(0, "workdir", &self.workdir);
        output.boolean(0, "enable-traffic-replay", self.enable_traffic_replay);

        output.top_table("proxy");
        output.string(1, "addr", &self.proxy.addr);
        output.string(1, "advertise-addr", &self.proxy.advertise_addr);
        output.string(1, "pd-addrs", &self.proxy.pd_addrs);
        output.integer_list(1, "port-range", &self.proxy.port_range);
        output.unsigned(1, "max-connections", self.proxy.online.max_connections);
        output.float(
            1,
            "high-memory-usage-reject-threshold",
            self.proxy.online.high_memory_usage_reject_threshold,
        );
        output.signed(1, "conn-buffer-size", self.proxy.online.conn_buffer_size);
        output.string(1, "proxy-protocol", &self.proxy.online.proxy_protocol);
        output.signed(
            1,
            "graceful-wait-before-shutdown",
            self.proxy.online.graceful_wait_before_shutdown,
        );
        output.signed(
            1,
            "graceful-close-conn-timeout",
            self.proxy.online.graceful_close_conn_timeout,
        );
        output.string_list(1, "public-endpoints", &self.proxy.online.public_endpoints);
        output.string_list(1, "fail-backend-list", &self.proxy.online.fail_backend_list);
        output.signed(1, "failover-timeout", self.proxy.online.failover_timeout);
        output.keepalive(
            "proxy.frontend-keepalive",
            &self.proxy.online.frontend_keepalive,
        );
        output.keepalive(
            "proxy.backend-healthy-keepalive",
            &self.proxy.online.backend_healthy_keepalive,
        );
        output.keepalive(
            "proxy.backend-unhealthy-keepalive",
            &self.proxy.online.backend_unhealthy_keepalive,
        );
        for cluster in &self.proxy.online.backend_clusters {
            output.array_table("proxy.backend-clusters", 1);
            output.string(2, "name", &cluster.name);
            output.string(2, "pd-addrs", &cluster.pd_addrs);
            output.string_list(2, "ns-servers", &cluster.ns_servers);
        }

        if !self.api.addr.is_empty() || !self.api.proxy_protocol.is_empty() {
            output.top_table("api");
            output.string(1, "addr", &self.api.addr);
            output.string(1, "proxy-protocol", &self.api.proxy_protocol);
        }

        output.top_table("security");
        output.string(1, "encryption-key-path", &self.security.encryption_key_path);
        output.boolean(1, "require-backend-tls", self.security.require_backend_tls);
        output.tls("security.server-tls", &self.security.server_tls);
        output.tls("security.server-http-tls", &self.security.server_http_tls);
        output.tls("security.cluster-tls", &self.security.cluster_tls);
        output.tls("security.sql-tls", &self.security.sql_tls);

        output.top_table("log");
        output.string(1, "encoder", &self.log.encoder);
        output.boolean(1, "simple", self.log.simple);
        output.string(1, "level", &self.log.online.level);
        if !self.log.online.log_file.is_zero() {
            output.sub_table("log.log-file", 1);
            output.string(2, "filename", &self.log.online.log_file.filename);
            output.signed(2, "max-size", self.log.online.log_file.max_size);
            output.signed(2, "max-days", self.log.online.log_file.max_days);
            output.signed(2, "max-backups", self.log.online.log_file.max_backups);
        }

        if !self.balance.is_zero() {
            output.top_table("balance");
            output.string(1, "label-name", &self.balance.label_name);
            output.string(1, "routing-rule", &self.balance.routing_rule);
            output.string(1, "policy", &self.balance.policy);
            output.string(1, "routing-policy", &self.balance.routing_policy);
            output.factor("balance.status", self.balance.status);
            output.factor("balance.health", self.balance.health);
            output.factor("balance.memory", self.balance.memory);
            output.factor("balance.cpu", self.balance.cpu);
            output.factor("balance.location", self.balance.location);
            if !self.balance.conn_count.is_zero() {
                output.sub_table("balance.conn-count", 1);
                output.float(
                    2,
                    "migrations-per-second",
                    self.balance.conn_count.factor.migrations_per_second,
                );
                output.float(
                    2,
                    "count-ratio-threshold",
                    self.balance.conn_count.count_ratio_threshold,
                );
            }
        }

        if !self.labels.is_empty() {
            output.top_table("labels");
            for (key, value) in &self.labels {
                output.string_named(1, key, value, false);
            }
        }

        if !self.ha.is_zero() {
            output.top_table("ha");
            output.string(1, "virtual-ip", &self.ha.virtual_ip);
            output.string(1, "interface", &self.ha.interface);
            output.signed(1, "garp-burst-count", self.ha.garp_burst_count);
            output.signed(1, "garp-refresh-count", self.ha.garp_refresh_count);
        }

        if !self.metering.is_zero() {
            output.top_table("metering");
            output.string(1, "type", &self.metering.provider_type);
            output.string(1, "region", &self.metering.region);
            output.string(1, "bucket", &self.metering.bucket);
            output.string(1, "prefix", &self.metering.prefix);
            output.string(1, "endpoint", &self.metering.endpoint);
            output.string(1, "shared-pool-id", &self.metering.shared_pool_id);
            if let Some(aws) = &self.metering.aws {
                output.sub_table("metering.aws", 1);
                output.string(2, "assume-role-arn", &aws.assume_role_arn);
                output.boolean(2, "s3-force-path-style", aws.s3_force_path_style);
                output.string(2, "access-key", &aws.access_key);
                output.string(2, "secret-access-key", &aws.secret_access_key);
                output.string(2, "session-token", &aws.session_token);
            }
            if let Some(oss) = &self.metering.oss {
                output.cloud_metering("metering.oss", oss);
            }
            if let Some(cos) = &self.metering.cos {
                output.cloud_metering("metering.cos", cos);
            }
            if let Some(azure) = &self.metering.azure {
                output.sub_table("metering.azure", 1);
                output.string(2, "account-name", &azure.account_name);
                output.string(2, "account-key", &azure.account_key);
                output.string(2, "sas-token", &azure.sas_token);
            }
            if let Some(localfs) = &self.metering.localfs {
                output.sub_table("metering.localfs", 1);
                output.string(2, "base-path", &localfs.base_path);
                output.boolean(2, "create-dirs", localfs.create_dirs);
                output.string(2, "permissions", &localfs.permissions);
            }
        }

        output.top_table("rust-dataplane");
        output.string(
            1,
            "routing-shadow-socket",
            &self.rust_dataplane.routing_shadow_socket,
        );
        output.boolean(1, "enabled", self.rust_dataplane.enabled);
        output.string(1, "control-socket", &self.rust_dataplane.control_socket);
        output.signed(1, "allowed-uid", self.rust_dataplane.allowed_uid);
        output.string_list(
            1,
            "tls-allowed-roots",
            &self.rust_dataplane.tls_allowed_roots,
        );
        output.unsigned(
            1,
            "metrics-owner-port",
            self.rust_dataplane.metrics_owner_port,
        );
        output.0
    }
}

impl NamespaceConfig {
    /// Returns the CP-CFG-owned routing identity projection.
    #[must_use]
    pub fn serving(&self) -> ServingNamespace {
        let users = if self.frontend.user.is_empty() {
            Arc::from([])
        } else {
            Arc::from([Arc::<str>::from(self.frontend.user.as_str())])
        };
        ServingNamespace {
            name: Arc::from(self.namespace.as_str()),
            users,
        }
    }

    /// Returns the CP-ROUTE-owned binding projection for this namespace.
    #[must_use]
    pub fn routing(&self) -> RoutingNamespace {
        let serving = self.serving();
        RoutingNamespace {
            name: serving.name,
            users: serving.users,
            backend_instances: Arc::from(
                self.backend
                    .instances
                    .iter()
                    .map(|address| Arc::<str>::from(address.as_str()))
                    .collect::<Vec<_>>(),
            ),
            backend_tls: client_tls(&self.backend.security),
        }
    }

    /// Appends every frontend/backend TLS material path referenced by this
    /// namespace.
    pub fn append_tls_material_paths<'a>(&'a self, paths: &mut Vec<&'a Path>) {
        self.frontend.security.append_material_paths(paths);
        self.backend.security.append_material_paths(paths);
    }

    /// Returns this namespace's future frontend server TLS policy.
    #[must_use]
    pub const fn frontend_tls(&self) -> &TlsConfig {
        &self.frontend.security
    }

    /// Returns this namespace's future backend client TLS policy.
    #[must_use]
    pub const fn backend_tls(&self) -> &TlsConfig {
        &self.backend.security
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct ProxyConfig {
    addr: String,
    advertise_addr: String,
    pd_addrs: String,
    port_range: Vec<i32>,
    #[serde(flatten)]
    online: ProxyOnlineConfig,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            addr: DEFAULT_SQL_ADDR.to_owned(),
            advertise_addr: String::new(),
            pd_addrs: DEFAULT_PD_ADDRS.to_owned(),
            port_range: Vec::new(),
            online: ProxyOnlineConfig::default(),
        }
    }
}

/// Dynamic proxy-server subset stored at `/config/proxy`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ProxyOnlineConfig {
    max_connections: u64,
    high_memory_usage_reject_threshold: f64,
    conn_buffer_size: i64,
    frontend_keepalive: KeepAliveConfig,
    backend_healthy_keepalive: KeepAliveConfig,
    backend_unhealthy_keepalive: KeepAliveConfig,
    proxy_protocol: String,
    graceful_wait_before_shutdown: i64,
    graceful_close_conn_timeout: i64,
    public_endpoints: Vec<String>,
    backend_clusters: Vec<BackendCluster>,
    fail_backend_list: Vec<String>,
    failover_timeout: i64,
}

impl Default for ProxyOnlineConfig {
    fn default() -> Self {
        Self {
            max_connections: 0,
            high_memory_usage_reject_threshold: 0.9,
            conn_buffer_size: 0,
            frontend_keepalive: KeepAliveConfig {
                enabled: true,
                ..KeepAliveConfig::default()
            },
            backend_healthy_keepalive: KeepAliveConfig {
                enabled: true,
                idle: 60 * NANOS_PER_SECOND,
                count: 5,
                interval: 3 * NANOS_PER_SECOND,
                timeout: 15 * NANOS_PER_SECOND,
            },
            backend_unhealthy_keepalive: KeepAliveConfig {
                enabled: true,
                idle: 10 * NANOS_PER_SECOND,
                count: 5,
                interval: NANOS_PER_SECOND,
                timeout: 5 * NANOS_PER_SECOND,
            },
            proxy_protocol: String::new(),
            graceful_wait_before_shutdown: 0,
            graceful_close_conn_timeout: 15,
            public_endpoints: Vec::new(),
            backend_clusters: Vec::new(),
            fail_backend_list: Vec::new(),
            failover_timeout: 60,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct KeepAliveConfig {
    enabled: bool,
    #[serde(deserialize_with = "deserialize_go_duration")]
    idle: i64,
    #[serde(rename = "cnt")]
    count: i64,
    #[serde(rename = "intvl", deserialize_with = "deserialize_go_duration")]
    interval: i64,
    #[serde(deserialize_with = "deserialize_go_duration")]
    timeout: i64,
}

fn deserialize_go_duration<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct GoDurationVisitor;

    impl Visitor<'_> for GoDurationVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a Go duration string or signed nanosecond integer")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value).map_err(|_| E::custom("duration exceeds int64"))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_go_duration(value).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(GoDurationVisitor)
}

fn parse_go_duration(value: &str) -> Result<i64, &'static str> {
    let (negative, mut rest) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err("invalid Go duration");
    }

    let mut total = 0_i128;
    while !rest.is_empty() {
        let integer_end = rest
            .as_bytes()
            .iter()
            .position(|byte| !byte.is_ascii_digit())
            .unwrap_or(rest.len());
        let integer_text = &rest[..integer_end];
        rest = &rest[integer_end..];

        let mut fraction_text = "";
        if let Some(after_dot) = rest.strip_prefix('.') {
            let fraction_end = after_dot
                .as_bytes()
                .iter()
                .position(|byte| !byte.is_ascii_digit())
                .unwrap_or(after_dot.len());
            fraction_text = &after_dot[..fraction_end];
            rest = &after_dot[fraction_end..];
        }
        if integer_text.is_empty() && fraction_text.is_empty() {
            return Err("invalid Go duration number");
        }

        let unit_end = rest
            .char_indices()
            .find_map(|(index, character)| {
                (character.is_ascii_digit() || character == '.').then_some(index)
            })
            .unwrap_or(rest.len());
        let unit_text = &rest[..unit_end];
        rest = &rest[unit_end..];
        let unit_nanos = match unit_text {
            "ns" => 1_i128,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 60 * 60 * 1_000_000_000,
            _ => return Err("invalid Go duration unit"),
        };

        let integer = if integer_text.is_empty() {
            0
        } else {
            integer_text
                .parse::<i128>()
                .map_err(|_| "Go duration integer overflow")?
        };
        let mut component = integer
            .checked_mul(unit_nanos)
            .ok_or("Go duration overflow")?;
        if !fraction_text.is_empty() {
            let fraction = fraction_text
                .parse::<i128>()
                .map_err(|_| "Go duration fraction overflow")?;
            let scale = 10_i128
                .checked_pow(
                    u32::try_from(fraction_text.len())
                        .map_err(|_| "Go duration fraction overflow")?,
                )
                .ok_or("Go duration fraction overflow")?;
            component = component
                .checked_add(
                    fraction
                        .checked_mul(unit_nanos)
                        .ok_or("Go duration overflow")?
                        / scale,
                )
                .ok_or("Go duration overflow")?;
        }
        total = total.checked_add(component).ok_or("Go duration overflow")?;
    }

    let signed = if negative {
        total.checked_neg().ok_or("Go duration overflow")?
    } else {
        total
    };
    i64::try_from(signed).map_err(|_| "Go duration exceeds int64")
}

pub(crate) fn format_go_duration(value: i64) -> String {
    let negative = value.is_negative();
    let nanos = u128::from(value.unsigned_abs());
    if nanos == 0 {
        return "0s".to_owned();
    }
    let sign = if negative { "-" } else { "" };
    if nanos < 1_000 {
        return format!("{sign}{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_decimal(sign, nanos / 1_000, nanos % 1_000, 3, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_decimal(sign, nanos / 1_000_000, nanos % 1_000_000, 6, "ms");
    }

    let total_seconds = nanos / 1_000_000_000;
    let fractional_nanos = nanos % 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let mut formatted = String::from(sign);
    if hours > 0 {
        formatted.push_str(&hours.to_string());
        formatted.push('h');
    }
    if hours > 0 || minutes > 0 {
        formatted.push_str(&minutes.to_string());
        formatted.push('m');
    }
    formatted.push_str(&format_decimal("", seconds, fractional_nanos, 9, "s"));
    formatted
}

fn format_decimal(sign: &str, integer: u128, fraction: u128, width: usize, unit: &str) -> String {
    if fraction == 0 {
        return format!("{sign}{integer}{unit}");
    }
    let mut fraction = format!("{fraction:0width$}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    format!("{sign}{integer}.{fraction}{unit}")
}

#[derive(Default)]
struct GoToml(String);

impl GoToml {
    fn top_table(&mut self, path: &str) {
        if !self.0.is_empty() {
            self.0.push('\n');
        }
        self.0.push('[');
        self.0.push_str(path);
        self.0.push_str("]\n");
    }

    fn sub_table(&mut self, path: &str, depth: usize) {
        self.indent(depth);
        self.0.push('[');
        self.0.push_str(path);
        self.0.push_str("]\n");
    }

    fn array_table(&mut self, path: &str, depth: usize) {
        if !self.0.is_empty() {
            self.0.push('\n');
        }
        self.indent(depth);
        self.0.push_str("[[");
        self.0.push_str(path);
        self.0.push_str("]]\n");
    }

    fn string(&mut self, depth: usize, key: &str, value: &str) {
        self.string_named(depth, key, value, true);
    }

    fn string_named(&mut self, depth: usize, key: &str, value: &str, omit_empty: bool) {
        if omit_empty && value.is_empty() {
            return;
        }
        self.key(depth, key);
        self.0.push_str(" = ");
        self.0.push_str(&quote_toml(value));
        self.0.push('\n');
    }

    fn boolean(&mut self, depth: usize, key: &str, value: bool) {
        if !value {
            return;
        }
        self.key(depth, key);
        self.0.push_str(" = true\n");
    }

    fn signed<T>(&mut self, depth: usize, key: &str, value: T)
    where
        T: fmt::Display,
    {
        self.key(depth, key);
        self.0.push_str(" = ");
        self.0.push_str(&value.to_string());
        self.0.push('\n');
    }

    fn unsigned<T>(&mut self, depth: usize, key: &str, value: T)
    where
        T: fmt::Display,
    {
        self.signed(depth, key, value);
    }

    fn float(&mut self, depth: usize, key: &str, value: f64) {
        self.key(depth, key);
        self.0.push_str(" = ");
        let mut formatted = if value.is_nan() {
            if value.is_sign_negative() {
                "-nan".to_owned()
            } else {
                "nan".to_owned()
            }
        } else if value.is_infinite() {
            if value.is_sign_negative() {
                "-inf".to_owned()
            } else {
                "inf".to_owned()
            }
        } else {
            value.to_string()
        };
        if !formatted.contains('.') && !formatted.contains(['e', 'E']) {
            formatted.push_str(".0");
        }
        self.0.push_str(&formatted);
        self.0.push('\n');
    }

    fn string_list(&mut self, depth: usize, key: &str, values: &[String]) {
        if values.is_empty() {
            return;
        }
        self.key(depth, key);
        self.0.push_str(" = [");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.0.push_str(", ");
            }
            self.0.push_str(&quote_toml(value));
        }
        self.0.push_str("]\n");
    }

    fn integer_list(&mut self, depth: usize, key: &str, values: &[i32]) {
        if values.is_empty() {
            return;
        }
        self.key(depth, key);
        self.0.push_str(" = [");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.0.push_str(", ");
            }
            self.0.push_str(&value.to_string());
        }
        self.0.push_str("]\n");
    }

    fn keepalive(&mut self, path: &str, keepalive: &KeepAliveConfig) {
        self.sub_table(path, 1);
        self.boolean(2, "enabled", keepalive.enabled);
        self.string_named(2, "idle", &format_go_duration(keepalive.idle), false);
        self.signed(2, "cnt", keepalive.count);
        self.string_named(2, "intvl", &format_go_duration(keepalive.interval), false);
        self.string_named(2, "timeout", &format_go_duration(keepalive.timeout), false);
    }

    fn tls(&mut self, path: &str, tls: &TlsConfig) {
        self.sub_table(path, 1);
        self.string(2, "cert", &tls.cert);
        self.string(2, "key", &tls.key);
        self.string(2, "ca", &tls.ca);
        self.string(2, "min-tls-version", &tls.min_tls_version);
        self.string_list(2, "cert-allowed-cn", &tls.cert_allowed_cn);
        self.boolean(2, "auto-certs", tls.auto_certs);
        self.signed(2, "rsa-key-size", tls.rsa_key_size);
        self.string(2, "autocert-expire-duration", &tls.autocert_expire_duration);
        self.boolean(2, "skip-ca", tls.skip_ca);
    }

    fn factor(&mut self, path: &str, factor: FactorConfig) {
        if factor.migrations_per_second == 0.0 {
            return;
        }
        self.sub_table(path, 1);
        self.float(2, "migrations-per-second", factor.migrations_per_second);
    }

    fn cloud_metering(&mut self, path: &str, config: &CloudMeteringConfig) {
        self.sub_table(path, 1);
        self.string(2, "assume-role-arn", &config.assume_role_arn);
        self.string(2, "access-key", &config.access_key);
        self.string(2, "secret-access-key", &config.secret_access_key);
        self.string(2, "session-token", &config.session_token);
    }

    fn key(&mut self, depth: usize, key: &str) {
        self.indent(depth);
        self.0.push_str(&quote_toml_key(key));
    }

    fn indent(&mut self, depth: usize) {
        for _ in 0..depth {
            self.0.push_str("  ");
        }
    }
}

fn quote_toml(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn quote_toml_key(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        value.to_owned()
    } else {
        quote_toml(value)
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct BackendCluster {
    name: String,
    pd_addrs: String,
    ns_servers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct ApiConfig {
    addr: String,
    proxy_protocol: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            addr: DEFAULT_API_ADDR.to_owned(),
            proxy_protocol: String::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct SecurityConfig {
    #[serde(rename = "server-tls")]
    server_tls: TlsConfig,
    #[serde(rename = "server-http-tls")]
    server_http_tls: TlsConfig,
    cluster_tls: TlsConfig,
    sql_tls: TlsConfig,
    encryption_key_path: String,
    require_backend_tls: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            server_tls: TlsConfig::global_default(),
            server_http_tls: TlsConfig::global_default(),
            cluster_tls: TlsConfig::global_default(),
            sql_tls: TlsConfig::global_default(),
            encryption_key_path: String::new(),
            require_backend_tls: false,
        }
    }
}

/// Complete Go-compatible TLS configuration.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct TlsConfig {
    #[serde(skip_serializing_if = "String::is_empty")]
    cert: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    key: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    ca: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    min_tls_version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    cert_allowed_cn: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    auto_certs: bool,
    #[serde(skip_serializing_if = "is_zero_i64")]
    rsa_key_size: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    autocert_expire_duration: String,
    #[serde(skip_serializing_if = "is_false")]
    skip_ca: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

impl TlsConfig {
    fn global_default() -> Self {
        Self {
            min_tls_version: "1.2".to_owned(),
            ..Self::default()
        }
    }

    fn append_material_paths<'a>(&'a self, paths: &mut Vec<&'a Path>) {
        for path in [&self.cert, &self.key, &self.ca] {
            if !path.is_empty() {
                paths.push(Path::new(path));
            }
        }
    }

    /// Returns whether this policy requests Go's server-side certificate generator.
    #[must_use]
    pub const fn auto_certs_enabled(&self) -> bool {
        self.auto_certs
    }

    /// Returns the path-and-policy projection used to validate immutable material.
    #[must_use]
    pub fn material_policy(&self) -> ServingTlsConfig {
        serving_tls(self)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct LogConfig {
    encoder: String,
    simple: bool,
    #[serde(flatten)]
    online: LogOnlineConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            encoder: "tidb".to_owned(),
            simple: false,
            online: LogOnlineConfig::default(),
        }
    }
}

/// Dynamic log subset stored at `/config/log`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct LogOnlineConfig {
    level: String,
    log_file: LogFileConfig,
}

impl Default for LogOnlineConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            log_file: LogFileConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct LogFileConfig {
    filename: String,
    max_size: i64,
    max_days: i64,
    max_backups: i64,
}

impl Default for LogFileConfig {
    fn default() -> Self {
        Self {
            filename: String::new(),
            max_size: 300,
            max_days: 3,
            max_backups: 3,
        }
    }
}

impl LogOnlineConfig {
    /// Configured log level string (`debug`, `info`, `warn`, `error`, `fatal`).
    #[must_use]
    pub fn level(&self) -> &str {
        &self.level
    }

    /// Log file name; empty means the process writes to its standard stream.
    #[must_use]
    pub fn log_file_name(&self) -> &str {
        &self.log_file.filename
    }

    /// `log-file.max-size` in megabytes (non-negative; zero means the default).
    #[must_use]
    pub fn log_file_max_size_mb(&self) -> u64 {
        u64::try_from(self.log_file.max_size).unwrap_or(0)
    }

    /// `log-file.max-days` (non-negative; zero disables age pruning).
    #[must_use]
    pub fn log_file_max_days(&self) -> u64 {
        u64::try_from(self.log_file.max_days).unwrap_or(0)
    }

    /// `log-file.max-backups` (non-negative; zero keeps every backup).
    #[must_use]
    pub fn log_file_max_backups(&self) -> u64 {
        u64::try_from(self.log_file.max_backups).unwrap_or(0)
    }
}

impl LogFileConfig {
    fn is_zero(&self) -> bool {
        self.filename.is_empty()
            && self.max_size == 0
            && self.max_days == 0
            && self.max_backups == 0
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct BalanceConfig {
    label_name: String,
    routing_rule: String,
    policy: String,
    routing_policy: String,
    status: FactorConfig,
    health: FactorConfig,
    memory: FactorConfig,
    cpu: FactorConfig,
    location: FactorConfig,
    conn_count: ConnCountFactorConfig,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            label_name: String::new(),
            routing_rule: String::new(),
            policy: "resource".to_owned(),
            routing_policy: "prefer-idle".to_owned(),
            status: FactorConfig::default(),
            health: FactorConfig::default(),
            memory: FactorConfig::default(),
            cpu: FactorConfig::default(),
            location: FactorConfig::default(),
            conn_count: ConnCountFactorConfig::default(),
        }
    }
}

impl BalanceConfig {
    fn is_zero(&self) -> bool {
        self.label_name.is_empty()
            && self.routing_rule.is_empty()
            && self.policy.is_empty()
            && self.routing_policy.is_empty()
            && self.status.migrations_per_second == 0.0
            && self.health.migrations_per_second == 0.0
            && self.memory.migrations_per_second == 0.0
            && self.cpu.migrations_per_second == 0.0
            && self.location.migrations_per_second == 0.0
            && self.conn_count.is_zero()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct FactorConfig {
    migrations_per_second: f64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct ConnCountFactorConfig {
    #[serde(flatten)]
    factor: FactorConfig,
    count_ratio_threshold: f64,
}

impl ConnCountFactorConfig {
    fn is_zero(self) -> bool {
        self.factor.migrations_per_second == 0.0 && self.count_ratio_threshold == 0.0
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct HaConfig {
    virtual_ip: String,
    interface: String,
    garp_burst_count: i64,
    garp_refresh_count: i64,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            virtual_ip: String::new(),
            interface: String::new(),
            garp_burst_count: 5,
            garp_refresh_count: 30,
        }
    }
}

impl HaConfig {
    fn is_zero(&self) -> bool {
        self.virtual_ip.is_empty()
            && self.interface.is_empty()
            && self.garp_burst_count == 0
            && self.garp_refresh_count == 0
    }
}

/// Restart-pinned metering storage configuration; debug output excludes credentials.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct MeteringConfig {
    #[serde(rename = "type")]
    /// Provider spelling accepted by the Go SDK.
    pub provider_type: String,
    /// Cloud region.
    pub region: String,
    /// Bucket or Azure container name.
    pub bucket: String,
    /// Object key prefix.
    pub prefix: String,
    /// Optional provider service endpoint.
    pub endpoint: String,
    /// AWS settings.
    pub aws: Option<AwsMeteringConfig>,
    /// Alibaba OSS settings.
    pub oss: Option<CloudMeteringConfig>,
    /// Tencent COS settings.
    pub cos: Option<CloudMeteringConfig>,
    /// Azure settings.
    pub azure: Option<AzureMeteringConfig>,
    /// Local filesystem settings.
    pub localfs: Option<LocalFsMeteringConfig>,
    /// Shared metering pool identity.
    pub shared_pool_id: String,
}

impl fmt::Debug for MeteringConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MeteringConfig")
            .field("provider_type", &self.provider_type)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("has_endpoint", &!self.endpoint.is_empty())
            .field("has_aws", &self.aws.is_some())
            .field("has_oss", &self.oss.is_some())
            .field("has_cos", &self.cos.is_some())
            .field("has_azure", &self.azure.is_some())
            .field("has_localfs", &self.localfs.is_some())
            .field("shared_pool_id", &self.shared_pool_id)
            .finish()
    }
}

impl MeteringConfig {
    fn is_zero(&self) -> bool {
        self.provider_type.is_empty()
            && self.region.is_empty()
            && self.bucket.is_empty()
            && self.prefix.is_empty()
            && self.endpoint.is_empty()
            && self.aws.is_none()
            && self.oss.is_none()
            && self.cos.is_none()
            && self.azure.is_none()
            && self.localfs.is_none()
            && self.shared_pool_id.is_empty()
    }
}

/// AWS storage credentials and addressing policy. Never log credential fields.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct AwsMeteringConfig {
    /// Optional role to assume using the base credentials.
    pub assume_role_arn: String,
    /// Address the bucket in the path rather than the hostname.
    pub s3_force_path_style: bool,
    /// Static access key identifier.
    pub access_key: String,
    /// Static secret access key; sensitive.
    pub secret_access_key: String,
    /// Optional temporary credential token; sensitive.
    pub session_token: String,
}

/// OSS/COS credential inputs. Never log credential fields.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct CloudMeteringConfig {
    /// Optional role to assume using the base credentials.
    pub assume_role_arn: String,
    /// Static access key identifier.
    pub access_key: String,
    /// Static secret access key; sensitive.
    pub secret_access_key: String,
    /// Optional temporary credential token; sensitive.
    pub session_token: String,
}

/// Azure storage credential inputs. Never log credential fields.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct AzureMeteringConfig {
    /// Azure storage account name.
    pub account_name: String,
    /// Azure shared account key; sensitive.
    pub account_key: String,
    /// Azure SAS query token; sensitive.
    pub sas_token: String,
}

/// Local filesystem storage options.
#[derive(Clone, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct LocalFsMeteringConfig {
    /// Local storage base directory.
    pub base_path: String,
    /// Create missing directories.
    pub create_dirs: bool,
    /// Octal directory and file permission spelling.
    pub permissions: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct RustDataplaneConfig {
    routing_shadow_socket: String,
    enabled: bool,
    control_socket: String,
    allowed_uid: i64,
    tls_allowed_roots: Vec<String>,
    metrics_owner_port: u16,
}

impl Default for RustDataplaneConfig {
    fn default() -> Self {
        Self {
            routing_shadow_socket: String::new(),
            enabled: false,
            control_socket: String::new(),
            allowed_uid: -1,
            tls_allowed_roots: Vec::new(),
            metrics_owner_port: 0,
        }
    }
}

fn validate_metrics_owner_port(
    proxy: &ProxyConfig,
    api: &ApiConfig,
    owner_port: u16,
) -> Result<(), ConfigError> {
    if owner_port == 0 {
        return Ok(());
    }
    if serving_listeners(proxy)?
        .iter()
        .any(|listener| listener.port == owner_port)
    {
        return invalid(
            "rust-dataplane.metrics-owner-port",
            "conflicts_with_proxy_sql_port",
        );
    }
    let (_, api_port) = split_host_port(&api.addr, "api.addr")?;
    if api_port == owner_port {
        return invalid(
            "rust-dataplane.metrics-owner-port",
            "conflicts_with_api_port",
        );
    }
    Ok(())
}

fn validate_proxy(proxy: &mut ProxyConfig) -> Result<(), ConfigError> {
    if proxy.online.proxy_protocol != "v2" && !proxy.online.proxy_protocol.is_empty() {
        return invalid("proxy.proxy-protocol", "unsupported");
    }
    if proxy.online.conn_buffer_size > 0
        && !(1024..=16 * 1024 * 1024).contains(&proxy.online.conn_buffer_size)
    {
        return invalid("proxy.conn-buffer-size", "out_of_range");
    }
    if !(0.0..=1.0).contains(&proxy.online.high_memory_usage_reject_threshold) {
        return invalid("proxy.high-memory-usage-reject-threshold", "out_of_range");
    }
    if proxy.online.high_memory_usage_reject_threshold > 0.0
        && proxy.online.high_memory_usage_reject_threshold < 0.5
    {
        proxy.online.high_memory_usage_reject_threshold = 0.5;
    }
    validate_sql_addrs(proxy)?;
    let mut names = BTreeSet::new();
    for cluster in &proxy.online.backend_clusters {
        let name = cluster.name.trim();
        if name.is_empty() || !names.insert(name.to_owned()) {
            return invalid("proxy.backend-clusters.name", "empty_or_duplicate");
        }
        for address in split_addr_list(&cluster.pd_addrs) {
            split_host_port(&address, "proxy.backend-clusters.pd-addrs")?;
        }
        if split_addr_list(&cluster.pd_addrs).is_empty() {
            return invalid("proxy.backend-clusters.pd-addrs", "empty");
        }
        for server in &cluster.ns_servers {
            normalize_ns_server(server)?;
        }
    }
    if proxy.online.failover_timeout < 0 {
        return invalid("proxy.failover-timeout", "negative");
    }
    let mut failed = BTreeSet::new();
    let mut normalized = Vec::with_capacity(proxy.online.fail_backend_list.len());
    for backend in &proxy.online.fail_backend_list {
        let backend = backend.trim();
        if backend.is_empty() {
            return invalid("proxy.fail-backend-list", "empty_entry");
        }
        if failed.insert(backend.to_owned()) {
            normalized.push(backend.to_owned());
        }
    }
    proxy.online.fail_backend_list = normalized;
    Ok(())
}

fn validate_sql_addrs(proxy: &ProxyConfig) -> Result<(), ConfigError> {
    let addrs = split_addr_list(&proxy.addr);
    if addrs.is_empty() {
        if proxy.port_range.is_empty() {
            return Ok(());
        }
        return invalid("proxy.addr", "empty_with_port_range");
    }
    if proxy.port_range.is_empty() {
        for address in addrs {
            split_host_port(&address, "proxy.addr")?;
        }
        return Ok(());
    }
    if proxy.port_range.len() != 2 || addrs.len() != 1 {
        return invalid("proxy.port-range", "invalid_shape");
    }
    let start = proxy.port_range[0];
    let end = proxy.port_range[1];
    if start < 1 || end > i32::from(u16::MAX) || start > end {
        return invalid("proxy.port-range", "out_of_range");
    }
    split_host_port(&addrs[0], "proxy.addr")?;
    Ok(())
}

fn validate_balance(balance: &mut BalanceConfig) -> Result<(), ConfigError> {
    if balance.policy.is_empty() {
        "resource".clone_into(&mut balance.policy);
    }
    if !matches!(
        balance.policy.as_str(),
        "resource" | "location" | "connection"
    ) {
        return invalid("balance.policy", "unsupported");
    }
    if !matches!(
        balance.routing_rule.as_str(),
        "" | "client_cidr" | "proxy_cidr" | "port"
    ) {
        return invalid("balance.routing-rule", "unsupported");
    }
    if balance.routing_policy.is_empty() {
        "prefer-idle".clone_into(&mut balance.routing_policy);
    }
    if !matches!(balance.routing_policy.as_str(), "prefer-idle" | "random") {
        return invalid("balance.routing-policy", "unsupported");
    }
    let migration_rates = [
        balance.status.migrations_per_second,
        balance.health.migrations_per_second,
        balance.memory.migrations_per_second,
        balance.cpu.migrations_per_second,
        balance.location.migrations_per_second,
        balance.conn_count.factor.migrations_per_second,
    ];
    if migration_rates.into_iter().any(|rate| rate < 0.0) {
        return invalid("balance.*.migrations-per-second", "negative");
    }
    if balance.conn_count.count_ratio_threshold != 0.0
        && balance.conn_count.count_ratio_threshold <= 1.0
    {
        return invalid("balance.conn-count.count-ratio-threshold", "out_of_range");
    }
    Ok(())
}

fn validate_tls(field: &'static str, tls: &TlsConfig) -> Result<(), ConfigError> {
    if tls.cert.is_empty() != tls.key.is_empty() {
        return invalid(field, "partial_cert_key");
    }
    if !tls.min_tls_version.is_empty() && !matches!(tls.min_tls_version.as_str(), "1.2" | "1.3") {
        return invalid(field, "unsupported_minimum_version");
    }
    for path in [&tls.cert, &tls.key, &tls.ca] {
        if !path.is_empty() && !Path::new(path).is_absolute() {
            return invalid(field, "relative_path");
        }
    }
    if tls
        .cert_allowed_cn
        .iter()
        .any(|name| name.trim().is_empty())
    {
        return invalid(field, "empty_common_name");
    }
    Ok(())
}

fn client_tls(tls: &TlsConfig) -> ClientTlsConfig {
    ClientTlsConfig {
        ca_path: path_arc(&tls.ca),
        certificate_path: path_arc(&tls.cert),
        private_key_path: path_arc(&tls.key),
        minimum_version: Arc::from(tls.min_tls_version.as_str()),
        skip_ca_verification: tls.skip_ca,
        allowed_common_names: normalized_common_names(&tls.cert_allowed_cn),
    }
}

fn serving_tls(tls: &TlsConfig) -> ServingTlsConfig {
    ServingTlsConfig {
        certificate_path: path_arc(&tls.cert),
        private_key_path: path_arc(&tls.key),
        ca_path: path_arc(&tls.ca),
        minimum_version: Arc::from(tls.min_tls_version.as_str()),
        allowed_common_names: normalized_common_names(&tls.cert_allowed_cn),
        skip_ca_verification: tls.skip_ca,
    }
}

fn normalized_common_names(names: &[String]) -> Arc<[Arc<str>]> {
    let mut normalized = names
        .iter()
        .map(|name| Arc::<str>::from(name.trim()))
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    Arc::from(normalized)
}

fn serving_keepalive(
    field: &'static str,
    keepalive: &KeepAliveConfig,
) -> Result<ServingKeepalive, ConfigError> {
    if keepalive.idle < 0 || keepalive.count < 0 || keepalive.interval < 0 || keepalive.timeout < 0
    {
        return invalid(field, "negative");
    }
    for value in [keepalive.idle, keepalive.interval, keepalive.timeout] {
        if value > 0 && value < NANOS_PER_MILLISECOND {
            return invalid(field, "sub_millisecond");
        }
    }
    Ok(ServingKeepalive {
        enabled: keepalive.enabled,
        idle_millis: u64::try_from(keepalive.idle / NANOS_PER_MILLISECOND).map_err(|_| {
            ConfigError::InvalidField {
                field,
                class: "out_of_range",
            }
        })?,
        probe_count: u32::try_from(keepalive.count).map_err(|_| ConfigError::InvalidField {
            field,
            class: "out_of_range",
        })?,
        interval_millis: u64::try_from(keepalive.interval / NANOS_PER_MILLISECOND).map_err(
            |_| ConfigError::InvalidField {
                field,
                class: "out_of_range",
            },
        )?,
        user_timeout_millis: u64::try_from(keepalive.timeout / NANOS_PER_MILLISECOND).map_err(
            |_| ConfigError::InvalidField {
                field,
                class: "out_of_range",
            },
        )?,
    })
}

fn serving_listeners(proxy: &ProxyConfig) -> Result<Vec<ServingListener>, ConfigError> {
    let addrs = split_addr_list(&proxy.addr);
    if addrs.is_empty() {
        return invalid("proxy.addr", "empty_listener");
    }
    let addresses = if proxy.port_range.is_empty() {
        addrs
            .into_iter()
            .map(|value| split_host_port(&value, "proxy.addr"))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let (host, _) = split_host_port(&addrs[0], "proxy.addr")?;
        (proxy.port_range[0]..=proxy.port_range[1])
            .map(|port| {
                u16::try_from(port)
                    .map(|port| (host.clone(), port))
                    .map_err(|_| ConfigError::InvalidField {
                        field: "proxy.port-range",
                        class: "out_of_range",
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut seen = BTreeSet::new();
    let mut listeners = Vec::with_capacity(addresses.len());
    for (index, (host, port)) in addresses.into_iter().enumerate() {
        if !seen.insert((host.clone(), port)) {
            return invalid("proxy.addr", "duplicate_listener");
        }
        listeners.push(ServingListener {
            name: Arc::from(format!("sql-{index}")),
            address: Arc::from(host),
            port,
        });
    }
    Ok(listeners)
}

fn seconds_to_millis(field: &'static str, seconds: i64) -> Result<u64, ConfigError> {
    u64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or(ConfigError::InvalidField {
            field,
            class: "out_of_range",
        })
}

fn normalize_public_endpoints(values: &[String]) -> Result<Vec<Arc<str>>, ConfigError> {
    let mut result = values
        .iter()
        .map(|value| normalize_ip_prefix(value))
        .collect::<Result<Vec<_>, _>>()?;
    result.sort();
    result.dedup();
    Ok(result.into_iter().map(Arc::<str>::from).collect())
}

fn normalize_ip_prefix(value: &str) -> Result<String, ConfigError> {
    let value = value.trim();
    let (address, prefix) = if let Some((address, prefix)) = value.split_once('/') {
        let address = address
            .parse::<IpAddr>()
            .map_err(|_| ConfigError::InvalidField {
                field: "proxy.public-endpoints",
                class: "invalid_ip_or_cidr",
            })?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| ConfigError::InvalidField {
                field: "proxy.public-endpoints",
                class: "invalid_ip_or_cidr",
            })?;
        (address, prefix)
    } else {
        let address = value
            .parse::<IpAddr>()
            .map_err(|_| ConfigError::InvalidField {
                field: "proxy.public-endpoints",
                class: "invalid_ip_or_cidr",
            })?;
        let prefix = if address.is_ipv4() { 32 } else { 128 };
        (address, prefix)
    };
    match address {
        IpAddr::V4(address) if prefix <= 32 => {
            let bits = u32::from(address);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            Ok(format!(
                "{}/{}",
                std::net::Ipv4Addr::from(bits & mask),
                prefix
            ))
        }
        IpAddr::V6(address) if prefix <= 128 => {
            let bits = u128::from(address);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            Ok(format!(
                "{}/{}",
                std::net::Ipv6Addr::from(bits & mask),
                prefix
            ))
        }
        IpAddr::V4(_) | IpAddr::V6(_) => invalid("proxy.public-endpoints", "invalid_ip_or_cidr"),
    }
}

fn parse_vip_address(value: &str) -> Result<(IpAddr, u8), ConfigError> {
    let failure = || ConfigError::InvalidField {
        field: "ha.virtual-ip",
        class: "invalid_ip_or_cidr",
    };
    let (ip, prefix_len) = if let Some((ip, prefix)) = value.split_once('/') {
        (
            ip.parse::<IpAddr>().map_err(|_| failure())?,
            prefix.parse::<u8>().map_err(|_| failure())?,
        )
    } else {
        let ip = value.parse::<IpAddr>().map_err(|_| failure())?;
        let prefix_len = if ip.is_ipv4() { 32 } else { 128 };
        (ip, prefix_len)
    };
    if prefix_len > if ip.is_ipv4() { 32 } else { 128 } {
        return Err(failure());
    }
    Ok((ip, prefix_len))
}

fn path_arc(value: &str) -> Option<Arc<Path>> {
    (!value.is_empty()).then(|| Arc::from(Path::new(value)))
}

fn split_addr_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

fn split_host_port(value: &str, field: &'static str) -> Result<(String, u16), ConfigError> {
    let value = value.trim();
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return invalid(field, "invalid_address");
        };
        if host.parse::<IpAddr>().is_err() {
            return invalid(field, "invalid_address");
        }
        (host, port)
    } else {
        let Some((host, port)) = value.rsplit_once(':') else {
            return invalid(field, "invalid_address");
        };
        if host.contains(':') {
            return invalid(field, "invalid_address");
        }
        (host, port)
    };
    let port =
        port.parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or(ConfigError::InvalidField {
                field,
                class: "invalid_port",
            })?;
    Ok((host.to_owned(), port))
}

fn normalize_ns_server(value: &str) -> Result<String, ConfigError> {
    const FIELD: &str = "proxy.backend-clusters.ns-servers";
    let value = value.trim();
    if value.is_empty() {
        return invalid(FIELD, "empty_host");
    }
    // Bracketed `[host]:port`. Go `net.SplitHostPort` accepts a bracketed
    // hostname, and `net.JoinHostPort` re-brackets ONLY when the host contains a
    // colon (an IPv6 literal); a bracketed hostname therefore normalizes to the
    // UNBRACKETED `host:port`. This widens only this raw-config face — the strict
    // downstream validators still reject `[hostname]:port`.
    if let Some(rest) = value.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return invalid(FIELD, "invalid_address");
        };
        if host.is_empty() {
            return invalid(FIELD, "empty_host");
        }
        let port = normalize_port(port, FIELD)?;
        if host.contains(':') {
            return Ok(format!("[{host}]:{port}"));
        }
        return Ok(format!("{host}:{port}"));
    }
    // A bare IPv6 literal has no port (Go `SplitHostPort` fails), so Go falls back
    // to `net.JoinHostPort(server, "53")` → `[<ip>]:53`.
    if value.parse::<std::net::Ipv6Addr>().is_ok() {
        return Ok(format!("[{value}]:53"));
    }
    // Plain `host:port` (hostname or IPv4).
    if value.contains(':') {
        let (host, port) = split_host_port(value, FIELD)?;
        if host.is_empty() {
            return invalid(FIELD, "empty_host");
        }
        return Ok(format!("{host}:{port}"));
    }
    // Bare hostname / IPv4 literal → default port 53.
    if value.contains(['[', ']']) {
        return invalid(FIELD, "invalid_host");
    }
    Ok(format!("{value}:53"))
}

/// Parses an explicit numeric port that is not zero, matching `split_host_port`.
fn normalize_port(port: &str, field: &'static str) -> Result<u16, ConfigError> {
    port.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or(ConfigError::InvalidField {
            field,
            class: "invalid_port",
        })
}

fn invalid<T>(field: &'static str, class: &'static str) -> Result<T, ConfigError> {
    Err(ConfigError::InvalidField { field, class })
}
