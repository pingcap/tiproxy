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

//! CP-CFG/NS process-local composition over the shrinking Go bridge.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use control_config::{
    CandidateValidator, ConfigNamespaceSnapshot, ConfigNamespaceSource, ConfigNamespaceStore,
    EffectiveConfig, NamespaceConfig, PreparedArtifact, ServingConfig, ServingKeepalive,
    ServingTlsConfig,
};
use control_plane::{
    ConfigSource, ControlConfig, ControlModule, ControlRuntime as InProcessControlRuntime,
    LifecyclePhase, LogLevel, MetricsPolicy, ModuleContext, ModuleError, ModuleFuture, TlsPolicy,
};
use control_proto::snapshot::{CompositionGenerations, SnapshotError, SnapshotStore, UnixTime};
use control_proto::v1::{
    BackendSnapshot, ConfigSnapshot, KeepalivePolicy, Listener, NamespaceSnapshot,
    ProxyProtocolMode, StateSnapshot, TlsPolicy as WireTlsPolicy,
};
use control_topology::{TopologyModuleHandle, TopologyUpdateObserver};
use dataplane::ServingSnapshotComposer;
use dataplane::control_runtime::SnapshotComposition;
use session_core::handshake::SUPPORTED_SERVER_CAPABILITIES;

/// Rust-owned config/namespace projection retaining only Go-owned topology and
/// protocol/static handshake facts from the legacy bridge.
/// Fallback server version advertised before any backend reports its own,
/// mirroring Go `parser/mysql.ServerVersion` (`mysqlCompatibilityVersion` +
/// `VersionSeparator` + the default `TiDBReleaseVersion` placeholder), which Go
/// serves through `pnet.ServerVersion`. A live backend's observed version
/// overrides it, exactly as in Go.
const DEFAULT_SERVER_VERSION: &str = "8.0.11-TiDB-v8.4.0-this-is-a-placeholder";

pub struct RustConfigComposer {
    source: ConfigNamespaceStore,
    topology: Option<TopologyModuleHandle>,
    generation: AtomicU64,
    drain_grace_override: Option<Duration>,
}

impl RustConfigComposer {
    #[must_use]
    pub fn new(source: ConfigNamespaceStore, drain_grace_override: Option<Duration>) -> Self {
        Self {
            source,
            topology: None,
            generation: AtomicU64::new(1),
            drain_grace_override,
        }
    }

    /// Installs the process-local CP-TOPO source used after
    /// `RUST_ROUTE_OWNER` empties the bridge backend list.
    #[must_use]
    pub fn with_topology(mut self, topology: TopologyModuleHandle) -> Self {
        self.topology = Some(topology);
        self
    }

    fn updates(&self) -> Option<TopologyUpdateObserver> {
        self.topology.as_ref().map(TopologyModuleHandle::updates)
    }

    fn advance_generation(&self) -> Result<(), ModuleError> {
        self.generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .map(|_| ())
            .map_err(|_| module_error("serving_generation_exhausted"))
    }

    fn compose_current(
        &self,
        bridge: &StateSnapshot,
    ) -> Result<SnapshotComposition, SnapshotError> {
        let owned = self.source.current();
        let composition_generation = self.generation.load(Ordering::Acquire);
        // The CP-CFG generation travels inside the composition: the
        // namespace-commit barrier only learns it from the serving apply
        // that installed this exact view, never from the act of composing.
        compose_snapshot(
            &owned,
            bridge,
            self.topology.as_ref(),
            composition_generation,
            self.drain_grace_override,
        )
    }

    /// Bridge-free static source snapshot for the first serving bind and
    /// later reloads. It carries only the two values the Go bridge used to
    /// supply -- the advertised capability mask and the server version --
    /// with empty backend/namespace state, so `compose` treats it as the
    /// `RUST_ROUTE_OWNER` static config and takes every other serving field
    /// from the local owned config and CP-TOPO source. This lets a standalone
    /// process bind SQL without a Go control peer.
    #[must_use]
    pub fn local_source_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            config: Some(ConfigSnapshot {
                advertised_capability: SUPPORTED_SERVER_CAPABILITIES.bits(),
                server_version: self.local_server_version(),
                ..ConfigSnapshot::default()
            }),
            backends: Vec::new(),
            namespaces: Vec::new(),
        }
    }

    /// The greeting server version. Mirrors Go
    /// `DefaultHandshakeHandler.GetServerVersion`: prefer a backend's observed
    /// `/status` version (the CP-TOPO health overlay carries it, as Go reads it
    /// from the default namespace's router), falling back to
    /// [`DEFAULT_SERVER_VERSION`] when no backend has reported one yet.
    fn local_server_version(&self) -> String {
        self.topology
            .as_ref()
            .and_then(|topology| {
                let routing = topology.routing_handle().current()?;
                let health = topology.health_overlay_handle().current_for(&routing)?;
                routing.backends.backends.iter().find_map(|merged| {
                    let version = health.get(merged.backend_id.as_ref()).server_version?;
                    let trimmed = version.trim();
                    (!trimmed.is_empty()).then(|| trimmed.to_owned())
                })
            })
            .unwrap_or_else(|| DEFAULT_SERVER_VERSION.to_owned())
    }
}

impl ServingSnapshotComposer for RustConfigComposer {
    fn compose(&self, source: &StateSnapshot) -> Result<SnapshotComposition, SnapshotError> {
        self.compose_current(source)
    }
}

/// Complete serving validator installed on the source before generation one
/// is published. TLS files are loaded and parsed here, so a bad candidate
/// cannot advance CP-CFG ahead of the serving view.
pub struct ServingCandidateValidator {
    snapshots: SnapshotStore,
    drain_grace_override: Option<Duration>,
}

/// Server-HTTP TLS identity prepared with the accepted config generation.
///
/// The opaque config artifact retains this value together with topology
/// clients, so the metric owner listener can select plaintext or the exact
/// accepted TLS identity without re-reading a path after publication.
#[derive(Clone)]
pub(crate) struct PreparedServingSet {
    server_http_tls: Option<Arc<rustls::ServerConfig>>,
}

impl PreparedServingSet {
    #[must_use]
    pub(crate) fn server_http_tls(&self) -> Option<Arc<rustls::ServerConfig>> {
        self.server_http_tls.clone()
    }
}

/// Supervised adapter applying every accepted CP-CFG generation to both the
/// CP-001 dynamic subset and the SQL serving snapshot. The source validator
/// makes these applications deterministic; any unexpected failure terminates
/// this module and therefore the whole single-owner process.
pub struct ConfigServingAdapter {
    source: ConfigNamespaceStore,
    serving: dataplane::DataplaneServingHandle,
    snapshots: SnapshotStore,
    runtime: Arc<InProcessControlRuntime>,
    health_port: u16,
    tls_roots: Arc<[std::path::PathBuf]>,
    drain_grace_override: Option<Duration>,
    composer: Arc<RustConfigComposer>,
    topology_updates: Option<TopologyUpdateObserver>,
}

impl ConfigServingAdapter {
    #[must_use]
    pub fn new(
        serving: dataplane::DataplaneServingHandle,
        snapshots: SnapshotStore,
        runtime: Arc<InProcessControlRuntime>,
        health_port: u16,
        tls_roots: Vec<std::path::PathBuf>,
        drain_grace_override: Option<Duration>,
        composer: Arc<RustConfigComposer>,
    ) -> Self {
        let topology_updates = composer.updates();
        let source = composer.source.clone();
        Self {
            source,
            serving,
            snapshots,
            runtime,
            health_port,
            tls_roots: Arc::from(tls_roots),
            drain_grace_override,
            composer,
            topology_updates,
        }
    }

    async fn run_inner(mut self, context: ModuleContext) -> Result<(), ModuleError> {
        let mut updates = self.source.subscribe();
        let mut lifecycle = context.lifecycle();
        // The process-local runtime is constructed from the file-only
        // generation one so it can supervise this module. Persistence is
        // incorporated before the adapter is spawned; if that bootstrap
        // published a newer generation, apply the receiver's current value
        // once before waiting for another watch edge. Otherwise the first
        // persistent /config view would not reach CP-001 until an unrelated
        // later edit. `reload_composed` is deliberately a no-op before the
        // first bridge topology and that first topology reads the latest
        // composer state, so startup ordering remains race-free.
        let initial = updates.borrow_and_update().clone();
        if initial.generation() > 1 {
            self.apply_snapshot(&initial).await?;
        }
        loop {
            tokio::select! {
                changed = lifecycle.changed() => {
                    if changed.is_err() || shutdown_started(lifecycle.borrow().phase) {
                        return Ok(());
                    }
                }
                changed = updates.changed() => {
                    if changed.is_err() {
                        return Err(module_error("config_source_stopped"));
                    }
                    let snapshot = updates.borrow_and_update().clone();
                    self.apply_snapshot(&snapshot).await?;
                }
                changed = async {
                    match self.topology_updates.as_mut() {
                        Some(updates) => updates.changed().await,
                        None => std::future::pending().await,
                    }
                }, if self.topology_updates.is_some() => {
                    if changed.is_err() {
                        return Err(module_error("topology_source_stopped"));
                    }
                    self.composer.advance_generation()?;
                    self.reload_serving().await?;
                }
            }
        }
    }

    async fn apply_snapshot(&self, snapshot: &ConfigNamespaceSnapshot) -> Result<(), ModuleError> {
        self.composer.advance_generation()?;
        // `watch` deliberately coalesces bursts. CP-001's local config
        // lineage is immediate-successor-only, so advance that consumer
        // lineage once for the latest accepted CP-CFG view instead of
        // feeding it a possibly skipped source generation.
        let generation =
            next_consumer_generation(self.runtime.handle().config().current().generation())?;
        let config = control_config_at_generation(
            snapshot,
            generation,
            self.health_port,
            &self.tls_roots,
            self.drain_grace_override,
        )
        .map_err(|_| module_error("control_projection_rejected"))?;
        self.runtime
            .apply_config(config)
            .map_err(|_| module_error("control_apply_rejected"))?;
        self.reload_serving().await
    }

    async fn reload_serving(&self) -> Result<(), ModuleError> {
        self.serving
            .reload_composed(&self.snapshots, unix_time_now())
            .await
            .map_err(|_| module_error("serving_apply_rejected"))?;
        Ok(())
    }
}

fn next_consumer_generation(current: u64) -> Result<u64, ModuleError> {
    current
        .checked_add(1)
        .ok_or_else(|| module_error("control_generation_exhausted"))
}

impl ControlModule for ConfigServingAdapter {
    fn name(&self) -> &'static str {
        "config_serving_adapter"
    }

    fn run(self: Box<Self>, context: ModuleContext) -> ModuleFuture {
        Box::pin(self.run_inner(context))
    }
}

impl ServingCandidateValidator {
    #[must_use]
    pub const fn new(snapshots: SnapshotStore, drain_grace_override: Option<Duration>) -> Self {
        Self {
            snapshots,
            drain_grace_override,
        }
    }
}

impl CandidateValidator for ServingCandidateValidator {
    fn validate(
        &self,
        effective: &EffectiveConfig,
        namespaces: &[NamespaceConfig],
    ) -> Result<PreparedArtifact, &'static str> {
        if effective.serving_auto_certs_enabled() {
            return Err("serving_auto_certs_unsupported");
        }
        if effective.server_http_tls().auto_certs_enabled()
            || namespaces
                .iter()
                .any(|namespace| namespace.frontend_tls().auto_certs_enabled())
        {
            return Err("server_auto_certs_unsupported");
        }
        if effective.api_proxy_protocol() == "v2" {
            return Err("api_proxy_protocol_unsupported");
        }
        if !effective.encryption_key_path().is_empty() {
            return Err("encryption_key_path_unsupported");
        }
        if !effective.ha_virtual_ip().is_empty() && !effective.ha_interface().is_empty() {
            return Err("ha_vip_unsupported");
        }
        let mut serving = effective.serving().map_err(|_| "serving_projection")?;
        apply_drain_override(&mut serving, self.drain_grace_override);
        let candidate = StateSnapshot {
            config: Some(wire_config(&serving, 0, "candidate-validation")),
            namespaces: wire_namespaces(namespaces),
            ..StateSnapshot::default()
        };
        let now = unix_time_now();
        self.snapshots
            .validate_composed(
                1,
                CompositionGenerations {
                    composition: 1,
                    config: 1,
                },
                candidate,
                unix_time_now(),
            )
            .map_err(|_| "serving_validation")?;
        let server_http_policy = wire_tls(&effective.server_http_tls().material_policy());
        let server_http_tls = self
            .snapshots
            .prepare_server_tls("server_http_tls", &server_http_policy, now)
            .map_err(|_| "tls_material_validation")?;
        self.validate_material("cluster_tls", effective.cluster_tls(), now, false)?;
        for namespace in namespaces {
            self.validate_material(
                "namespace_frontend_tls",
                namespace.frontend_tls(),
                now,
                true,
            )?;
            self.validate_material("namespace_backend_tls", namespace.backend_tls(), now, false)?;
        }
        Ok(PreparedArtifact::new(Arc::new(PreparedServingSet {
            server_http_tls,
        })))
    }
}

impl ServingCandidateValidator {
    fn validate_material(
        &self,
        field: &str,
        policy: &control_config::model::TlsConfig,
        now: UnixTime,
        server_side: bool,
    ) -> Result<(), &'static str> {
        let policy = wire_tls(&policy.material_policy());
        self.snapshots
            .validate_tls_material(field, &policy, now, server_side)
            .map_err(|_| "tls_material_validation")
    }
}

/// Projects the CP-CFG generation into CP-001's shared dynamic subset.
pub fn control_config(
    snapshot: &ConfigNamespaceSnapshot,
    health_port: u16,
    tls_roots: &[std::path::PathBuf],
    drain_grace_override: Option<Duration>,
) -> Result<ControlConfig, String> {
    control_config_at_generation(
        snapshot,
        snapshot.generation(),
        health_port,
        tls_roots,
        drain_grace_override,
    )
}

fn control_config_at_generation(
    snapshot: &ConfigNamespaceSnapshot,
    generation: u64,
    health_port: u16,
    tls_roots: &[std::path::PathBuf],
    drain_grace_override: Option<Duration>,
) -> Result<ControlConfig, String> {
    let mut serving = snapshot
        .effective()
        .serving()
        .map_err(|error| format!("project serving config: {error}"))?;
    apply_drain_override(&mut serving, drain_grace_override);
    let log_level = match snapshot.effective().log_level() {
        "debug" => LogLevel::Debug,
        "info" => LogLevel::Info,
        "warn" => LogLevel::Warn,
        "error" | "fatal" => LogLevel::Error,
        _ => return Err("project log level: unsupported value".to_owned()),
    };
    let tls = TlsPolicy::new(tls_roots.to_vec())
        .map_err(|error| format!("validate in-process TLS policy: {error}"))?;
    ControlConfig::new(
        generation,
        Duration::from_millis(serving.graceful_close_millis),
        health_port,
        tls,
        log_level,
        MetricsPolicy::default(),
    )
    .map_err(|error| format!("validate in-process control config: {error}"))
}

fn compose_snapshot(
    owned: &ConfigNamespaceSnapshot,
    bridge: &StateSnapshot,
    topology: Option<&TopologyModuleHandle>,
    composition_generation: u64,
    drain_grace_override: Option<Duration>,
) -> Result<SnapshotComposition, SnapshotError> {
    if !bridge.backends.is_empty() || !bridge.namespaces.is_empty() {
        return Err(SnapshotError::invalid(
            "RUST_ROUTE_OWNER bridge snapshot contains backend or namespace state",
        ));
    }
    let mut serving = owned
        .effective()
        .serving()
        .map_err(|_| SnapshotError::invalid("Rust config serving projection is invalid"))?;
    apply_drain_override(&mut serving, drain_grace_override);
    let bridge_config = bridge.config.as_ref().ok_or_else(|| {
        SnapshotError::invalid("bridge protocol/static config snapshot is required")
    })?;
    Ok(SnapshotComposition {
        snapshot: StateSnapshot {
            config: Some(wire_config(
                &serving,
                bridge_config.advertised_capability,
                &bridge_config.server_version,
            )),
            backends: topology.map_or_else(Vec::new, wire_backends),
            namespaces: wire_namespaces(owned.namespaces()),
        },
        generation: composition_generation,
        config_generation: owned.generation(),
    })
}

fn wire_backends(topology: &TopologyModuleHandle) -> Vec<BackendSnapshot> {
    let routing_handle = topology.routing_handle();
    let Some(routing) = routing_handle.current() else {
        return Vec::new();
    };
    let health = topology.health_overlay_handle().current_for(&routing);
    routing
        .backends
        .backends
        .iter()
        .map(|merged| {
            let verdict = health
                .as_ref()
                .map(|snapshot| snapshot.get(merged.backend_id.as_ref()));
            BackendSnapshot {
                backend_id: merged.backend_id.to_string(),
                address: merged.backend.addr.clone(),
                cluster_name: merged.cluster_name.to_string(),
                keyspace: merged.backend.keyspace.clone(),
                healthy: verdict.as_ref().is_some_and(|value| value.healthy),
                local: verdict.as_ref().is_some_and(|value| value.local),
                draining: false,
                cidrs: Vec::new(),
                labels: merged.backend.labels.clone(),
            }
        })
        .collect()
}

fn apply_drain_override(serving: &mut ServingConfig, override_value: Option<Duration>) {
    if let Some(value) = override_value {
        serving.graceful_close_millis = value.as_millis().try_into().unwrap_or(u64::MAX);
    }
}

fn wire_config(serving: &ServingConfig, advertised: u32, server_version: &str) -> ConfigSnapshot {
    ConfigSnapshot {
        max_connections: serving.max_connections,
        high_memory_reject_threshold: serving.high_memory_reject_threshold,
        connection_buffer_bytes: serving.connection_buffer_bytes,
        frontend_keepalive: Some(wire_keepalive(serving.frontend_keepalive)),
        healthy_backend_keepalive: Some(wire_keepalive(serving.healthy_backend_keepalive)),
        unhealthy_backend_keepalive: Some(wire_keepalive(serving.unhealthy_backend_keepalive)),
        proxy_protocol: if serving.proxy_protocol_v2 {
            ProxyProtocolMode::V2 as i32
        } else {
            ProxyProtocolMode::Disabled as i32
        },
        require_backend_tls: serving.require_backend_tls,
        graceful_wait_millis: serving.graceful_wait_millis,
        graceful_close_millis: serving.graceful_close_millis,
        listeners: serving
            .listeners
            .iter()
            .map(|listener| Listener {
                address: listener.address.to_string(),
                port: u32::from(listener.port),
                name: listener.name.to_string(),
            })
            .collect(),
        public_cidrs: serving
            .public_cidrs
            .iter()
            .map(ToString::to_string)
            .collect(),
        advertised_capability: advertised,
        server_version: server_version.trim().to_owned(),
        frontend_tls: Some(wire_tls(&serving.frontend_tls)),
        backend_tls: Some(wire_tls(&serving.backend_tls)),
        traffic_replay_enabled: serving.traffic_replay_enabled,
    }
}

const fn wire_keepalive(value: ServingKeepalive) -> KeepalivePolicy {
    KeepalivePolicy {
        enabled: value.enabled,
        idle_millis: value.idle_millis,
        probe_count: value.probe_count,
        interval_millis: value.interval_millis,
        user_timeout_millis: value.user_timeout_millis,
    }
}

fn wire_tls(value: &ServingTlsConfig) -> WireTlsPolicy {
    WireTlsPolicy {
        certificate_path: path_text(value.certificate_path.as_deref()),
        private_key_path: path_text(value.private_key_path.as_deref()),
        ca_path: path_text(value.ca_path.as_deref()),
        minimum_version: value.minimum_version.to_string(),
        allowed_common_names: value
            .allowed_common_names
            .iter()
            .map(ToString::to_string)
            .collect(),
        skip_ca_verification: value.skip_ca_verification,
    }
}

fn wire_namespaces(values: &[NamespaceConfig]) -> Vec<NamespaceSnapshot> {
    values
        .iter()
        .map(NamespaceConfig::serving)
        .map(|namespace| NamespaceSnapshot {
            name: namespace.name.to_string(),
            users: namespace.users.iter().map(ToString::to_string).collect(),
            // Backend selection/keyspace moves with CP-TOPO/CP-ROUTE. CP-CFG
            // owns identity and persistence, so it must not invent a binding.
            backend_cluster: String::new(),
        })
        .collect()
}

fn path_text(value: Option<&Path>) -> String {
    value.map_or_else(String::new, |path| path.to_string_lossy().into_owned())
}

fn unix_time_now() -> UnixTime {
    UnixTime::since_unix_epoch(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

const fn shutdown_started(phase: LifecyclePhase) -> bool {
    matches!(
        phase,
        LifecyclePhase::Quiescing
            | LifecyclePhase::Draining
            | LifecyclePhase::Stopping
            | LifecyclePhase::Stopped
            | LifecyclePhase::Failed
    )
}

const fn module_error(error_class: &'static str) -> ModuleError {
    ModuleError {
        module: "config_serving_adapter",
        error_class,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use control_config::{
        ConfigNamespaceStore, PersistentConfigSnapshot, SourceRevision, StoreError,
    };
    use control_plane::{OwnershipRegistry, ShutdownReason};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn create() -> Result<Self, std::io::Error> {
            let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "tiproxy-config-composition-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn local_source_snapshot_feeds_compose_without_a_bridge()
    -> Result<(), Box<dyn std::error::Error>> {
        let owned = ConfigNamespaceStore::from_toml(
            b"enable-traffic-replay = false\n[proxy]\naddr = '127.0.0.1:7001'\nmax-connections = 41\n",
            None,
            Path::new("/tmp"),
        )?;
        let composer = RustConfigComposer::new(owned, None);
        let source = composer.local_source_snapshot();
        // The two former-bridge fields come from local sources, and no
        // backend/namespace state is invented; with no CP-TOPO backend yet the
        // server version is the Go-parity fallback.
        let source_config = source.config.clone().unwrap_or_default();
        assert_eq!(
            source_config.advertised_capability,
            SUPPORTED_SERVER_CAPABILITIES.bits()
        );
        assert_eq!(source_config.server_version, DEFAULT_SERVER_VERSION);
        assert!(source.backends.is_empty() && source.namespaces.is_empty());
        // compose accepts the local source with no bridge peer and keeps every
        // other serving field from the local owned config.
        let composition = composer.compose_current(&source)?;
        let config = composition.snapshot.config.unwrap_or_default();
        assert_eq!(config.max_connections, 41);
        assert_eq!(config.listeners[0].port, 7001);
        assert_eq!(
            config.advertised_capability,
            SUPPORTED_SERVER_CAPABILITIES.bits()
        );
        assert_eq!(config.server_version, DEFAULT_SERVER_VERSION);
        Ok(())
    }

    #[test]
    fn composer_rejects_bridge_route_state_and_keeps_only_static_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let owned = ConfigNamespaceStore::from_toml(
            b"enable-traffic-replay = false\n[proxy]\naddr = '127.0.0.1:7001'\nmax-connections = 41\n",
            None,
            Path::new("/tmp"),
        )?;
        let composer = RustConfigComposer::new(owned, None);
        let nonempty = StateSnapshot {
            config: Some(ConfigSnapshot {
                max_connections: 999,
                advertised_capability: 123,
                server_version: " TiProxy-test ".to_owned(),
                ..ConfigSnapshot::default()
            }),
            namespaces: vec![NamespaceSnapshot {
                name: "bridge-owned".to_owned(),
                ..NamespaceSnapshot::default()
            }],
            backends: vec![BackendSnapshot {
                backend_id: "bridge-owned".to_owned(),
                address: "127.0.0.1:4000".to_owned(),
                ..BackendSnapshot::default()
            }],
        };
        assert!(composer.compose_current(&nonempty).is_err());
        let bridge = StateSnapshot {
            config: nonempty.config,
            ..StateSnapshot::default()
        };
        let composition = composer.compose_current(&bridge)?;
        let config = composition.snapshot.config.unwrap_or_default();
        assert_eq!(composition.generation, 1);
        assert_eq!(config.max_connections, 41);
        assert_eq!(config.listeners[0].port, 7001);
        assert_eq!(config.advertised_capability, 123);
        assert_eq!(config.server_version, "TiProxy-test");
        assert!(composition.snapshot.namespaces.is_empty());
        composer.advance_generation()?;
        assert_eq!(composer.compose_current(&bridge)?.generation, 2);
        Ok(())
    }

    /// Every composition carries the CP-CFG generation it was composed from
    /// as a value of its own: the initial bind composition carries
    /// generation 1, a topology-only wake advances the composer counter but
    /// keeps the config lineage, and a bridge re-compose at an unchanged
    /// composition counter after a newer configuration carries that newer
    /// generation without touching any earlier composition's value (there
    /// is no composer-side history to overwrite; only the serving apply
    /// that installs a view publishes its generation).
    #[test]
    fn compositions_carry_the_config_generation_they_were_composed_from()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir();
        let owned =
            ConfigNamespaceStore::from_toml(b"[proxy]\naddr = \"127.0.0.1:7001\"\n", None, &dir)?;
        let composer = RustConfigComposer::new(owned.clone(), None);
        let bridge = StateSnapshot {
            config: Some(ConfigSnapshot {
                advertised_capability: 123,
                server_version: "TiProxy-test".to_owned(),
                ..ConfigSnapshot::default()
            }),
            ..StateSnapshot::default()
        };
        let initial = composer.compose_current(&bridge)?;
        assert_eq!((initial.generation, initial.config_generation), (1, 1));
        composer.advance_generation()?;
        let topology_wake = composer.compose_current(&bridge)?;
        assert_eq!(
            (topology_wake.generation, topology_wake.config_generation),
            (2, 1),
            "topology-only wake keeps the config lineage"
        );
        owned.apply_toml(
            b"[proxy]\naddr = \"127.0.0.1:7001\"\nmax-connections = 5\n",
            None,
            2,
            &dir,
        )?;
        assert_eq!(owned.current().generation(), 2);
        let bridge_recompose = composer.compose_current(&bridge)?;
        assert_eq!(
            (
                bridge_recompose.generation,
                bridge_recompose.config_generation
            ),
            (2, 2),
            "a bridge re-compose before the adapter wake carries the new config"
        );
        assert_eq!(
            topology_wake.config_generation, 1,
            "the earlier composition still reports the generation it used"
        );
        composer.advance_generation()?;
        let adapter_wake = composer.compose_current(&bridge)?;
        assert_eq!(
            (adapter_wake.generation, adapter_wake.config_generation),
            (3, 2)
        );
        Ok(())
    }

    #[test]
    fn candidate_validator_rejects_unsupported_serving_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshots = SnapshotStore::new([])?;
        let validator = Arc::new(ServingCandidateValidator::new(snapshots, None));
        let store = ConfigNamespaceStore::new_with_validator(
            EffectiveConfig::default(),
            Vec::new(),
            SourceRevision::default(),
            Path::new("/tmp"),
            validator,
        )?;
        assert!(
            store.current().effective().traffic_replay_enabled(),
            "the Go default enable-traffic-replay=true must be accepted"
        );
        assert!(
            store
                .current()
                .effective()
                .serving()?
                .traffic_replay_enabled
        );
        let server_auto = ConfigNamespaceStore::from_toml_with_validator(
            b"enable-traffic-replay = false\n[security.server-tls]\nauto-certs = true\n",
            None,
            Path::new("/tmp"),
            Arc::new(ServingCandidateValidator::new(
                SnapshotStore::new([])?,
                None,
            )),
        );
        assert!(matches!(
            server_auto,
            Err(StoreError::CandidateRejected {
                class: "serving_auto_certs_unsupported"
            })
        ));
        for (toml, class) in [
            (
                &b"api.proxy-protocol = \"v2\"\n"[..],
                "api_proxy_protocol_unsupported",
            ),
            (
                &b"security.encryption-key-path = \"/tmp/key\"\n"[..],
                "encryption_key_path_unsupported",
            ),
            (
                &b"ha.virtual-ip = \"192.0.2.10\"\nha.interface = \"eth0\"\n"[..],
                "ha_vip_unsupported",
            ),
        ] {
            let rejected = ConfigNamespaceStore::from_toml_with_validator(
                toml,
                None,
                Path::new("/tmp"),
                Arc::new(ServingCandidateValidator::new(
                    SnapshotStore::new([])?,
                    None,
                )),
            );
            assert!(
                matches!(
                    rejected,
                    Err(StoreError::CandidateRejected { class: got }) if got == class
                ),
                "{class} was not rejected"
            );
        }
        let vip_only = ConfigNamespaceStore::from_toml_with_validator(
            b"ha.virtual-ip = \"192.0.2.10\"\n",
            None,
            Path::new("/tmp"),
            Arc::new(ServingCandidateValidator::new(
                SnapshotStore::new([])?,
                None,
            )),
        )?;
        assert_eq!(vip_only.current().effective().ha_virtual_ip(), "192.0.2.10");
        Ok(())
    }

    #[test]
    fn candidate_validator_ignores_client_side_auto_certs_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshots = SnapshotStore::new([])?;
        let validator = Arc::new(ServingCandidateValidator::new(snapshots, None));
        let store = ConfigNamespaceStore::from_toml_with_validator(
            b"enable-traffic-replay = false\n[security.cluster-tls]\nauto-certs = true\n[security.sql-tls]\nauto-certs = true\n",
            None,
            Path::new("/tmp"),
            validator,
        )?;
        let namespace = control_config::source::decode_namespace(
            "alpha",
            br#"{"namespace":"alpha","backend":{"security":{"auto-certs":true}}}"#,
        )?;
        assert!(
            store
                .apply_persistent(
                    PersistentConfigSnapshot {
                        namespaces: vec![namespace],
                        ..PersistentConfigSnapshot::default()
                    },
                    2,
                    Path::new("/tmp"),
                )
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn candidate_validator_covers_detached_cluster_and_namespace_tls_material()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = TestDirectory::create()?;
        let invalid_ca = directory.0.join("invalid-ca.pem");
        fs::write(&invalid_ca, b"not a certificate")?;
        let validator = Arc::new(ServingCandidateValidator::new(
            SnapshotStore::new([directory.0.clone()])?,
            None,
        ));
        let config = format!(
            "enable-traffic-replay = false\n[security.cluster-tls]\nca = {:?}\n",
            invalid_ca.to_string_lossy()
        );
        assert!(matches!(
            ConfigNamespaceStore::from_toml_with_validator(
                config.as_bytes(),
                None,
                directory.0.as_path(),
                Arc::clone(&validator) as Arc<dyn CandidateValidator>,
            ),
            Err(StoreError::CandidateRejected {
                class: "tls_material_validation"
            })
        ));

        let store = ConfigNamespaceStore::from_toml_with_validator(
            b"enable-traffic-replay = false\n",
            None,
            directory.0.as_path(),
            validator,
        )?;
        let namespace = control_config::source::decode_namespace(
            "alpha",
            format!(
                "{{\"namespace\":\"alpha\",\"backend\":{{\"security\":{{\"ca\":{:?}}}}}}}",
                invalid_ca.to_string_lossy()
            )
            .as_bytes(),
        )?;
        assert!(matches!(
            store.apply_persistent(
                PersistentConfigSnapshot {
                    namespaces: vec![namespace],
                    ..PersistentConfigSnapshot::default()
                },
                2,
                directory.0.as_path(),
            ),
            Err(StoreError::CandidateRejected {
                class: "tls_material_validation"
            })
        ));
        Ok(())
    }

    #[test]
    fn coalesced_source_updates_advance_cp001_by_one() {
        // A watch receiver may observe CP-CFG generation 9 immediately after
        // generation 6. CP-001 still requires its own successor 42 -> 43;
        // source revisions/generations must never leak into that lineage.
        assert_eq!(next_consumer_generation(42), Ok(43));
        assert!(next_consumer_generation(u64::MAX).is_err());
    }

    #[tokio::test]
    async fn adapter_applies_a_generation_accepted_before_it_starts()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = ConfigNamespaceStore::from_toml(
            b"enable-traffic-replay = false\n",
            None,
            Path::new("/tmp"),
        )?;
        let initial = control_config(source.current().as_ref(), 0, &[], None)?;
        let registry = OwnershipRegistry::new();
        let runtime = Arc::new(InProcessControlRuntime::claim_process(
            &registry,
            "config-adapter-catch-up",
            initial,
            Arc::new(control_plane::JsonStderrSink),
        )?);

        source.apply_toml(b"[log]\nlevel = 'debug'\n", None, 2, Path::new("/tmp"))?;
        assert_eq!(source.current().generation(), 2);

        let snapshots = SnapshotStore::new([])?;
        let composer = Arc::new(RustConfigComposer::new(source.clone(), None));
        let handler: Arc<dyn dataplane::ConnectionHandler> =
            Arc::new(|_connection: dataplane::AcceptedConnection| async {});
        let (_consumer, serving) = dataplane::DataplaneSnapshotConsumer::new_with_composer(
            Arc::new(dataplane::SystemMemoryProbe::new()),
            handler,
            composer.clone(),
        );
        let adapter = ConfigServingAdapter::new(
            serving,
            snapshots,
            Arc::clone(&runtime),
            0,
            Vec::new(),
            None,
            composer,
        );
        let context = runtime.handle().module_context();
        let task = tokio::spawn(adapter.run_inner(context));

        let mut applied = runtime.handle().config().subscribe();
        tokio::time::timeout(Duration::from_secs(1), async {
            while applied.borrow().generation() == 1 {
                applied.changed().await?;
            }
            Ok::<(), tokio::sync::watch::error::RecvError>(())
        })
        .await??;
        assert_eq!(applied.borrow().generation(), 2);
        assert_eq!(applied.borrow().log_level(), LogLevel::Debug);

        runtime.begin_shutdown(ShutdownReason::Signal)?;
        task.await??;
        runtime.advance_shutdown(LifecyclePhase::Draining)?;
        runtime.advance_shutdown(LifecyclePhase::Stopping)?;
        runtime.finish()?;
        Ok(())
    }
}
