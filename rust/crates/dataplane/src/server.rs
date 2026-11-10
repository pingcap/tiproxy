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

//! Multi-listener SQL accept lifecycle and the session-runtime composition
//! hook. This module stops at an admitted, registered `TcpStream`; DPL-01 owns
//! `MySQL` session execution.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use control_proto::snapshot::ValidatedSnapshot;
use control_proto::v1::{ConfigSnapshot, KeepalivePolicy as SnapshotKeepalive, StateSnapshot};
use proxy_io::socket::{
    BoundListener, KeepalivePolicy, SocketError, apply_keepalive, bind_listeners, configure_stream,
};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::admission::{
    AdmissionController, AdmissionPolicy, AdmissionPolicyError, AdmissionRejection, MemoryProbe,
};
use crate::registry::{ConnectionLease, ConnectionMetadata, ConnectionRegistry, RegistryError};

const ACCEPT_BACKOFF_INITIAL: Duration = Duration::from_millis(5);
const ACCEPT_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// How much longer the listener-task join waits than the session-task
/// join it contains, so the inner grace can elapse first.
const FORCE_JOIN_MARGIN: Duration = Duration::from_secs(1);

/// Immutable configured SQL listener.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ListenerSpec {
    /// Stable listener name projected by the Go control plane.
    pub name: String,
    /// Host or IP without brackets. Empty means IPv4 unspecified.
    pub address: String,
    /// TCP port. Zero is supported by the topology test hook.
    pub port: u16,
}

impl ListenerSpec {
    /// Creates a listener specification.
    ///
    /// # Errors
    ///
    /// Returns an invalid-listener error for an empty name or malformed host.
    pub fn new(
        name: impl Into<String>,
        address: impl Into<String>,
        port: u16,
    ) -> Result<Self, ServerError> {
        let name = name.into();
        let address = address.into();
        if name.is_empty() {
            return Err(ServerError::InvalidListener("listener name is empty"));
        }
        if address.contains(['[', ']', '/']) || address.chars().any(char::is_whitespace) {
            return Err(ServerError::InvalidListener(
                "listener address is malformed",
            ));
        }
        Ok(Self {
            name,
            address,
            port,
        })
    }
}

/// Configured and actual address for one successfully bound listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundListenerInfo {
    /// Stable configured listener name.
    pub name: Arc<str>,
    /// Host/IP and requested port from the initial snapshot.
    pub configured_address: String,
    /// OS-reported address, including the real ephemeral port.
    pub actual_address: SocketAddr,
}

struct NamedListener {
    name: Arc<str>,
    listener: TcpListener,
    actual_address: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenerSignature(Vec<(String, String, u32)>);

impl ListenerSignature {
    fn from_snapshot(snapshot: &ValidatedSnapshot) -> Result<Self, ServerError> {
        let config = snapshot_config(snapshot)?;
        Ok(Self(
            config
                .listeners
                .iter()
                .map(|listener| {
                    (
                        listener.name.clone(),
                        listener.address.clone(),
                        listener.port,
                    )
                })
                .collect(),
        ))
    }
}

#[derive(Debug, Default)]
struct ServerCounters {
    accept_errors: AtomicU64,
    socket_policy_failures: AtomicU64,
    registration_failures: AtomicU64,
    handler_panics: AtomicU64,
}

/// Connection and listener lifecycle metrics without a Prometheus dependency.
/// DPL-05 exports these atomics through the control-plane metrics batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerMetricsSnapshot {
    /// Current admitted connections.
    pub active_connections: u64,
    /// Current connection-buffer reservation.
    pub connection_buffer_bytes: u64,
    /// Connections successfully inserted into the registry.
    pub registered_total: u64,
    /// Connections rejected for memory pressure.
    pub rejected_memory_total: u64,
    /// Connections rejected at the configured maximum.
    pub rejected_max_connections_total: u64,
    /// Memory samples that failed open.
    pub memory_probe_failures_total: u64,
    /// Listener accept errors that entered bounded backoff.
    pub accept_errors_total: u64,
    /// Accepted sockets on which a best-effort policy could not be applied.
    pub socket_policy_failures_total: u64,
    /// Stable-ID/registry failures after admission.
    pub registration_failures_total: u64,
    /// Panics contained to one injected connection handler.
    pub handler_panics_total: u64,
}

/// Cloneable control surface for snapshot reload, shutdown, diagnostics, and
/// the registry consumed by later control-command handlers.
#[derive(Clone)]
pub struct DataplaneHandle {
    snapshot_tx: watch::Sender<Arc<ValidatedSnapshot>>,
    shutdown_tx: watch::Sender<bool>,
    draining_tx: watch::Sender<bool>,
    listener_signature: ListenerSignature,
    listeners: Arc<[BoundListenerInfo]>,
    admission: AdmissionController,
    registry: ConnectionRegistry,
    counters: Arc<ServerCounters>,
}

impl DataplaneHandle {
    /// Returns the load-balancer removal window from the last applied serving
    /// snapshot. A candidate that failed validation must not change shutdown.
    #[must_use]
    pub fn graceful_wait_before_shutdown(&self) -> Duration {
        Duration::from_millis(
            self.snapshot_tx
                .borrow()
                .raw()
                .config
                .as_ref()
                .map_or(0, |config| config.graceful_wait_millis),
        )
    }

    /// Atomically publishes a complete validated generation for new
    /// admissions/sessions. Existing sessions retain their captured `Arc`,
    /// while their private watch receiver exposes the explicit live-topology
    /// exceptions (currently backend-health keepalive refresh). Listener
    /// changes fail because listeners are restart-required in v1.
    ///
    /// # Errors
    ///
    /// Returns a preflight, policy, or restart-required error without changing
    /// the last-good generation.
    pub fn update_snapshot(&self, snapshot: Arc<ValidatedSnapshot>) -> Result<(), ServerError> {
        preflight_snapshot(snapshot.raw())?;
        if ListenerSignature::from_snapshot(&snapshot)? != self.listener_signature {
            return Err(ServerError::ListenerReloadRequiresRestart);
        }
        let _ = policy_from_snapshot(&snapshot)?;
        self.snapshot_tx.send_replace(snapshot);
        Ok(())
    }

    /// Requests idempotent listener and session-task shutdown.
    pub fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
    }

    /// Stops accepting new connections on every listener while existing
    /// sessions keep running — the first phase of the coordinated
    /// shutdown order (stop-accept → graceful drain → deadline force →
    /// join). [`DataplaneHandle::shutdown`] remains the force phase.
    pub fn stop_accepting(&self) {
        self.draining_tx.send_replace(true);
    }

    /// Whether accepting has been stopped.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        *self.draining_tx.borrow()
    }

    /// Returns whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    /// Returns all actual bound addresses in configured order.
    #[must_use]
    pub fn listeners(&self) -> Arc<[BoundListenerInfo]> {
        Arc::clone(&self.listeners)
    }

    /// Returns the shared, payload-free live registry.
    #[must_use]
    pub fn registry(&self) -> ConnectionRegistry {
        self.registry.clone()
    }

    /// Returns a consistent-enough atomic metrics observation. Live gauge and
    /// registry count converge exactly after every lifecycle transition.
    #[must_use]
    pub fn metrics(&self) -> ServerMetricsSnapshot {
        let admission = self.admission.metrics();
        let registry = self.registry.snapshot();
        ServerMetricsSnapshot {
            active_connections: admission.active_connections,
            connection_buffer_bytes: admission.connection_buffer_bytes,
            registered_total: registry.registered_total,
            rejected_memory_total: admission.rejected_memory_total,
            rejected_max_connections_total: admission.rejected_max_connections_total,
            memory_probe_failures_total: admission.memory_probe_failures_total,
            accept_errors_total: self.counters.accept_errors.load(Ordering::Relaxed),
            socket_policy_failures_total: self
                .counters
                .socket_policy_failures
                .load(Ordering::Relaxed),
            registration_failures_total: self
                .counters
                .registration_failures
                .load(Ordering::Relaxed),
            handler_panics_total: self.counters.handler_panics.load(Ordering::Relaxed),
        }
    }
}

/// Admitted client socket passed to the DPL-01 session-runtime hook. The
/// private lease keeps registry membership and admission gauges alive for
/// exactly as long as this value.
pub struct AcceptedConnection {
    stream: TcpStream,
    snapshot: Arc<ValidatedSnapshot>,
    snapshot_updates: watch::Receiver<Arc<ValidatedSnapshot>>,
    lease: ConnectionLease,
}

impl AcceptedConnection {
    /// Returns payload-free identity and accounting metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ConnectionMetadata {
        self.lease.metadata()
    }

    /// Returns the immutable generation captured at admission.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<ValidatedSnapshot> {
        &self.snapshot
    }

    /// Borrows the owned client stream.
    #[must_use]
    pub const fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Mutably borrows the owned client stream for session I/O.
    pub const fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Consumes the admission into the owned stream plus a seat that
    /// keeps the registry lease (and the captured snapshot) alive for
    /// the session's whole lifetime. Dropping the seat releases the
    /// admission exactly as dropping the connection would.
    #[must_use]
    pub fn into_session_io(self) -> (TcpStream, SessionSeat) {
        (
            self.stream,
            SessionSeat {
                snapshot: self.snapshot,
                snapshot_updates: self.snapshot_updates,
                lease: self.lease,
            },
        )
    }
}

/// The non-I/O remainder of an admitted connection: the captured
/// snapshot and the registry lease, alive until dropped.
pub struct SessionSeat {
    snapshot: Arc<ValidatedSnapshot>,
    snapshot_updates: watch::Receiver<Arc<ValidatedSnapshot>>,
    lease: ConnectionLease,
}

impl SessionSeat {
    /// The immutable snapshot captured at admission.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<ValidatedSnapshot> {
        &self.snapshot
    }

    /// Subscribes to complete last-good generations published after admission.
    /// The admission snapshot remains immutable; session owners use this feed
    /// only for fields explicitly documented as live, such as backend health.
    #[must_use]
    pub fn subscribe_snapshot_updates(&self) -> watch::Receiver<Arc<ValidatedSnapshot>> {
        self.snapshot_updates.clone()
    }

    /// Payload-free identity and accounting metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ConnectionMetadata {
        self.lease.metadata()
    }
}

impl fmt::Debug for AcceptedConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedConnection")
            .field("metadata", self.metadata())
            .field("snapshot_generation", &self.snapshot.generation())
            .finish_non_exhaustive()
    }
}

/// Boxed, owned connection future used by the DPL-01 composition boundary.
pub type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Injected owner of one admitted connection. A panic is contained to that
/// task; returning or cancellation drops the socket and registry lease.
pub trait ConnectionHandler: Send + Sync + 'static {
    /// Runs one connection until it closes.
    fn handle(&self, connection: AcceptedConnection) -> ConnectionFuture;
}

impl<F, Fut> ConnectionHandler for F
where
    F: Fn(AcceptedConnection) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(&self, connection: AcceptedConnection) -> ConnectionFuture {
        Box::pin(self(connection))
    }
}

/// Bound SQL listeners and their one accept-lifecycle owner.
pub struct DataplaneServer {
    listeners: Vec<NamedListener>,
    handle: DataplaneHandle,
    force_join_grace: Duration,
}

impl DataplaneServer {
    /// Preflights and binds every listener from the initial validated snapshot.
    /// Any bind failure drops all sockets already opened in this attempt.
    ///
    /// # Errors
    ///
    /// Returns a preflight, resolution, policy, or socket bind error.
    pub async fn bind(
        snapshot: Arc<ValidatedSnapshot>,
        memory: Arc<dyn MemoryProbe>,
    ) -> Result<Self, ServerError> {
        let specs = listener_specs(&snapshot)?;
        Self::bind_specs(snapshot, specs, memory).await
    }

    async fn bind_specs(
        snapshot: Arc<ValidatedSnapshot>,
        specs: Vec<ListenerSpec>,
        memory: Arc<dyn MemoryProbe>,
    ) -> Result<Self, ServerError> {
        preflight_snapshot(snapshot.raw())?;
        let _ = policy_from_snapshot(&snapshot)?;
        validate_specs(&specs)?;
        let addresses = resolve_specs(&specs).await?;
        let bound = bind_listeners(&addresses).await?;
        let mut listeners = Vec::with_capacity(bound.len());
        let mut listener_info = Vec::with_capacity(bound.len());
        for (spec, bound) in specs.iter().zip(bound) {
            let BoundListener {
                listener,
                actual_address,
            } = bound;
            let name: Arc<str> = Arc::from(spec.name.as_str());
            listener_info.push(BoundListenerInfo {
                name: Arc::clone(&name),
                configured_address: format_listener_address(&spec.address, spec.port),
                actual_address,
            });
            listeners.push(NamedListener {
                name,
                listener,
                actual_address,
            });
        }
        let (snapshot_tx, _) = watch::channel(Arc::clone(&snapshot));
        let (shutdown_tx, _) = watch::channel(false);
        let (draining_tx, _) = watch::channel(false);
        let admission = AdmissionController::new(memory);
        let registry = ConnectionRegistry::new();
        let counters = Arc::new(ServerCounters::default());
        let handle = DataplaneHandle {
            snapshot_tx,
            shutdown_tx,
            draining_tx,
            listener_signature: ListenerSignature::from_snapshot(&snapshot)?,
            listeners: listener_info.into(),
            admission,
            registry,
            counters,
        };
        Ok(Self {
            listeners,
            handle,
            force_join_grace: Duration::ZERO,
        })
    }

    /// Bounds how long forced shutdown waits for session tasks to finish
    /// their own terminal work (close notices, bounded engine joins)
    /// before the abort backstop. Zero — the default — aborts
    /// immediately; compositions whose sessions self-bound their force
    /// path should pass that bound plus margin.
    #[must_use]
    pub fn with_force_join_grace(mut self, grace: Duration) -> Self {
        self.force_join_grace = grace;
        self
    }

    /// Returns a cloneable control/reload/diagnostic handle before `run`
    /// consumes the listener owner.
    #[must_use]
    pub fn handle(&self) -> DataplaneHandle {
        self.handle.clone()
    }

    /// Runs all listeners independently until handle shutdown. Listener and
    /// connection tasks are always joined or aborted before this returns.
    ///
    /// # Errors
    ///
    /// Returns if a listener owner panics or exits before shutdown.
    pub async fn run<H>(self, handler: H) -> Result<(), ServerError>
    where
        H: ConnectionHandler,
    {
        let handler: Arc<dyn ConnectionHandler> = Arc::new(handler);
        let mut listener_tasks = JoinSet::new();
        for listener in self.listeners {
            listener_tasks.spawn(run_listener(
                listener,
                Arc::clone(&handler),
                self.handle.snapshot_tx.subscribe(),
                self.handle.shutdown_tx.subscribe(),
                self.handle.draining_tx.subscribe(),
                self.handle.admission.clone(),
                self.handle.registry.clone(),
                Arc::clone(&self.handle.counters),
                self.force_join_grace,
            ));
        }
        let mut shutdown = self.handle.shutdown_tx.subscribe();
        let outcome = loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break Ok(());
                    }
                }
                joined = listener_tasks.join_next() => {
                    match joined {
                        Some(Ok(())) if self.handle.is_shutdown() => break Ok(()),
                        Some(Err(error)) if error.is_panic() => {
                            break Err(ServerError::ListenerTaskPanicked);
                        }
                        Some(Ok(()) | Err(_)) | None => {
                            break Err(ServerError::ListenerTaskStopped);
                        }
                    }
                }
            }
        };
        self.handle.shutdown();
        if outcome.is_ok() && !self.force_join_grace.is_zero() {
            // Ordered force: the listener owners are themselves waiting
            // out the same grace for their sessions' terminal work, so
            // hold the abort backstop a margin longer than they do.
            let deadline = sleep(self.force_join_grace + FORCE_JOIN_MARGIN);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    biased;
                    () = &mut deadline => break,
                    joined = listener_tasks.join_next() => {
                        if joined.is_none() {
                            break;
                        }
                    }
                }
            }
        }
        listener_tasks.abort_all();
        while listener_tasks.join_next().await.is_some() {}
        outcome
    }
}

/// Preflights raw control state before any SQL listener is provisioned.
///
/// # Errors
///
/// Returns a configuration error when the complete config is absent.
/// `traffic_replay_enabled` does not fail preflight: the Go default is true,
/// and the native process serves SQL without implementing capture or replay.
pub fn preflight_snapshot(snapshot: &StateSnapshot) -> Result<(), ServerError> {
    if snapshot.config.is_none() {
        return Err(ServerError::MissingConfig);
    }
    Ok(())
}

/// Typed SQL-server startup, reload, registry, and owner failures.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The complete snapshot lacks its required config.
    #[error("dataplane snapshot config is required")]
    MissingConfig,
    /// Traffic capture/replay is explicitly outside the Rust dataplane.
    #[error("traffic capture/replay is unsupported by the Rust dataplane")]
    TrafficReplayUnsupported,
    /// Listener syntax or uniqueness is invalid.
    #[error("invalid listener configuration: {0}")]
    InvalidListener(&'static str),
    /// A configured hostname had no usable socket address.
    #[error("listener {listener_name} cannot be resolved")]
    ListenerResolution {
        /// Stable listener name; host input is omitted from diagnostics.
        listener_name: String,
    },
    /// A reload attempted to change restart-required listeners.
    #[error("listener configuration changed; restart is required")]
    ListenerReloadRequiresRestart,
    /// The validated snapshot could not produce a new-session policy.
    #[error(transparent)]
    AdmissionPolicy(#[from] AdmissionPolicyError),
    /// A required frontend keepalive policy is absent.
    #[error("frontend keepalive policy is required")]
    MissingFrontendKeepalive,
    /// Binding or configuring the transport failed.
    #[error(transparent)]
    Socket(#[from] SocketError),
    /// Stable registry allocation failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// A listener owner exited without shutdown.
    #[error("SQL listener task stopped before shutdown")]
    ListenerTaskStopped,
    /// A listener owner panicked; all peers were stopped and joined.
    #[error("SQL listener task panicked")]
    ListenerTaskPanicked,
}

#[derive(Debug, Clone, Copy)]
struct NewSessionPolicy {
    admission: AdmissionPolicy,
    frontend_keepalive: KeepalivePolicy,
}

fn policy_from_snapshot(snapshot: &ValidatedSnapshot) -> Result<NewSessionPolicy, ServerError> {
    let config = snapshot_config(snapshot)?;
    let admission = AdmissionPolicy::new(
        config.max_connections,
        config.high_memory_reject_threshold,
        config.connection_buffer_bytes,
    )?;
    let frontend_keepalive = snapshot_keepalive(
        config
            .frontend_keepalive
            .as_ref()
            .ok_or(ServerError::MissingFrontendKeepalive)?,
    );
    Ok(NewSessionPolicy {
        admission,
        frontend_keepalive,
    })
}

fn snapshot_config(snapshot: &ValidatedSnapshot) -> Result<&ConfigSnapshot, ServerError> {
    snapshot
        .raw()
        .config
        .as_ref()
        .ok_or(ServerError::MissingConfig)
}

/// Default per-connection reader buffer when a snapshot carries no config
/// (mirrors `proxy-io`'s 32-KiB `DEFAULT_STREAM_BUFFER_SIZE`).
const DEFAULT_READER_STREAM_BUFFER: std::num::NonZeroUsize =
    match std::num::NonZeroUsize::new(32 * 1024) {
        Some(value) => value,
        None => std::num::NonZeroUsize::MIN,
    };

/// The per-connection reader buffer size for a session admitted under
/// `snapshot`.
///
/// Read from the same committed generation that admission used for the
/// `connection_buffer_bytes * 2` reservation, so the two session reader buffers
/// (the over-read prefetch window and the forward scratch) always match the
/// reserved budget. Because each session captures an immutable seat snapshot,
/// a config reload takes effect on the next admitted session while existing
/// sessions keep their captured size.
#[must_use]
pub(crate) fn reader_stream_buffer_size(snapshot: &ValidatedSnapshot) -> std::num::NonZeroUsize {
    snapshot_config(snapshot)
        .ok()
        .and_then(|config| std::num::NonZeroUsize::new(config.connection_buffer_bytes as usize))
        .unwrap_or(DEFAULT_READER_STREAM_BUFFER)
}

/// Converts a snapshot keepalive policy into the socket layer's.
#[must_use]
pub fn snapshot_keepalive(policy: &SnapshotKeepalive) -> KeepalivePolicy {
    KeepalivePolicy {
        enabled: policy.enabled,
        idle: Duration::from_millis(policy.idle_millis),
        probes: policy.probe_count,
        interval: Duration::from_millis(policy.interval_millis),
        user_timeout: Duration::from_millis(policy.user_timeout_millis),
    }
}

fn listener_specs(snapshot: &ValidatedSnapshot) -> Result<Vec<ListenerSpec>, ServerError> {
    snapshot_config(snapshot)?
        .listeners
        .iter()
        .map(|listener| {
            let port = u16::try_from(listener.port)
                .map_err(|_| ServerError::InvalidListener("listener port exceeds u16"))?;
            ListenerSpec::new(&listener.name, &listener.address, port)
        })
        .collect()
}

fn validate_specs(specs: &[ListenerSpec]) -> Result<(), ServerError> {
    if specs.is_empty() {
        return Err(ServerError::InvalidListener("no SQL listeners configured"));
    }
    let mut names = BTreeSet::new();
    let mut addresses = BTreeSet::new();
    for spec in specs {
        if !names.insert(spec.name.as_str()) {
            return Err(ServerError::InvalidListener("duplicate listener name"));
        }
        if !addresses.insert((spec.address.as_str(), spec.port)) {
            return Err(ServerError::InvalidListener("duplicate listener address"));
        }
    }
    Ok(())
}

async fn resolve_specs(specs: &[ListenerSpec]) -> Result<Vec<SocketAddr>, ServerError> {
    let mut addresses = Vec::with_capacity(specs.len());
    for spec in specs {
        let address = if spec.address.is_empty() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), spec.port)
        } else if let Ok(ip) = spec.address.parse::<IpAddr>() {
            SocketAddr::new(ip, spec.port)
        } else {
            lookup_host((spec.address.as_str(), spec.port))
                .await
                .map_err(|_| ServerError::ListenerResolution {
                    listener_name: spec.name.clone(),
                })?
                .next()
                .ok_or_else(|| ServerError::ListenerResolution {
                    listener_name: spec.name.clone(),
                })?
        };
        addresses.push(address);
    }
    Ok(addresses)
}

fn format_listener_address(host: &str, port: u16) -> String {
    if let Ok(ip) = host.parse::<IpAddr>() {
        SocketAddr::new(ip, port).to_string()
    } else {
        format!("{host}:{port}")
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_listener(
    named: NamedListener,
    handler: Arc<dyn ConnectionHandler>,
    snapshot_rx: watch::Receiver<Arc<ValidatedSnapshot>>,
    mut shutdown: watch::Receiver<bool>,
    mut draining: watch::Receiver<bool>,
    admission: AdmissionController,
    registry: ConnectionRegistry,
    counters: Arc<ServerCounters>,
    force_join_grace: Duration,
) {
    let NamedListener {
        name,
        listener,
        actual_address,
    } = named;
    let mut sessions = JoinSet::new();
    let mut backoff = AcceptBackoff::new();
    loop {
        if *shutdown.borrow() || *draining.borrow() {
            break;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            changed = draining.changed() => {
                if changed.is_err() || *draining.borrow() {
                    // Stop-accept phase: close the listener but keep
                    // every running session alive.
                    break;
                }
            }
            joined = sessions.join_next(), if !sessions.is_empty() => {
                if joined.is_some_and(|result| result.is_err_and(|error| error.is_panic())) {
                    counters.handler_panics.fetch_add(1, Ordering::Relaxed);
                }
            }
            accepted = listener.accept() => {
                if let Ok((stream, peer_address)) = accepted {
                    backoff.reset();
                    let snapshot = Arc::clone(&snapshot_rx.borrow());
                    if let Some(connection) = prepare_connection(
                        stream,
                        peer_address,
                        Arc::clone(&name),
                        actual_address,
                        snapshot,
                        snapshot_rx.clone(),
                        &admission,
                        &registry,
                        &counters,
                    ) {
                        let handler = Arc::clone(&handler);
                        sessions.spawn(async move {
                            handler.handle(connection).await;
                        });
                    }
                } else {
                    counters.accept_errors.fetch_add(1, Ordering::Relaxed);
                    let delay = backoff.fail();
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        () = sleep(delay) => {}
                    }
                }
            }
        }
    }
    drop(listener);
    // Drain mode: sessions keep running until they finish on their own
    // (the composition injects graceful closes and per-session drain
    // deadlines); the shutdown signal remains the force phase that
    // aborts whatever is left. The owner itself stays alive until that
    // force phase even when every session has already finished: the
    // server loop treats a listener owner that returns before shutdown
    // as a failed listener, so an early return here would turn a clean
    // drain into "SQL listener task stopped before shutdown".
    if !*shutdown.borrow() {
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                joined = sessions.join_next(), if !sessions.is_empty() => {
                    if joined.is_some_and(|result| result.is_err_and(|error| error.is_panic())) {
                        counters.handler_panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
    join_sessions_bounded(&mut sessions, force_join_grace, &counters).await;
    sessions.abort_all();
    while let Some(joined) = sessions.join_next().await {
        if joined.is_err_and(|error| error.is_panic()) {
            counters.handler_panics.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Force phase: session owners still emit their close notices and join
/// their engines under the per-session cleanup bound; hold the abort
/// backstop until they finish or the grace ends.
async fn join_sessions_bounded(
    sessions: &mut JoinSet<()>,
    grace: Duration,
    counters: &ServerCounters,
) {
    if grace.is_zero() || sessions.is_empty() {
        return;
    }
    let deadline = sleep(grace);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            () = &mut deadline => break,
            joined = sessions.join_next() => {
                match joined {
                    Some(result) => {
                        if result.is_err_and(|error| error.is_panic()) {
                            counters.handler_panics.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    None => break,
                }
            }
        }
        if sessions.is_empty() {
            break;
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the accept boundary intentionally makes every owned resource and address explicit"
)]
fn prepare_connection(
    stream: TcpStream,
    peer_address: SocketAddr,
    listener_name: Arc<str>,
    listener_address: SocketAddr,
    snapshot: Arc<ValidatedSnapshot>,
    snapshot_updates: watch::Receiver<Arc<ValidatedSnapshot>>,
    admission: &AdmissionController,
    registry: &ConnectionRegistry,
    counters: &ServerCounters,
) -> Option<AcceptedConnection> {
    let Ok(policy) = policy_from_snapshot(&snapshot) else {
        return None;
    };
    // Preserve Go's admission order: memory, then max-connections, before
    // allocating an ID or any session-owned state.
    let permit = match admission.try_acquire(policy.admission) {
        Ok(permit) => permit,
        Err(AdmissionRejection::Memory { .. } | AdmissionRejection::MaxConnections { .. }) => {
            return None;
        }
    };
    if configure_stream(&stream).is_err() {
        counters
            .socket_policy_failures
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let Ok(lease) = registry.register(
        permit,
        snapshot.generation(),
        listener_name,
        listener_address,
        peer_address,
    ) else {
        counters
            .registration_failures
            .fetch_add(1, Ordering::Relaxed);
        return None;
    };
    // Go applies frontend keepalive after registration, logs failures, and
    // continues serving the socket.
    if apply_keepalive(&stream, policy.frontend_keepalive).is_err() {
        counters
            .socket_policy_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    Some(AcceptedConnection {
        stream,
        snapshot,
        snapshot_updates,
        lease,
    })
}

#[derive(Debug, Clone, Copy)]
struct AcceptBackoff {
    next: Duration,
}

impl AcceptBackoff {
    const fn new() -> Self {
        Self {
            next: ACCEPT_BACKOFF_INITIAL,
        }
    }

    fn fail(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(ACCEPT_BACKOFF_MAX);
        delay
    }

    const fn reset(&mut self) {
        self.next = ACCEPT_BACKOFF_INITIAL;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::sync::Mutex;

    use control_proto::snapshot::{SnapshotLineage, SnapshotStore};
    use control_proto::v1::{
        ConfigSnapshot, KeepalivePolicy as SnapshotKeepalive, Listener, ProxyProtocolMode,
        TlsPolicy,
    };
    use rustls::pki_types::UnixTime;
    use tokio::io::AsyncReadExt;
    use tokio::sync::mpsc;
    use tokio::time::{Duration as TokioDuration, timeout};

    use crate::{MemoryProbeError, MemorySample};

    #[derive(Debug)]
    struct MutableMemory(Mutex<Result<MemorySample, MemoryProbeError>>);

    impl MutableMemory {
        fn new(used_bytes: u64, limit_bytes: u64) -> Self {
            Self(Mutex::new(Ok(MemorySample::now(used_bytes, limit_bytes))))
        }

        fn set(&self, used_bytes: u64, limit_bytes: u64) {
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Ok(MemorySample::now(used_bytes, limit_bytes));
        }
    }

    impl MemoryProbe for MutableMemory {
        fn sample(&self) -> Result<MemorySample, MemoryProbeError> {
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    fn snapshot(
        generation: u64,
        max_connections: u64,
        threshold: f64,
        listeners: Vec<Listener>,
    ) -> Result<Arc<ValidatedSnapshot>, Box<dyn Error>> {
        let keepalive = SnapshotKeepalive {
            enabled: true,
            idle_millis: 0,
            probe_count: 0,
            interval_millis: 0,
            user_timeout_millis: 0,
        };
        let raw = StateSnapshot {
            config: Some(ConfigSnapshot {
                max_connections,
                high_memory_reject_threshold: threshold,
                connection_buffer_bytes: 4096,
                frontend_keepalive: Some(keepalive),
                healthy_backend_keepalive: Some(keepalive),
                unhealthy_backend_keepalive: Some(keepalive),
                proxy_protocol: ProxyProtocolMode::Disabled as i32,
                listeners,
                server_version: "TiProxy-test".to_owned(),
                frontend_tls: Some(TlsPolicy::default()),
                backend_tls: Some(TlsPolicy::default()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let store = SnapshotStore::new([])?;
        Ok(store
            .apply(
                generation,
                raw,
                UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
                SnapshotLineage::for_tests("go-fixture"),
            )?
            .snapshot)
    }

    /// A snapshot that starts from [`snapshot`]'s baseline and applies one
    /// field change, so a reload test states exactly what it varied.
    fn snapshot_with(
        generation: u64,
        listeners: Vec<Listener>,
        mutate: impl FnOnce(&mut ConfigSnapshot),
    ) -> Result<Arc<ValidatedSnapshot>, Box<dyn Error>> {
        let keepalive = SnapshotKeepalive {
            enabled: true,
            idle_millis: 0,
            probe_count: 0,
            interval_millis: 0,
            user_timeout_millis: 0,
        };
        let mut config = ConfigSnapshot {
            max_connections: 0,
            high_memory_reject_threshold: 0.0,
            connection_buffer_bytes: 4096,
            frontend_keepalive: Some(keepalive),
            healthy_backend_keepalive: Some(keepalive),
            unhealthy_backend_keepalive: Some(keepalive),
            proxy_protocol: ProxyProtocolMode::Disabled as i32,
            listeners,
            server_version: "TiProxy-test".to_owned(),
            frontend_tls: Some(TlsPolicy::default()),
            backend_tls: Some(TlsPolicy::default()),
            ..Default::default()
        };
        mutate(&mut config);
        let raw = StateSnapshot {
            config: Some(config),
            ..Default::default()
        };
        let store = SnapshotStore::new([])?;
        Ok(store
            .apply(
                generation,
                raw,
                UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
                SnapshotLineage::for_tests("go-fixture"),
            )?
            .snapshot)
    }

    #[tokio::test]
    async fn lb_wait_reads_last_applied_serving_generation() -> Result<(), Box<dyn Error>> {
        let first = snapshot_with(1, one_listener(), |config| {
            config.graceful_wait_millis = 250;
            config.graceful_close_millis = 9_000;
        })?;
        let server = ephemeral_server(first, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        assert_eq!(
            handle.graceful_wait_before_shutdown(),
            Duration::from_millis(250)
        );

        let second = snapshot_with(2, one_listener(), |config| {
            config.graceful_wait_millis = 800;
            config.graceful_close_millis = 1;
        })?;
        handle.update_snapshot(second)?;
        assert_eq!(
            handle.graceful_wait_before_shutdown(),
            Duration::from_millis(800),
            "LB wait is independent of the session drain grace"
        );
        let mut changed_listener = one_listener();
        changed_listener[0].port += 1;
        let rejected = snapshot_with(3, changed_listener, |config| {
            config.graceful_wait_millis = 4_000;
        })?;
        assert!(handle.update_snapshot(rejected).is_err());
        assert_eq!(
            handle.graceful_wait_before_shutdown(),
            Duration::from_millis(800),
            "a rejected serving generation cannot extend shutdown"
        );
        Ok(())
    }

    fn one_listener() -> Vec<Listener> {
        vec![Listener {
            address: "127.0.0.1".to_owned(),
            port: 6000,
            name: "sql-0".to_owned(),
        }]
    }

    async fn ephemeral_server(
        snapshot: Arc<ValidatedSnapshot>,
        memory: Arc<MutableMemory>,
    ) -> Result<DataplaneServer, ServerError> {
        DataplaneServer::bind_specs(
            snapshot,
            vec![ListenerSpec::new("sql-0", "127.0.0.1", 0)?],
            memory,
        )
        .await
    }

    #[tokio::test]
    async fn actual_address_keepalive_and_shutdown_cleanup() -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 0, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        assert_ne!(actual.port(), 0);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let keepalive = proxy_io::socket::read_keepalive(connection.stream())
                    .map(|state| state.enabled)
                    .unwrap_or(false);
                let _ = tx.send((connection.metadata().clone(), keepalive));
                std::future::pending::<()>().await;
            }
        }));
        let mut client = TcpStream::connect(actual).await?;
        let (metadata, keepalive) = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("handler did not report")?;
        assert_eq!(metadata.listener_address, actual);
        assert_eq!(metadata.connection_id.get(), 1);
        assert!(keepalive);
        assert_eq!(handle.registry().len(), 1);

        handle.shutdown();
        owner.await??;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(TokioDuration::from_secs(2), client.read(&mut byte)).await??,
            0
        );
        assert!(TcpStream::connect(actual).await.is_err());
        assert!(handle.registry().is_empty());
        let metrics = handle.metrics();
        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.connection_buffer_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn session_finishing_during_stop_accept_keeps_owner_until_shutdown()
    -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 0, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let (mut stream, _seat) = connection.into_session_io();
                let _ = tx.send(());
                let mut byte = [0_u8; 1];
                while matches!(stream.read(&mut byte).await, Ok(read) if read > 0) {}
            }
        }));
        let client = TcpStream::connect(actual).await?;
        timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("handler did not report")?;

        handle.stop_accepting();
        assert!(TcpStream::connect(actual).await.is_err());
        // The only session finishes on its own inside the drain window.
        drop(client);
        assert!(
            timeout(TokioDuration::from_millis(500), &mut owner)
                .await
                .is_err(),
            "listener owner must stay alive until the force phase"
        );
        assert!(handle.registry().is_empty());

        handle.shutdown();
        timeout(TokioDuration::from_secs(2), &mut owner).await???;
        Ok(())
    }

    #[tokio::test]
    async fn existing_session_keeps_admission_snapshot_and_observes_live_generation()
    -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 0, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let (_stream, seat) = connection.into_session_io();
                let admission_generation = seat.snapshot().generation();
                let mut updates = seat.subscribe_snapshot_updates();
                let _ = tx.send((admission_generation, None));
                if updates.changed().await.is_ok() {
                    let live_generation = updates.borrow_and_update().generation();
                    let _ = tx.send((seat.snapshot().generation(), Some(live_generation)));
                }
                std::future::pending::<()>().await;
            }
        }));

        let _client = TcpStream::connect(actual).await?;
        assert_eq!(
            timeout(TokioDuration::from_secs(2), rx.recv()).await?,
            Some((1, None))
        );
        handle.update_snapshot(snapshot(2, 0, 0.0, one_listener())?)?;
        assert_eq!(
            timeout(TokioDuration::from_secs(2), rx.recv()).await?,
            Some((1, Some(2))),
            "live generations must not replace the admission snapshot"
        );

        handle.shutdown();
        owner.await??;
        Ok(())
    }

    #[tokio::test]
    async fn owner_cancellation_aborts_children_and_releases_every_resource()
    -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 0, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(connection.metadata().connection_id);
                std::future::pending::<()>().await;
            }
        }));
        let mut client = TcpStream::connect(actual).await?;
        let _ = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("handler did not start")?;
        assert_eq!(handle.registry().len(), 1);

        owner.abort();
        let cancellation = owner.await;
        assert!(cancellation.is_err_and(|error| error.is_cancelled()));
        timeout(TokioDuration::from_secs(2), async {
            while !handle.registry().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(TokioDuration::from_secs(2), client.read(&mut byte)).await??,
            0
        );
        assert!(TcpStream::connect(actual).await.is_err());
        let metrics = handle.metrics();
        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.connection_buffer_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_rejection_precedes_id_and_reload_applies_to_new_connection()
    -> Result<(), Box<dyn Error>> {
        let memory = Arc::new(MutableMemory::new(900, 1000));
        let snap = snapshot(1, 0, 0.9, one_listener())?;
        let server = ephemeral_server(snap, Arc::clone(&memory)).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(connection.metadata().connection_id);
                std::future::pending::<()>().await;
            }
        }));

        let mut rejected = TcpStream::connect(actual).await?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(TokioDuration::from_secs(2), rejected.read(&mut byte)).await??,
            0
        );
        assert_eq!(handle.metrics().rejected_memory_total, 1);
        assert_eq!(handle.metrics().registered_total, 0);
        assert!(handle.registry().is_empty());

        memory.set(1, 1000);
        handle.update_snapshot(snapshot(2, 0, 0.0, one_listener())?)?;
        let _accepted = TcpStream::connect(actual).await?;
        let first_id = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("accepted connection not reported")?;
        assert_eq!(first_id.get(), 1, "memory reject must not consume an ID");
        handle.shutdown();
        owner.await??;
        assert!(handle.registry().is_empty());
        assert_eq!(handle.metrics().active_connections, 0);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_connections_reject_at_boundary_without_gauge_drift()
    -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 2, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(connection.metadata().connection_id);
                std::future::pending::<()>().await;
            }
        }));
        let _first = TcpStream::connect(actual).await?;
        let _second = TcpStream::connect(actual).await?;
        let _ = timeout(TokioDuration::from_secs(2), rx.recv()).await?;
        let _ = timeout(TokioDuration::from_secs(2), rx.recv()).await?;
        let mut third = TcpStream::connect(actual).await?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(TokioDuration::from_secs(2), third.read(&mut byte)).await??,
            0
        );
        assert_eq!(handle.registry().len(), 2);
        let metrics = handle.metrics();
        assert_eq!(metrics.active_connections, 2);
        assert_eq!(metrics.rejected_max_connections_total, 1);

        // `0` means unlimited, and the bounded reject above happened
        // BEFORE an ID was created: the next admitted connection takes 3,
        // not 4, and the old bound of 2 no longer holds any of them back.
        let mut unlimited = Vec::new();
        handle.update_snapshot(snapshot(2, 0, 0.0, one_listener())?)?;
        for expected_id in 3_u64..=5 {
            unlimited.push(TcpStream::connect(actual).await?);
            let id = timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("accepted connection not reported")?;
            assert_eq!(id.get(), expected_id, "a reject must not consume an ID");
        }
        assert_eq!(handle.registry().len(), 5);
        assert_eq!(handle.metrics().rejected_max_connections_total, 1);

        handle.shutdown();
        owner.await??;
        assert!(handle.registry().is_empty());
        assert_eq!(handle.metrics().active_connections, 0);
        assert_eq!(handle.metrics().connection_buffer_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn handler_panic_is_contained_and_raii_cleanup_runs() -> Result<(), Box<dyn Error>> {
        let snap = snapshot(1, 0, 0.0, one_listener())?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let calls = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run({
            let calls = Arc::clone(&calls);
            move |connection: AcceptedConnection| {
                let call = calls.fetch_add(1, Ordering::Relaxed);
                let tx = tx.clone();
                async move {
                    assert_ne!(call, 0, "injected handler panic");
                    let _ = tx.send(connection.metadata().connection_id);
                    std::future::pending::<()>().await;
                }
            }
        }));
        let mut first = TcpStream::connect(actual).await?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(TokioDuration::from_secs(2), first.read(&mut byte)).await??,
            0
        );
        let _second = TcpStream::connect(actual).await?;
        let second_id = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("second handler did not run")?;
        assert_eq!(second_id.get(), 2);
        assert_eq!(handle.registry().len(), 1);
        assert_eq!(handle.metrics().handler_panics_total, 1);
        handle.shutdown();
        owner.await??;
        assert!(handle.registry().is_empty());
        assert_eq!(handle.metrics().active_connections, 0);
        Ok(())
    }

    #[tokio::test]
    async fn expanded_port_range_binds_all_and_releases_together() -> Result<(), Box<dyn Error>> {
        let mut bound = None;
        for _ in 0..128 {
            let (start, end) = reserve_contiguous_ports(3).await?;
            let listeners: Vec<Listener> = (start..=end)
                .enumerate()
                .map(|(index, port)| Listener {
                    address: "127.0.0.1".to_owned(),
                    port: u32::from(port),
                    name: format!("sql-{index}"),
                })
                .collect();
            let snap = snapshot(1, 0, 0.0, listeners)?;
            if let Ok(server) =
                DataplaneServer::bind(snap, Arc::new(MutableMemory::new(1, 100))).await
            {
                bound = Some((server, start, end));
                break;
            }
        }
        let (server, start, end) = bound.ok_or("could not bind a contiguous port range")?;
        let handle = server.handle();
        let actual: Vec<u16> = handle
            .listeners()
            .iter()
            .map(|listener| listener.actual_address.port())
            .collect();
        assert_eq!(actual, (start..=end).collect::<Vec<_>>());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(connection.metadata().clone());
                std::future::pending::<()>().await;
            }
        }));

        // Each expanded port accepts on its own owner: the handler for the
        // first connection never returns, and the later listeners still
        // serve their own clients under their own name/address.
        let expected: Vec<(String, SocketAddr)> = handle
            .listeners()
            .iter()
            .map(|listener| (listener.name.to_string(), listener.actual_address))
            .collect();
        let mut clients = Vec::new();
        let mut accepted = Vec::new();
        for (_, address) in &expected {
            clients.push(TcpStream::connect(*address).await?);
            let metadata = timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("listener did not accept")?;
            accepted.push((
                metadata.listener_name.to_string(),
                metadata.listener_address,
            ));
        }
        assert_eq!(accepted, expected);

        handle.shutdown();
        owner.await??;
        for address in handle
            .listeners()
            .iter()
            .map(|listener| listener.actual_address)
        {
            assert!(TcpStream::connect(address).await.is_err());
        }
        Ok(())
    }

    /// CFG-001: a reloaded buffer size reaches the next admission and leaves
    /// the established session's reservation alone.
    ///
    /// The gauge is the observable: each connection reserves
    /// `connection_buffer_bytes * 2`, so a session admitted before the reload
    /// and one admitted after must contribute *different* amounts. Asserting
    /// the new snapshot's contents would prove nothing about admission.
    #[tokio::test]
    async fn reloaded_buffer_size_applies_to_the_next_admission_only() -> Result<(), Box<dyn Error>>
    {
        let snap = snapshot_with(1, one_listener(), |config| {
            config.connection_buffer_bytes = 4096;
        })?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                // Report both the reservation and the reader-buffer size derived
                // from the same admission snapshot; they must stay in lockstep
                // (reservation = reader size * 2).
                let reader = reader_stream_buffer_size(connection.snapshot()).get();
                let _ = tx.send((reader, connection.metadata().reserved_buffer_bytes));
                std::future::pending::<()>().await;
            }
        }));

        let _first = TcpStream::connect(actual).await?;
        let (first_reader, first) = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("first handler did not report")?;
        assert_eq!(first, 8192, "4 KiB buffer reserves double");
        assert_eq!(
            first_reader, 4096,
            "the two reader buffers use the admitted generation's size"
        );
        assert_eq!(
            first_reader as u64 * 2,
            first,
            "reader size and reservation agree"
        );
        assert_eq!(handle.metrics().connection_buffer_bytes, 8192);

        handle.update_snapshot(snapshot_with(2, one_listener(), |config| {
            config.connection_buffer_bytes = 16384;
        })?)?;

        let _second = TcpStream::connect(actual).await?;
        let (second_reader, second) = timeout(TokioDuration::from_secs(2), rx.recv())
            .await?
            .ok_or("second handler did not report")?;
        assert_eq!(second, 32768, "the reload reached the new admission");
        assert_eq!(
            second_reader, 16384,
            "the next session's readers use the reloaded size"
        );
        assert_eq!(
            second_reader as u64 * 2,
            second,
            "reader size and reservation agree after reload"
        );
        assert_eq!(
            handle.metrics().connection_buffer_bytes,
            8192 + 32768,
            "the established session keeps its own reservation"
        );

        handle.shutdown();
        owner.await??;
        assert_eq!(handle.metrics().connection_buffer_bytes, 0);
        Ok(())
    }

    /// CFG-001: a reloaded frontend keepalive is applied to the next accepted
    /// socket, read back from the socket rather than from the snapshot.
    ///
    /// Enablement is the portable observable — idle/probe/interval readback is
    /// platform-dependent, so flipping `enabled` is what every Unix runner can
    /// discriminate.
    #[tokio::test]
    #[cfg(unix)]
    async fn reloaded_frontend_keepalive_reaches_the_next_socket() -> Result<(), Box<dyn Error>> {
        let snap = snapshot_with(1, one_listener(), |config| {
            if let Some(keepalive) = config.frontend_keepalive.as_mut() {
                keepalive.enabled = true;
            }
        })?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let enabled = proxy_io::socket::read_keepalive(connection.stream())
                    .map(|state| state.enabled)
                    .unwrap_or(false);
                let _ = tx.send(enabled);
                std::future::pending::<()>().await;
            }
        }));

        let _first = TcpStream::connect(actual).await?;
        assert!(
            timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("first handler did not report")?,
            "the first socket has keepalive on"
        );

        handle.update_snapshot(snapshot_with(2, one_listener(), |config| {
            if let Some(keepalive) = config.frontend_keepalive.as_mut() {
                keepalive.enabled = false;
            }
        })?)?;

        let _second = TcpStream::connect(actual).await?;
        assert!(
            !timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("second handler did not report")?,
            "the reload reached the next accepted socket"
        );

        handle.shutdown();
        owner.await??;
        Ok(())
    }

    /// CFG-001: public-endpoint classification is taken from the admitting
    /// snapshot, so a CIDR reload changes the next connection's class while the
    /// established session keeps the class it was admitted under.
    ///
    /// This is the accept-time freeze in ADM-004's note, driven rather than
    /// asserted: the handler classifies through the same
    /// `metering::is_public_endpoint` call the session path uses, against the
    /// connection's own seat.
    #[tokio::test]
    async fn reloaded_public_cidrs_change_only_the_next_connections_class()
    -> Result<(), Box<dyn Error>> {
        let snap = snapshot_with(1, one_listener(), |config| {
            config.public_cidrs = Vec::new();
        })?;
        let server = ephemeral_server(snap, Arc::new(MutableMemory::new(1, 100))).await?;
        let handle = server.handle();
        let actual = handle.listeners()[0].actual_address;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let owner = tokio::spawn(server.run(move |connection: AcceptedConnection| {
            let tx = tx.clone();
            async move {
                let public = crate::metering::is_public_endpoint(
                    connection.metadata().peer_address.ip(),
                    connection
                        .snapshot()
                        .raw()
                        .config
                        .as_ref()
                        .map_or(&[][..], |config| config.public_cidrs.as_slice()),
                );
                let _ = tx.send(public);
                std::future::pending::<()>().await;
            }
        }));

        let _first = TcpStream::connect(actual).await?;
        assert!(
            !timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("first handler did not report")?,
            "loopback is private with no CIDRs configured"
        );

        // Naming loopback explicitly is the only way to flip a test peer's
        // class: it is private, so the ADM-004 non-private fallback cannot.
        handle.update_snapshot(snapshot_with(2, one_listener(), |config| {
            config.public_cidrs = vec!["127.0.0.0/8".to_owned()];
        })?)?;

        let _second = TcpStream::connect(actual).await?;
        assert!(
            timeout(TokioDuration::from_secs(2), rx.recv())
                .await?
                .ok_or("second handler did not report")?,
            "the reload reached the next admission's seat"
        );

        handle.shutdown();
        owner.await??;
        Ok(())
    }

    #[test]
    fn capture_preflight_and_accept_backoff_are_explicit() {
        let raw = StateSnapshot {
            config: Some(ConfigSnapshot {
                traffic_replay_enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            preflight_snapshot(&raw).is_ok(),
            "the Go default enable-traffic-replay=true must still serve"
        );
        assert!(matches!(
            preflight_snapshot(&StateSnapshot::default()),
            Err(ServerError::MissingConfig)
        ));
        let mut backoff = AcceptBackoff::new();
        assert_eq!(backoff.fail(), ACCEPT_BACKOFF_INITIAL);
        for _ in 0..16 {
            let _ = backoff.fail();
        }
        assert_eq!(backoff.fail(), ACCEPT_BACKOFF_MAX);
        backoff.reset();
        assert_eq!(backoff.fail(), ACCEPT_BACKOFF_INITIAL);
    }

    async fn reserve_contiguous_ports(size: u16) -> Result<(u16, u16), Box<dyn Error>> {
        for _ in 0..128 {
            let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let start = probe.local_addr()?.port();
            drop(probe);
            let Some(end) = start.checked_add(size.saturating_sub(1)) else {
                continue;
            };
            let mut reservations = Vec::new();
            let mut complete = true;
            for port in start..=end {
                if let Ok(listener) = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
                    reservations.push(listener);
                } else {
                    complete = false;
                    break;
                }
            }
            drop(reservations);
            if complete {
                return Ok((start, end));
            }
        }
        Err("no free contiguous port range found".into())
    }
}
