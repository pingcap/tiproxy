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

//! The production session owner (DPL-04): composes the DPL-01
//! [`SessionLoop`] FSM with real `MySQL` wire I/O, control-plane routing,
//! and the CTL-06 terminal-effect notices.
//!
//! # Ownership
//!
//! One **engine task** owns every socket byte: both halves of the
//! client stream and, once dialed, of the backend. Bytes never cross
//! tasks. The [`SessionLoop`] owns the FSM in its own task; classified
//! [`SessionEvent`]s flow engine → loop over a one-slot channel into
//! the DPL-01 pump (the engine reads at most one frame beyond the last
//! event it handed over — exactly the pump's own classify-one-ahead
//! contract), and [`SessionEffect`]s flow loop → engine over a bounded
//! FIFO. Phase progression is command-gated: after the
//! `ClientHandshakeResponse` event the engine performs no backend work
//! until the FSM's `DialBackend` effect arrives.
//!
//! # Forward-then-observe
//!
//! Response bytes stream backend → client with a bounded 23-byte
//! prefix capture; classification runs on the captured metadata after
//! the packet is on the wire, exactly like Go (which parses while
//! forwarding). The FSM's `Forward*` effects therefore execute as
//! ordering acknowledgements: the effect confirms the FSM authorized
//! what the wire already carried. An effect the engine cannot honor in
//! its phase ends the session as a proxy-internal error — never a
//! silent divergence.
//!
//! # Authentication
//!
//! The multi-round backend authentication relay (auth switch, extra
//! data) is engine-internal via [`AuthRelay`]; the FSM observes only
//! the terminal `BackendAuthOk`/`BackendAuthFailed`, per the SES-00
//! vocabulary.
//!
//! # Slice scope (recorded for review)
//!
//! This slice serves the TLS-capable, uncompressed path. The greeting
//! advertises `SSL` iff this session's snapshot carries a frontend TLS server
//! config; when the client sends a strict `SSLRequest`, TLS is activated in
//! place on the client leg (and, per the backend TLS plan, on the backend leg
//! before any credential leaves) with the `MySQL` sequence continuing across
//! the upgrade. An `SSLRequest` against a greeting that withheld `SSL`, or a
//! malformed one, fails closed with no plaintext fallback. Compression is
//! advertised (`COMPRESS` + `ZSTD`) and, when a client negotiates it, activated
//! at the auth-OK boundary on each leg independently (WIRE-activation C); the
//! compressed sequence is slaved to the packet sequence and reset once per
//! command. `COM_CHANGE_USER` runs its backend-challenged auth relay on the
//! same single socket owner and commits identity/prepared state only after the
//! final OK. `COM_STMT_PREPARE` streams its special multi-packet response and
//! registers statement metadata only after the terminal success boundary. A
//! control redirect executes the bounded `SHOW SESSION_STATES` exchange at
//! the FSM safe boundary, validates the signed token/session JSON, dials and
//! authenticates the exact gate-admitted target with `tidb_session_token`,
//! restores the escaped state, and only then atomically replaces the backend
//! owner. Candidate-side failures drop the candidate while preserving the
//! aligned old backend; an old-backend disconnect or incomplete snapshot
//! response closes the poisoned session.

use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use control_proto::control_transport::ControlClient;
use control_proto::snapshot::ValidatedSnapshot;
use control_proto::v1::control_envelope::Body;
use control_proto::v1::{
    ConnectionIdentity, ControlEnvelope, ErrorCode, ErrorSource as WireErrorSource,
    HandshakeMetadata, HandshakeResponseEvent, Priority, ProxyProtocolMode, RouteRequest,
};
use control_router::{
    MigrationCommand, RouteCommandEnvelope, RouteCommandReceiver, RouteError, RoutePlaneHandle,
};
use control_routing::{RouteAssignment, RouteResult};
use mysql_wire::limits::MAX_PHYSICAL_PAYLOAD_LEN;
use mysql_wire::{
    Attribute, CapabilityFlags, CommandCode, CommandPacket, HandshakeResponseParams,
    PHYSICAL_PACKET_HEADER_LEN, StatusFlags, encode_error_packet, encode_handshake_response,
    encode_initial_handshake, encode_ssl_request, parse_handshake_response, parse_ssl_request,
};
use proxy_io::compression::{CompressedIo, CompressionAlgorithm, CompressionLimits};
use proxy_io::counted::{ByteCounters, CountedIo};
use proxy_io::direction::DirectionSync;
use proxy_io::proxy_protocol::{
    EncodeAddresses, ProxyCommand, ProxyVersion, TransportProtocol, encode_proxy_v2,
};
use proxy_io::tls::{
    DEFAULT_CONN_BUFFER_SIZE, accept_frontend, build_backend_config, connect_backend,
};
use proxy_io::{InboundProxyV2Header, IoSide, PacketIo, PacketIoError, RunDecision};
use session_core::auth::{
    AuthEffect, AuthEvent, AuthOutcome, AuthRelay, AuthTurn, BackendTlsMode, CompressionSelection,
    UNKNOWN_AUTH_PLUGIN, classify_backend_auth_packet, compression_selection,
    plan_backend_handshake, plan_backend_migration_handshake,
};
use session_core::boundary::{HeldBegin, HoldEffect, need_hold_request};
use session_core::command::{
    Command, CommandSessionState, CommandStateEffects, ExpectedResponse, SessionMutation, dispatch,
};
use session_core::error_source::{
    ClientResponse, DisconnectState, FailureDescriptor, FailureKind, SideMarker, client_response,
    is_disconnect_io,
};
use session_core::fsm::{SessionEffect, SessionEvent};
use session_core::handshake::{
    BackendVerificationError, ConnectionEndpoints, SUPPORTED_SERVER_CAPABILITIES, build_greeting,
    check_handshake_packet_size, greeting_capability, negotiate_frontend, verify_backend,
};
use session_core::internal_client::{
    InternalLimits, InternalParserState, InternalProgress, InternalQuery, InternalResult,
    SessionStateSnapshot,
};
use session_core::prepared::{PrepareDisposition, PrepareObserver, PreparedRegistry};
use session_core::response::{
    DEFAULT_RESPONSE_FLUSH_THRESHOLD, FlushAction, ResponseDisposition, ResponseEffect,
    ResponseObserver, ResponsePacket,
};
use session_core::special::{
    ChangeUserEffect, ChangeUserEvent, ChangeUserPlan, ChangeUserRelay, ChangeUserTurn,
    SessionIdentity, change_user_ok_in_transaction, plan_change_user,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::LocalRouteChannel;
use crate::control_dispatch::{CommandKind, CommandToken, RedirectTarget, ResponseKind};
use crate::metering::{
    MeteringAttribution, MeteringSamplerError, MeteringSourceRegistry, is_public_endpoint,
};
use crate::observability::{
    BackendTraffic, MetricsRecorder, Observation, QuitSource, SessionLogContext, log_session,
};
use crate::route::{
    AcquireError, CenteredJitter, DialSchedule, RouteChannel, RouteChannelError, RouteEngine,
};
use crate::route_control::{
    ClusterTcpDialer, TrafficTotals, route_assignment_from_wire, route_result_to_wire,
};
use crate::route_local::{LOCAL_ROUTE_TRANSIENT_WAIT, release_local_route_lease};
use crate::server::{AcceptedConnection, ConnectionFuture, SessionSeat};
use crate::session::{
    EffectHandler, SessionControl, SessionEnd, SessionEventSource, SessionLoop, SessionLoopConfig,
    SessionSummary,
};
use crate::session_control::{
    BoundSessionHandler, ResponseStream, SessionCommander, SessionControlBinding,
};
use crate::transport::{BackendTransport, ClientTransport};

/// The proxy's full advertised capability set, including `SSL`. `SSL` is
/// retained or stripped per session by [`proxy_capabilities`] according to
/// whether that session's snapshot carries a frontend TLS server config.
/// `COMPRESS` and `ZSTD_COMPRESSION_ALGORITHM` are advertised (WIRE-activation
/// C): a client that negotiates either activates compressed framing at the
/// auth-OK boundary.
fn proxy_capability_base() -> CapabilityFlags {
    SUPPORTED_SERVER_CAPABILITIES
}

/// The proxy capabilities for one session: the full base with `SSL`
/// advertised only when this session's snapshot has a frontend TLS server
/// config, so the advertised capability always matches the live capability.
fn proxy_capabilities(frontend_tls_available: bool) -> CapabilityFlags {
    greeting_capability(proxy_capability_base(), frontend_tls_available)
}

/// The client's leading capability flags — the first four little-endian bytes
/// that begin both an `SSLRequest` and a full handshake response. Used only to
/// classify the first client packet; a payload shorter than four bytes carries
/// no `SSL` bit and falls through to the (fail-closed) handshake parser.
fn leading_capabilities(payload: &[u8]) -> CapabilityFlags {
    if payload.len() < 4 {
        return CapabilityFlags::from_bits_retain(0);
    }
    let bits = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    CapabilityFlags::from_bits_retain(bits)
}

/// Overwrites the leading capability flags of a handshake-response payload with
/// the trusted pre-TLS `SSLRequest` mask (Go `handshakeFirstTime`: the mask sent
/// before the encrypted response is authoritative, so it — not the in-TLS
/// response's own bytes — governs the response's field layout and negotiation).
/// A payload shorter than four bytes is left unchanged; it fails the subsequent
/// parse fail-closed.
fn normalize_leading_capabilities(payload: &mut [u8], trusted: CapabilityFlags) {
    if payload.len() >= 4 {
        payload[0..4].copy_from_slice(&trusted.bits().to_le_bytes());
    }
}

/// Resolves the lifecycle-log client carried inside PROXY v2. The direct TCP
/// peer remains the fallback for direct connections and headers without an
/// inet source (LOCAL/UNSPEC/Unix), matching Go `RemoteAddr()`.
fn proxy_client_log_address(
    peer: std::net::SocketAddr,
    source: Option<std::net::SocketAddr>,
) -> String {
    source.unwrap_or(peer).to_string()
}

/// Publishes the one decoded inet source to both lifecycle-log owners. The
/// source cannot change after the one-shot probe, so an immutable cell avoids
/// a lock and keeps force-close attribution available after Engine abort.
fn record_proxy_client_log_source(
    shared: &OnceLock<std::net::SocketAddr>,
    context: &mut SessionLogContext,
    peer: std::net::SocketAddr,
    source: Option<std::net::SocketAddr>,
) {
    if let Some(source) = source {
        let _ = shared.set(source);
    }
    context.proxy_client_address = proxy_client_log_address(peer, source);
}

/// The SNI/server name for a backend TLS handshake: the host of a `host:port`
/// routing address, with any IPv6 brackets stripped (Go: "use the DNS name as
/// much as possible"; both DNS names and IP literals parse as a server name).
fn backend_server_name(address: &str) -> String {
    let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned()
}

/// Writes a PROXY protocol v2 header announcing the original client as the
/// source and the dialed backend as the destination, straight to the raw
/// backend socket before any `MySQL` byte. The family is unified across a
/// mixed client/backend IP pair exactly like Go's `unifyIPFamily`.
///
/// Any failure (unresolvable backend peer, encode, or write) fails closed as a
/// backend-network error — the preamble must reach the backend intact before
/// the handshake, so a partial or absent header is never tolerated.
async fn write_backend_proxy_v2_header(
    conn: &mut CountedIo<tokio::net::TcpStream>,
    inbound: Option<&InboundProxyV2Header>,
    client_addr: std::net::SocketAddr,
) -> Result<(), WireErrorSource> {
    // The header is written THROUGH the raw byte counter (this `CountedIo`
    // wraps the socket before the header is emitted), so its wire bytes count
    // once at the innermost layer just like Go's dial path.
    let header = if let Some(inbound) = inbound {
        inbound.proxy_command_wire()
    } else {
        let Ok(backend_addr) = conn.get_ref().peer_addr() else {
            return Err(WireErrorSource::BackendNetwork);
        };
        let Ok(header) = encode_proxy_v2(
            ProxyVersion::V2,
            ProxyCommand::PROXY,
            TransportProtocol::STREAM,
            EncodeAddresses::Ip {
                src: (client_addr.ip(), client_addr.port()),
                dst: (backend_addr.ip(), backend_addr.port()),
            },
            &[],
        ) else {
            return Err(WireErrorSource::Proxy);
        };
        header
    };
    if conn.write_all(&header).await.is_err() {
        return Err(WireErrorSource::BackendNetwork);
    }
    Ok(())
}

/// Maps a per-leg [`CompressionSelection`] to the codec algorithm, or `None`
/// when the leg negotiated no compression.
fn selection_to_compression_algorithm(
    selection: CompressionSelection,
) -> Option<CompressionAlgorithm> {
    match selection {
        CompressionSelection::None => None,
        CompressionSelection::Zlib => Some(CompressionAlgorithm::Zlib),
        CompressionSelection::Zstd { level } => Some(CompressionAlgorithm::Zstd {
            level: i32::from(level),
        }),
    }
}

/// Failure of the fresh-exchange send for a proxy-owned query.
pub enum ProxyOwnedQueryError {
    /// The layered (compression) sequence could not be reset to a clean command
    /// boundary because a frame is still in flight.
    LayeredReset,
    /// The request could not be written to the backend.
    Send,
}

/// Starts a fresh proxy-owned command exchange on a backend [`PacketIo`]:
/// resets the compression layer to sequence zero (Go `cmd_processor_query`
/// parity), resets the packet write/read sequences, then sends `request`.
///
/// This is the single production seam for a proxy-owned query's reset+send, so
/// the migration snapshot and its regression exercise the same code. Deleting
/// the layered reset makes the sent frame carry the previous exchange's stale
/// compressed sequence, which the regression catches.
///
/// # Errors
///
/// [`ProxyOwnedQueryError::LayeredReset`] if the compressed layer is not at a
/// clean boundary; [`ProxyOwnedQueryError::Send`] if the write fails.
pub async fn send_proxy_owned_query<T>(
    io: &mut PacketIo<T>,
    request: &[u8],
) -> Result<(), ProxyOwnedQueryError>
where
    T: tokio::io::AsyncWrite + Unpin + DirectionSync,
{
    io.reset_layer_sequence()
        .map_err(|_| ProxyOwnedQueryError::LayeredReset)?;
    io.reset_write_sequence(0);
    io.reset_read_sequence(1);
    io.write_logical(request, true)
        .await
        .map_err(|_| ProxyOwnedQueryError::Send)?;
    Ok(())
}

/// Handshake-phase logical payload bound.
const HANDSHAKE_PAYLOAD_LIMIT: usize = 64 * 1024;
/// Client command / infile chunk payload bound for this slice.
const COMMAND_PAYLOAD_LIMIT: usize = 64 * 1024 * 1024;
/// PKT-003: bytes retained from a streamed command. Go keeps the same 1024
/// (`forwardCommand` -> `ForwardPacketTo(backendIO, 1024)`) with the note
/// "generally, the stmtID is enough" — the prefix exists to recover command
/// state, never to hold the request.
const STREAMED_COMMAND_CAPTURE: usize = 1024;
/// Streaming prefix capture for response classification.
const RESPONSE_CAPTURE: usize = 23;
/// Engine effect-command queue depth (FSM effects per event are few).
const ENGINE_CMD_CAPACITY: usize = 16;
/// opt#5: yield to the scheduler once per this many consecutively bypassed
/// response packets, so the FSM/SessionLoop producer task (the sender into
/// `cmds`), not this Engine consumer task, gets a scheduling opportunity to
/// enqueue a pending close/redirect on a continuously readable response.
const BYPASS_YIELD_INTERVAL: u32 = 64;
/// Engine → owner report queue depth.
const ENGINE_REPORT_CAPACITY: usize = 8;
const BACKEND_HEALTH_RECHECK_INTERVAL: Duration = Duration::from_secs(5);
/// Server-version bytes advertised in the proxy greeting.
const SERVER_VERSION: &[u8] = b"8.0.11-TiProxy-rs";

/// Bounds one candidate attempt by both the issuer's absolute deadline and
/// the dataplane acquisition budget. Zero is the legacy/control default and
/// still receives a finite bound.
fn candidate_budget(deadline_unix_millis: u64) -> Duration {
    let local = DialSchedule::default().total;
    if deadline_unix_millis == 0 {
        return local;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let deadline = Duration::from_millis(deadline_unix_millis);
    deadline.saturating_sub(now).min(local)
}

/// Commands into the engine task.
enum EngineCmd {
    /// Execute one FSM effect in order.
    Effect(SessionEffect),
    /// Idle-safe backend liveness probe (KA-003).
    Probe(oneshot::Sender<bool>),
    /// Bind the exact gate-admitted redirect target before the FSM can emit
    /// `StartRedirectHandshake`. This command shares the effect FIFO, so the
    /// target cannot race or be inferred from mutable control state.
    PrepareRedirect(RedirectTarget),
}

/// Reports from the engine to the session owner.
#[derive(Debug)]
enum EngineReport {
    /// The bridge-independent command receiver registered before the local
    /// selector can reserve its first backend.
    LocalRouteCommands(RouteCommandReceiver),
    /// A redirect attempt finished.
    RedirectFinished {
        /// Whether the migration succeeded.
        succeeded: bool,
        /// The owning backend after the attempt.
        backend_id: String,
        /// Failure code when unsuccessful.
        code: ErrorCode,
    },
}

/// The engine's terminal accounting, returned from its task.
#[derive(Debug, Clone)]
struct EngineExit {
    totals: TrafficTotals,
    source: WireErrorSource,
    quit_source: QuitSource,
    backend_id: String,
    backend_address: String,
    cluster: String,
    capabilities: u64,
    /// The namespace the accepted decision resolved (the process seed
    /// until a decision arrives), so the owner's close log attributes
    /// the connection to its real routing class.
    namespace: String,
}

/// The event-source half: the loop's pump polls this; the engine feeds
/// it through a one-slot channel.
struct EventRx {
    events: crate::gate::GatedReceiver<SessionEvent>,
}

impl SessionEventSource for EventRx {
    async fn next_event(&mut self) -> Option<SessionEvent> {
        // Only reached if the gated hand-off is bypassed (it is not for the
        // engine path); drop the permit since the pump path is ungated.
        self.events.recv().await.map(|(event, _permit)| event)
    }

    fn into_gated_event_channel(self) -> Result<crate::gate::GatedReceiver<SessionEvent>, Self> {
        Ok(self.events)
    }
}

/// The effect-handler half: forwards each FSM effect into the engine's
/// FIFO and runs the idle-safe probe through the engine (the socket
/// owner).
struct CmdTx {
    cmds: crate::gate::GatedSender<EngineCmd>,
}

impl EffectHandler for CmdTx {
    async fn execute(&mut self, effect: SessionEffect, _children: &mut JoinSet<()>) {
        // A closed engine means the wire is gone; the loop then
        // observes the exhausted event source.
        let _ = self.cmds.send(EngineCmd::Effect(effect)).await;
    }

    async fn backend_active(&mut self) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.cmds.send(EngineCmd::Probe(tx)).await.is_err() {
            return false;
        }
        rx.await.unwrap_or(false)
    }
}

/// Route channel over the split #37 binding: every expectation is
/// armed — and acknowledged by the dispatcher — **before** the request
/// envelope that provokes the answer is sent.
struct BindingRouteChannel {
    client: Arc<ControlClient>,
    commander: SessionCommander,
    responses: ResponseStream,
    identity: ConnectionIdentity,
    metadata: HandshakeMetadata,
    namespace: String,
    generation: u64,
}

impl BindingRouteChannel {
    async fn send_durable(&self, body: Body) -> Result<u64, RouteChannelError> {
        let Some(request_id) = self.client.allocate_request_id() else {
            return Err(RouteChannelError::ControlLost);
        };
        let envelope = ControlEnvelope {
            request_id,
            priority: Priority::Control.into(),
            body: Some(body),
            ..ControlEnvelope::default()
        };
        self.client
            .send(envelope)
            .await
            .map_err(|_| RouteChannelError::ControlLost)?;
        Ok(request_id)
    }
}

impl RouteChannel for BindingRouteChannel {
    async fn request_route(
        &mut self,
        excluded_backend_ids: Vec<String>,
    ) -> Result<(), RouteChannelError> {
        let Some(request_id) = self.client.allocate_request_id() else {
            return Err(RouteChannelError::ControlLost);
        };
        // Causal barrier: the dispatcher acknowledges the armed
        // expectation before the request that provokes the answer.
        self.commander
            .expect_response(request_id, ResponseKind::RouteAssignment)
            .await
            .map_err(|_| RouteChannelError::ControlLost)?;
        let envelope = ControlEnvelope {
            request_id,
            generation: self.generation,
            priority: Priority::Control.into(),
            body: Some(Body::RouteRequest(RouteRequest {
                connection: Some(self.identity.clone()),
                handshake: Some(self.metadata.clone()),
                namespace_hint: self.namespace.clone(),
                excluded_backend_ids,
            })),
            ..ControlEnvelope::default()
        };
        self.client
            .send(envelope)
            .await
            .map_err(|_| RouteChannelError::ControlLost)
    }

    async fn next_assignment(&mut self) -> Result<RouteAssignment, RouteChannelError> {
        loop {
            let Some(envelope) = self.responses.recv().await else {
                return Err(RouteChannelError::ControlLost);
            };
            if let Some(Body::RouteAssignment(assignment)) = envelope.body {
                return Ok(route_assignment_from_wire(assignment));
            }
            // A correlated non-assignment here is a dispatcher routing
            // bug; skip defensively rather than act on it.
        }
    }

    async fn report_result(&mut self, result: RouteResult) -> Result<(), RouteChannelError> {
        self.send_durable(Body::RouteResult(route_result_to_wire(result)))
            .await
            .map(|_| ())
    }
}

/// Transitional channel shape while focused tests still exercise the legacy
/// bridge adapter. The production binary always installs `Local` after T2.
enum SessionRouteChannel {
    Local(LocalRouteChannel),
    Bridge(BindingRouteChannel),
}

const fn local_admission_client_error(error: RouteError) -> Option<(u16, [u8; 5], &'static str)> {
    match error {
        RouteError::NamespaceMissing => Some((1105, *b"HY000", "failed to find a namespace")),
        RouteError::InvalidConfig => Some((1105, *b"HY000", "invalid namespace configuration")),
        _ => None,
    }
}

impl RouteChannel for SessionRouteChannel {
    async fn request_route(
        &mut self,
        excluded_backend_ids: Vec<String>,
    ) -> Result<(), RouteChannelError> {
        match self {
            Self::Local(channel) => channel.request_route(excluded_backend_ids).await,
            Self::Bridge(channel) => channel.request_route(excluded_backend_ids).await,
        }
    }

    async fn next_assignment(&mut self) -> Result<RouteAssignment, RouteChannelError> {
        match self {
            Self::Local(channel) => channel.next_assignment().await,
            Self::Bridge(channel) => channel.next_assignment().await,
        }
    }

    async fn report_result(&mut self, result: RouteResult) -> Result<(), RouteChannelError> {
        match self {
            Self::Local(channel) => channel.report_result(result).await,
            Self::Bridge(channel) => channel.report_result(result).await,
        }
    }
}

/// The production [`BoundSessionHandler`]: composes the engine for each
/// registered connection.
pub struct EngineSessionOwner {
    client: Arc<ControlClient>,
    namespace: Arc<str>,
    shutdown: watch::Receiver<bool>,
    drain: watch::Receiver<Option<Duration>>,
    loop_config: SessionLoopConfig,
    metrics: MetricsRecorder,
    metering: Option<MeteringSourceRegistry>,
    route_plane: Option<RoutePlaneHandle>,
}

impl EngineSessionOwner {
    /// Builds the owner for the given control client and namespace.
    #[must_use]
    pub fn new(
        client: Arc<ControlClient>,
        namespace: impl Into<Arc<str>>,
        shutdown: watch::Receiver<bool>,
        drain: watch::Receiver<Option<Duration>>,
        loop_config: SessionLoopConfig,
    ) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            shutdown,
            drain,
            loop_config,
            metrics: MetricsRecorder::default(),
            metering: None,
            route_plane: None,
        }
    }

    /// Attaches the process-wide non-blocking metrics recorder.
    #[must_use]
    pub fn with_metrics(mut self, metrics: MetricsRecorder) -> Self {
        self.metrics = metrics;
        self
    }

    /// Attaches the process-wide immutable-source metering registry.
    #[must_use]
    pub fn with_metering(mut self, metering: MeteringSourceRegistry) -> Self {
        self.metering = Some(metering);
        self
    }

    /// Uses the process-local Rust route plane for handshake resolution and
    /// initial backend acquisition. Without this handle, the legacy bridge
    /// path remains available to focused compatibility tests only.
    #[must_use]
    pub fn with_route_plane(mut self, route_plane: RoutePlaneHandle) -> Self {
        self.route_plane = Some(route_plane);
        self
    }
}

impl BoundSessionHandler for EngineSessionOwner {
    fn handle(
        &self,
        connection: AcceptedConnection,
        binding: SessionControlBinding,
    ) -> ConnectionFuture {
        let client = Arc::clone(&self.client);
        let namespace = self.namespace.to_string();
        let shutdown = self.shutdown.clone();
        let drain = self.drain.clone();
        let config = self.loop_config;
        let metrics = self.metrics.clone();
        let metering = self.metering.clone();
        let route_plane = self.route_plane.clone();
        Box::pin(async move {
            run_bound_session_observed(
                connection,
                binding,
                client,
                namespace,
                shutdown,
                drain,
                config,
                metrics,
                metering,
                route_plane,
            )
            .await;
        })
    }
}

/// Runs one admitted, registered session to completion: FSM loop plus
/// wire engine plus terminal notices. Every task is joined before this
/// returns, and every gate-admitted command token resolves to exactly
/// one terminal under its exact id.
#[allow(clippy::too_many_lines)]
pub async fn run_bound_session(
    connection: AcceptedConnection,
    binding: SessionControlBinding,
    client: Arc<ControlClient>,
    namespace: String,
    shutdown: watch::Receiver<bool>,
    drain: watch::Receiver<Option<Duration>>,
    loop_config: SessionLoopConfig,
) {
    run_bound_session_observed(
        connection,
        binding,
        client,
        namespace,
        shutdown,
        drain,
        loop_config,
        MetricsRecorder::default(),
        None,
        None,
    )
    .await;
}

/// Whether the v5 steady-state fast path is enabled, read once from the
/// `TIPROXY_FSM_FASTPATH` environment variable. Default off: only the exact
/// values `1`/`true`/`on` (case-insensitive) enable it, so the gate plumbing is
/// behaviourally inert unless deliberately switched on for same-binary A/B.
fn fast_path_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("TIPROXY_FSM_FASTPATH").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on"
            )
        })
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_bound_session_observed(
    connection: AcceptedConnection,
    binding: SessionControlBinding,
    client: Arc<ControlClient>,
    namespace: String,
    shutdown: watch::Receiver<bool>,
    mut drain: watch::Receiver<Option<Duration>>,
    loop_config: SessionLoopConfig,
    metrics: MetricsRecorder,
    metering: Option<MeteringSourceRegistry>,
    route_plane: Option<RoutePlaneHandle>,
) {
    let accepted_at = tokio::time::Instant::now();
    let (stream, seat) = connection.into_session_io();
    let metadata = seat.metadata();
    let peer_address = metadata.peer_address;
    let identity = ConnectionIdentity {
        connection_id: metadata.connection_id.get(),
        listener_address: metadata.listener_address.to_string(),
        client_address: metadata.peer_address.to_string(),
        proxy_address: metadata.peer_address.to_string(),
        public_endpoint: is_public_endpoint(
            metadata.peer_address.ip(),
            seat.snapshot()
                .raw()
                .config
                .as_ref()
                .map(|config| config.public_cidrs.as_slice())
                .unwrap_or_default(),
        ),
    };
    let endpoints = ConnectionEndpoints {
        listener_addr: metadata.listener_address,
        client_addr: metadata.peer_address,
    };
    let log_context = SessionLogContext {
        connection_id: identity.connection_id,
        listener: metadata.listener_address.to_string(),
        client_address: metadata.peer_address.to_string(),
        proxy_client_address: metadata.peer_address.to_string(),
        namespace: namespace.clone(),
        generation: seat.snapshot().generation(),
    };
    // The inbound header is consumed only after the greeting, inside the
    // Engine socket owner. Share its immutable decoded inet source with the
    // outer owner so even a force-aborted engine keeps the correct close-log
    // attribution. Local CIDR routing also uses that logical source; residual
    // IPC identity and public/private metering remain peer-based.
    let proxy_client_source = Arc::new(OnceLock::new());
    let public_endpoint = identity.public_endpoint;

    let (mut directives, responses, commander) = binding.split();

    // The admission gate counts in-flight messages across the engine↔loop and
    // owner→loop channels. S1 wires the cmd channel through it (events/control
    // follow in later slices); the counter is maintained but not yet read.
    let gate = crate::gate::Gate::new();
    let (event_tx, event_rx) = crate::gate::channel::<SessionEvent>(gate.clone(), 1);
    let (cmd_tx, cmd_rx) = crate::gate::channel::<EngineCmd>(gate.clone(), ENGINE_CMD_CAPACITY);
    let (report_tx, mut report_rx) = mpsc::channel(ENGINE_REPORT_CAPACITY);
    let (control_tx, control_rx) = crate::gate::channel::<SessionControl>(gate.clone(), 8);
    let session_metering = metering.clone();

    // Wrap the raw client socket in the innermost byte counter before any
    // framing/TLS/compression layer; the handle survives in-place upgrades.
    let client_socket = CountedIo::new(stream);
    let client_counters = client_socket.counters();
    // `proxy.conn-buffer-size` is Dynamic: size the two per-connection reader
    // buffers (client and backend over-read window + forward scratch) from the
    // same committed generation this connection was admitted under, so they
    // always match the `conn-buffer-size * 2` admission reservation. A reload
    // takes effect on the next admitted session; existing sessions keep the
    // value captured in their immutable seat snapshot.
    let stream_buffer_size = crate::server::reader_stream_buffer_size(seat.snapshot());
    let snapshot_updates = seat.subscribe_snapshot_updates();
    let engine = Engine {
        connection_id: identity.connection_id,
        endpoints,
        inbound_proxy_header: None,
        proxy_client_source: Arc::clone(&proxy_client_source),
        client_io: PacketIo::with_read_stream_buffer_size(
            ClientTransport::Plain(client_socket),
            stream_buffer_size,
        ),
        client_counters,
        backend: None,
        candidate: None,
        redirect_target: None,
        retired_backend_in: 0,
        retired_backend_out: 0,
        backend_generation: 0,
        public_endpoint,
        snapshot_updates,
        metering,
        events: event_tx,
        cmds: cmd_rx,
        reports: report_tx,
        route: Some(RouteSeed {
            client: Arc::clone(&client),
            commander: commander.clone(),
            responses,
            identity: identity.clone(),
            namespace,
            route_plane,
        }),
        local_route_lease: None,
        salt: [0; 20],
        negotiated: CapabilityFlags::from_bits_retain(0),
        client_handshake_raw: Vec::new(),
        session_identity: None,
        relay_hold: None,
        cmd_state: None,
        in_transaction: false,
        prepared: PreparedRegistry::new(),
        held: None,
        hold_replay_ready: false,
        pending_command: None,
        wire_end: None,
        quit_source: QuitSource::None,
        closing: false,
        attributed_outbound: 0,
        wire_failed: false,
        accepted_at,
        handshake_deadline: loop_config.handshake_deadline,
        stream_buffer_size,
        frontend_tls_active: false,
        metrics: metrics.clone(),
        log_context: log_context.clone(),
        seat,
        core: gate.clone(),
        shutdown: shutdown.clone(),
        fast_path_enabled: fast_path_enabled(),
    };

    // The owner watches the same shutdown signal the loop consumes, so
    // it can arm the shared absolute force budget the moment the signal
    // fires — not only when the loop finishes its own cleanup.
    let mut owner_shutdown = shutdown.clone();
    let session_loop = SessionLoop::new(
        gate.clone(),
        EventRx { events: event_rx },
        CmdTx {
            cmds: cmd_tx.clone(),
        },
        control_rx,
        shutdown,
        loop_config,
    );
    let mut loop_task = AbortOnDrop(tokio::spawn(session_loop.run()));
    let mut engine_task = AbortOnDrop(tokio::spawn(Box::new(engine).run()));

    // The owner: forwards directives while holding the exact command
    // tokens, consumes engine reports, and waits for the loop.
    let mut redirect_token: Option<CommandToken> = None;
    let mut close_token: Option<CommandToken> = None;
    let mut local_commands: Option<RouteCommandReceiver> = None;
    let mut local_redirect: Option<RouteCommandEnvelope> = None;
    let mut local_close: Option<RouteCommandEnvelope> = None;
    let mut directives_open = true;
    let mut forced_by_control = false;
    // One absolute force budget: armed when the force signal is first
    // observed, it bounds the loop's own cleanup AND the engine join
    // below — never two stacked deadlines.
    let mut force_deadline: Option<tokio::time::Instant> = None;
    if *owner_shutdown.borrow() {
        force_deadline = Some(tokio::time::Instant::now() + loop_config.cleanup_deadline);
    }
    let initial_drain = *drain.borrow();
    let mut drain_signaled = initial_drain.is_some();
    if let Some(deadline) = initial_drain {
        // Admitted after stop-accept began: close at the first safe
        // boundary.
        let _ = control_tx
            .send(SessionControl::GracefulCloseAfter(deadline))
            .await;
    }
    let summary: Option<SessionSummary> = loop {
        tokio::select! {
            joined = &mut loop_task.0 => {
                break joined.ok();
            }
            changed = owner_shutdown.changed(), if force_deadline.is_none() => {
                if changed.is_err() || *owner_shutdown.borrow() {
                    force_deadline =
                        Some(tokio::time::Instant::now() + loop_config.cleanup_deadline);
                }
            }
            changed = drain.changed(), if !drain_signaled => {
                let deadline = if changed.is_ok() { *drain.borrow() } else { None };
                if let Some(deadline) = deadline {
                    drain_signaled = true;
                    // Local coordinated shutdown: graceful close at the
                    // next safe boundary using the latest accepted dynamic
                    // drain deadline. No command token — this is not a
                    // gate-admitted command.
                    let _ = control_tx
                        .send(SessionControl::GracefulCloseAfter(deadline))
                        .await;
                } else if changed.is_err() {
                    drain_signaled = true;
                }
            }
            directive = directives.recv(), if directives_open => {
                let Some(directive) = directive else {
                    // Control detach: last-good — the session continues;
                    // stop polling this arm.
                    directives_open = false;
                    continue;
                };
                match &directive.command {
                    Some(token) if token.kind == CommandKind::Redirect => {
                        redirect_token = Some(token.clone());
                    }
                    Some(token) => {
                        close_token = Some(token.clone());
                    }
                    None => {}
                }
                // Redirect is an atomic pair (PrepareRedirect + the control):
                // hold one operation permit across both sends so `pending`
                // cannot drop to zero between the engine consuming the first
                // message and the second being registered. Dropped at the end
                // of this arm (after the control send registered its permit) and
                // on every early exit.
                let _redirect_op = directive
                    .redirect_target
                    .as_ref()
                    .map(|_| gate.operation_permit());
                if let Some(target) = directive.redirect_target {
                    let _ = cmd_tx.send(EngineCmd::PrepareRedirect(target)).await;
                }
                if directive.control == SessionControl::CloseImmediate {
                    forced_by_control = true;
                    force_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + loop_config.cleanup_deadline
                    });
                }
                let _ = control_tx.send(directive.control).await;
            }
            local_command = recv_local_command(&mut local_commands), if local_commands.is_some() => {
                let Some(envelope) = local_command else {
                    local_commands = None;
                    continue;
                };
                match envelope.command() {
                    MigrationCommand::Redirect(redirect) => {
                        if local_close.is_some() {
                            let _ = envelope.finish_redirect(false);
                            continue;
                        }
                        if let Some(pending) = local_redirect.as_ref() {
                            if pending.same_operation(&envelope) {
                                envelope.ignore_duplicate();
                            } else {
                                let _ = envelope.finish_redirect(false);
                            }
                            continue;
                        }
                        let Some(budget) = envelope.redirect_budget() else {
                            let _ = envelope.finish_redirect(false);
                            continue;
                        };
                        if budget.is_zero() {
                            let _ = envelope.finish_redirect(false);
                            continue;
                        }
                        let assignment = redirect.to();
                        let deadline = u64::try_from(
                            SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .saturating_add(budget)
                            .as_millis(),
                        )
                        .unwrap_or(u64::MAX);
                        let target = RedirectTarget {
                            backend_id: assignment.backend_id.clone(),
                            backend_address: assignment.backend_address.clone(),
                            cluster_name: assignment.cluster_name.clone(),
                            keyspace: assignment.keyspace.clone(),
                            backend_healthy: assignment.healthy,
                            backend_local: assignment.local,
                            deadline_unix_millis: deadline,
                        };
                        local_redirect = Some(envelope);
                        // Atomic redirect pair: one operation permit spans both
                        // sends so the gate never transiently empties between
                        // them; it drops after the control send (which has
                        // registered its own permit) and on the failure path.
                        let _redirect_op = gate.operation_permit();
                        if (cmd_tx.send(EngineCmd::PrepareRedirect(target)).await.is_err()
                            || control_tx.send(SessionControl::Redirect).await.is_err())
                            && let Some(envelope) = local_redirect.take()
                        {
                            let _ = envelope.finish_redirect(false);
                        }
                    }
                    MigrationCommand::ForceClose(_) => {
                        if let Some(pending) = local_close.as_ref() {
                            if pending.same_operation(&envelope) {
                                envelope.ignore_duplicate();
                            } else {
                                // A second exact close cannot be minted by one
                                // ledger. Its guard remains the conservative
                                // terminal backstop for fault injection.
                                drop(envelope);
                            }
                            continue;
                        }
                        if let Some(redirect) = local_redirect.take() {
                            let _ = redirect.finish_redirect(false);
                        }
                        local_close = Some(envelope);
                        forced_by_control = true;
                        force_deadline.get_or_insert_with(|| {
                            tokio::time::Instant::now() + loop_config.cleanup_deadline
                        });
                        let _ = control_tx.send(SessionControl::CloseImmediate).await;
                    }
                }
            }
            report = report_rx.recv() => {
                if let Some(report) = report {
                    consume_report(
                        report,
                        &commander,
                        &mut redirect_token,
                        &mut local_commands,
                        &mut local_redirect,
                    ).await;
                }
            }
        }
    };

    // The session loop has observed the physical close. Settle its exact local
    // close token while the engine still owns the route lease; selector-drop
    // remains only an abort/backstop path.
    if let Some(envelope) = local_redirect.take() {
        let _ = envelope.finish_redirect(false);
    }
    if let Some(envelope) = local_close.take() {
        let _ = envelope.observe_close();
    }
    // The loop returned: its handler (holding one cmd sender clone) is
    // gone; drop ours so the engine drains and exits, then join it.
    drop(cmd_tx);
    // The engine may be blocked in a socket forward (a stalled backend
    // mid-command). A forced end shares the ONE absolute budget armed at
    // the force signal — whatever the loop's own cleanup already spent
    // is not re-granted here — then hard-cancels: the sockets drop with
    // the task, which IS the force close. A non-forced end (the client
    // quit) budgets its ordinary cleanup from now.
    let join_deadline = force_deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + loop_config.cleanup_deadline);
    let engine_exit =
        if let Ok(joined) = tokio::time::timeout_at(join_deadline, &mut engine_task.0).await {
            joined.ok()
        } else {
            engine_task.0.abort();
            let _ = (&mut engine_task.0).await;
            None
        };
    while let Ok(report) = report_rx.try_recv() {
        consume_report(
            report,
            &commander,
            &mut redirect_token,
            &mut local_commands,
            &mut local_redirect,
        )
        .await;
    }

    let totals = engine_exit.as_ref().map_or_else(
        || {
            session_metering
                .as_ref()
                .and_then(|registry| {
                    if let Ok(totals) = registry.finalize_connection(identity.connection_id) {
                        totals
                    } else {
                        registry.fail_closed();
                        None
                    }
                })
                .map_or_else(TrafficTotals::default, |(backend_in, backend_out)| {
                    TrafficTotals {
                        backend_in,
                        backend_out,
                        ..TrafficTotals::default()
                    }
                })
        },
        |exit| exit.totals,
    );
    let shutdown_end = summary
        .as_ref()
        .is_some_and(|summary| summary.end == SessionEnd::ServerShutdown);
    let forced = shutdown_end || forced_by_control;
    // Go parity: a timeout/immediate force-close reports the proxy
    // shutdown source; everything else keeps the wire classification.
    let source = if forced {
        WireErrorSource::Shutdown
    } else {
        engine_exit
            .as_ref()
            .map_or(WireErrorSource::Proxy, |exit| exit.source)
    };
    let quit_source = if forced {
        QuitSource::ProxyQuit
    } else {
        engine_exit
            .as_ref()
            .map_or(QuitSource::ProxyError, |exit| exit.quit_source)
    };
    // Exactness: an unresolved redirect terminal fails closed so the
    // gate id never dangles; an accepted close that ran the session to
    // its end reports under its exact admitted id.
    if let Some(token) = redirect_token.take() {
        let _ = commander
            .redirect_finished(
                token.id.to_string(),
                false,
                engine_exit
                    .as_ref()
                    .map(|exit| exit.backend_id.clone())
                    .unwrap_or_default(),
                ErrorCode::RedirectFailed,
            )
            .await;
    }
    if let Some(token) = close_token.take() {
        let _ = commander.close_finished(token.id.to_string()).await;
    }
    metrics.try_record(Observation::SessionClosed {
        source: quit_source,
        lifetime: accepted_at.elapsed(),
        traffic: totals,
    });
    let (backend_id, backend_address, cluster, capabilities) =
        engine_exit.as_ref().map_or(("", "", "", 0), |exit| {
            (
                exit.backend_id.as_str(),
                exit.backend_address.as_str(),
                exit.cluster.as_str(),
                exit.capabilities,
            )
        });
    // Attribute the close to the namespace the decision resolved, not
    // the pre-decision process seed.
    let mut log_context = log_context;
    if let Some(exit) = engine_exit.as_ref()
        && !exit.namespace.is_empty()
    {
        log_context.namespace.clone_from(&exit.namespace);
    }
    log_context.proxy_client_address =
        proxy_client_log_address(peer_address, proxy_client_source.get().copied());
    log_session(
        "connection_closed",
        &log_context,
        backend_id,
        backend_address,
        cluster,
        capabilities,
        quit_source,
    );
    let _ = commander.session_closed(forced, source, totals).await;
    if let Some(registry) = session_metering {
        registry.forget_connection(identity.connection_id);
    }
}

/// Aborts the owned task when dropped: an externally cancelled session
/// owner never detaches its loop or engine task — abort cancels them at
/// their next await point and their sockets drop with them.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn consume_report(
    report: EngineReport,
    commander: &SessionCommander,
    redirect_token: &mut Option<CommandToken>,
    local_commands: &mut Option<RouteCommandReceiver>,
    local_redirect: &mut Option<RouteCommandEnvelope>,
) {
    match report {
        EngineReport::LocalRouteCommands(receiver) => {
            // There is exactly one local route lease per session. Replacing a
            // live receiver would close and drain the previous FIFO, so fail
            // closed by retaining the first registration.
            if local_commands.is_none() {
                *local_commands = Some(receiver);
            }
        }
        EngineReport::RedirectFinished {
            succeeded,
            backend_id,
            code,
        } => {
            if let Some(token) = redirect_token.take() {
                let _ = commander
                    .redirect_finished(token.id.to_string(), succeeded, backend_id, code)
                    .await;
            }
            if let Some(envelope) = local_redirect.take() {
                let _ = envelope.finish_redirect(succeeded);
            }
        }
    }
}

async fn recv_local_command(
    receiver: &mut Option<RouteCommandReceiver>,
) -> Option<RouteCommandEnvelope> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// Route dependencies consumed at dial time.
struct RouteSeed {
    client: Arc<ControlClient>,
    commander: SessionCommander,
    responses: ResponseStream,
    identity: ConnectionIdentity,
    namespace: String,
    route_plane: Option<RoutePlaneHandle>,
}

/// The dialed backend's I/O and identity.
struct BackendIo {
    #[allow(clippy::struct_field_names)]
    backend_io: PacketIo<BackendTransport>,
    /// Raw-socket byte counters for THIS backend socket. A redirected backend
    /// gets a fresh `BackendIo` with its own counters, so a swap snapshots and
    /// closes out the old leg's totals rather than smearing them across sockets.
    counters: Arc<ByteCounters>,
    id: String,
    address: String,
    cluster: String,
    keyspace: String,
    local: bool,
    /// Latest health observed for this exact backend identity.
    healthy: bool,
    /// Whether the policy for `healthy` reached the raw socket. Failures are
    /// best-effort like Go and retried by the health ticker.
    keepalive_applied: bool,
}

fn backend_health_in_snapshot(
    backends: &[control_proto::v1::BackendSnapshot],
    backend_id: &str,
    address: &str,
    cluster: &str,
) -> bool {
    backends.iter().any(|candidate| {
        candidate.backend_id == backend_id
            && candidate.address == address
            && candidate.cluster_name == cluster
            && candidate.healthy
    })
}

/// What the peeked command header alone allows the intake to do (PKT-003).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeekDecision {
    /// Read the logical request in full.
    Materialize,
    /// Relay it without reading; this command byte is all that is known.
    Stream(Command),
    /// Oversized, and its first byte is not a command at all.
    RejectUnknown,
}

/// Decides the intake from the peeked header, before a single payload byte is
/// read.
///
/// Go's `streamingForwardThreshold` is exactly `MaxPayloadLen`: once the FIRST
/// physical packet is already maximal the logical request continues into
/// further fragments, and materializing it is what invites the OOM. Go takes
/// the command from the peeked first byte in every case — that is the only
/// reason it can decide this before reading — and exempts `COM_CHANGE_USER`,
/// which is rewritten before forwarding and so must be read in full whatever
/// its size.
const fn classify_intake(first_byte: Option<u8>, first_packet_length: u32) -> PeekDecision {
    if (first_packet_length as usize) < MAX_PHYSICAL_PAYLOAD_LEN {
        return PeekDecision::Materialize;
    }
    let Some(byte) = first_byte else {
        return PeekDecision::Materialize;
    };
    match Command::try_from_byte(byte) {
        // Rewritten before forwarding: it cannot be relayed unread.
        Some(Command::ChangeUser) => PeekDecision::Materialize,
        Some(command) => PeekDecision::Stream(command),
        None => PeekDecision::RejectUnknown,
    }
}

/// How the next client command arrived.
enum CommandIntake {
    /// The logical request was read in full.
    Materialized(Vec<u8>),
    /// PKT-003: the first physical packet is already maximal, so nothing was
    /// read. Only the peeked command byte is known; the request is relayed at
    /// the forward point and the bounded prefix comes back from that relay.
    Streamed(Command),
}

/// One client command held between its event and the FSM's forward
/// authorization.
struct PendingCommand {
    /// The logical request, or — once a streamed command has been
    /// forwarded — the bounded prefix that replaces it. Never the whole
    /// request in the streamed case; see [`PendingCommand::streamed`].
    payload: Vec<u8>,
    command: Command,
    expected: ExpectedResponse,
    /// PKT-003. The request was relayed to the backend as it arrived and was
    /// never held in memory, so `payload` is at most
    /// [`STREAMED_COMMAND_CAPTURE`] bytes and the forward has already
    /// happened by the time the ordinary forward point is reached.
    streamed: bool,
    started: tokio::time::Instant,
    since_connection: Duration,
    traffic_before: BackendTraffic,
}

enum ChangeUserRoundProgress {
    Continue,
    Finished,
}

fn classify_change_user_response(
    payload: &[u8],
    negotiated: CapabilityFlags,
) -> Result<(ChangeUserEvent, bool, Option<bool>), ()> {
    let classified = classify_backend_auth_packet(payload, negotiated).map_err(|_| ())?;
    match classified {
        AuthEvent::BackendOk => {
            let in_transaction =
                change_user_ok_in_transaction(payload, negotiated).map_err(|_| ())?;
            Ok((
                ChangeUserEvent::BackendOk { in_transaction },
                true,
                Some(in_transaction),
            ))
        }
        AuthEvent::BackendError { .. } => {
            let code = u16::from_le_bytes([payload[1], payload[2]]);
            Ok((ChangeUserEvent::BackendError { code }, false, None))
        }
        AuthEvent::AuthSwitchRequest { .. }
        | AuthEvent::FastAuthSuccess
        | AuthEvent::ExtraAuthData => Ok((ChangeUserEvent::BackendAuthData, false, None)),
        AuthEvent::ClientAuthResponse | AuthEvent::BackendReconnected { .. } => Err(()),
    }
}

/// The single owner of all session wire I/O.
#[allow(clippy::struct_excessive_bools)]
struct Engine {
    connection_id: u64,
    endpoints: ConnectionEndpoints,
    /// The real client address from an inbound PROXY v2 header, when the
    /// listener consumed one. The owned header supplies the outbound backend
    /// preamble and its decoded inet source supplies lifecycle-log attribution;
    /// routing/admission/public-endpoint metering and IPC remain peer-based.
    inbound_proxy_header: Option<InboundProxyV2Header>,
    /// Set at most once when the one-shot inbound probe decodes an inet source.
    /// The outer owner reads it for the close log even if the Engine is aborted.
    proxy_client_source: Arc<OnceLock<std::net::SocketAddr>>,
    client_io: PacketIo<ClientTransport>,
    /// Raw-socket byte counters for the client socket. Created once at accept
    /// and kept for the session: TLS/compression upgrades wrap the same
    /// `CountedIo` in place, so this handle keeps counting the same socket.
    client_counters: Arc<ByteCounters>,
    backend: Option<BackendIo>,
    /// Fully authenticated/restored redirect target, invisible to command I/O
    /// until the FSM authorizes the atomic swap.
    candidate: Option<BackendIo>,
    /// Exact target carried by the one admitted redirect command.
    redirect_target: Option<RedirectTarget>,
    /// Traffic from successfully retired backend owners, retained for the
    /// connection-lifetime CLOSED event after an atomic swap.
    retired_backend_in: u64,
    retired_backend_out: u64,
    /// Successful backend generation: zero until initial auth attaches, then
    /// incremented only by a successful atomic redirect swap.
    backend_generation: u64,
    /// Immutable classification of the direct upstream/LB peer at accept.
    public_endpoint: bool,
    /// Complete last-good generations published after admission. Existing
    /// sessions consult only live topology health; connection-scoped config
    /// continues to come from `seat.snapshot()`.
    snapshot_updates: watch::Receiver<Arc<ValidatedSnapshot>>,
    /// Process-wide registry; absent only in legacy/unit compositions.
    metering: Option<MeteringSourceRegistry>,
    events: crate::gate::GatedSender<SessionEvent>,
    cmds: crate::gate::GatedReceiver<EngineCmd>,
    reports: mpsc::Sender<EngineReport>,
    route: Option<RouteSeed>,
    /// Process-local selector/router incarnation retained until the session
    /// engine itself exits. Selector drop is the final accounting backstop for
    /// the active assignment and any locally terminal pending attempt.
    local_route_lease: Option<LocalRouteChannel>,
    salt: [u8; 20],
    negotiated: CapabilityFlags,
    /// The client's raw handshake-response payload, re-sent verbatim to
    /// the backend (the backend re-challenges through the auth-switch
    /// relay, so the proxy-salt-scaled reply is acceptable there).
    client_handshake_raw: Vec<u8>,
    /// Current authenticated user/database/attributes. Unlike the immutable
    /// connection-shape fields in `client_handshake_raw`, these values change
    /// only after a successful `COM_CHANGE_USER` final OK and are also the
    /// identity used by a later session-token migration handshake.
    session_identity: Option<SessionIdentity>,
    /// A backend auth payload held between relay classification and its
    /// forward effect.
    relay_hold: Option<Vec<u8>>,
    cmd_state: Option<CommandSessionState>,
    in_transaction: bool,
    /// SES-00 prepared-statement registry: long-data/cursor guards
    /// synchronize into the FSM before command-completion boundaries.
    prepared: PreparedRegistry,
    /// SES-07/MIG-005 pending-redirect `BEGIN` hold. Present only between the
    /// internal `COMMIT` and the exactly-once replay/drop of a held
    /// transaction opener; the buffered request bytes live in
    /// `pending_command`, this owns only the one-shot discipline.
    held: Option<HeldBegin>,
    /// Set by [`SessionEffect::ResumeHeldRequest`] once the FSM authorizes the
    /// held request to replay; the hold loop consumes it to leave the pump.
    hold_replay_ready: bool,
    pending_command: Option<PendingCommand>,
    wire_end: Option<WireErrorSource>,
    quit_source: QuitSource,
    closing: bool,
    /// opt#13: raw outbound counter value (current backend) up to which
    /// bytes have been attributed to an observation — by a command's
    /// window or by a pure-traffic settlement. Both start from this shared
    /// boundary, so a queued no-response command's bytes are counted
    /// exactly once wherever they leave (inside a later command's write,
    /// on an idle/control flush, at session end) and never estimated ahead
    /// of the write. Reset to zero when the backend (and its counters) is
    /// swapped.
    attributed_outbound: u64,
    /// opt#13: set once a queued no-response command could not be written.
    /// From then on control commands are answered but only teardown effects
    /// run, whatever a later flush reports (a failed drain empties the
    /// queue, so the flush result alone is not a durable gate).
    wire_failed: bool,
    accepted_at: tokio::time::Instant,
    /// Absolute handshake budget (Go parity, `handshake_deadline`), measured
    /// from `accepted_at`. TLS accept/connect consume the *remaining* budget
    /// rather than a fresh timer, so the whole handshake — plaintext greeting,
    /// `SSLRequest`, TLS, auth — shares one deadline.
    handshake_deadline: Duration,
    /// Per-connection read-buffer size (`proxy.conn-buffer-size`, 32 KiB
    /// default); sizes the client and backend reader buffers only.
    stream_buffer_size: NonZeroUsize,
    /// Whether the client upgraded this connection to TLS via `SSLRequest`.
    /// Drives the greeting-response `tls` metadata and capability trust.
    frontend_tls_active: bool,
    metrics: MetricsRecorder,
    log_context: SessionLogContext,
    seat: SessionSeat,
    /// Shared session core (FSM + admission gate). S2b: the engine may commit a
    /// whitelisted steady transition inline via [`crate::gate::Gate::try_commit_steady`]
    /// instead of the `events`→loop→`cmd` round-trip.
    core: crate::gate::Gate,
    /// Engine-side view of the server shutdown signal, read under the core lock
    /// by the fast-path predicate so shutdown is linearized with the commit.
    shutdown: watch::Receiver<bool>,
    /// Whether the steady-state fast path is enabled (`TIPROXY_FSM_FASTPATH`).
    /// Default off: when false the engine always takes the slow path, so the
    /// gate plumbing is behaviourally inert.
    fast_path_enabled: bool,
}

/// Outcome of waiting for one specific FSM effect.
enum Awaited {
    /// The expected effect arrived (any others were handled inline).
    Got,
    /// Teardown began (or the loop is gone); abandon the wire phase.
    Closing,
}

/// Result of [`Engine::await_effect`]. On `Got` it carries the expected
/// effect's admission permit: the actual I/O for that effect (write greeting,
/// forward command/response, …) runs in the caller *after* `await_effect`
/// returns, so the caller holds the permit across that work and drops it only
/// once the effect's observable action is done — keeping the gate non-empty
/// until then.
enum EffectAck {
    Got(crate::gate::GatePermit),
    Closing,
}

/// Resolution of a SES-07/MIG-005 pending-redirect `BEGIN` hold.
enum HoldFlow {
    /// The redirect/commit phase resolved and the held request must replay
    /// exactly once as an ordinary command on the current backend owner.
    Replay,
    /// The internal `COMMIT` failed with a `MySQL` error that was forwarded to
    /// the client as the answer to its `BEGIN`; the command is complete and is
    /// deliberately not executed.
    Answered,
    /// A graceful close consumed the session before the held request could
    /// replay; it is dropped, never executed.
    Dropped,
    /// An unrecoverable wire/proxy error ended the exchange; the poison event
    /// was already sent to the FSM.
    Fatal(WireErrorSource),
}

/// Terminal outcome of the proxy-owned internal `COMMIT` round-trip.
enum InternalCommitOutcome {
    /// The commit completed with an OK carrying the terminal transaction
    /// status (`in_transaction` is the `SERVER_STATUS_IN_TRANS` bit).
    Committed {
        /// Whether the terminal OK still reported an open transaction.
        in_transaction: bool,
    },
    /// The commit completed with a `MySQL` ERR packet; the original bytes are
    /// forwarded to the client verbatim as the answer to the `BEGIN`.
    MysqlError {
        /// The exact ERR packet payload as read from the backend.
        packet: Vec<u8>,
    },
    /// The backend socket failed (send or read); poison as a backend-network
    /// end.
    BackendNetwork,
    /// A malformed/desynchronized or impossible internal response; poison as a
    /// proxy invariant.
    ProxyInvariant,
}

/// Whether a failed migration-snapshot attempt can safely return to the old
/// backend. Payload-bearing parser errors are deliberately collapsed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotFailure {
    /// A complete response (including backend ERR) was consumed, so the old
    /// command stream remains aligned and reusable.
    OldBackendUsable,
    /// The backend disconnected while the internal exchange was in flight.
    BackendNetwork,
    /// The response ended before the parser could consume a complete result;
    /// keeping the connection would risk treating unread internal bytes as a
    /// user-command response.
    Desynchronized,
    /// Fixed allowlist construction failed, which is a proxy invariant.
    ProxyInvariant,
}

/// Secret-free failure class for candidate construction. No variant carries
/// token, session-state, SQL, or backend payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFailure {
    InvalidTarget,
    Dial,
    Handshake,
    Authentication,
    Restore,
}

/// Short-lived wire payload containing token or session-state bytes. Its
/// backing allocation is overwritten on every return path before release;
/// `Debug` is intentionally unavailable so diagnostics cannot print it.
struct SensitiveBytes(Vec<u8>);

impl SensitiveBytes {
    const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl Drop for SensitiveBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Applies Go's second-handshake capability rules after the normal negotiated
/// intersection. The session token's encoding is payload-length driven:
/// `MakeHandshakeResponse` forces LENENC above 250 bytes even though real `TiDB`
/// greetings omit that bit.
fn migration_auth_capabilities(
    planned: CapabilityFlags,
    backend: CapabilityFlags,
    has_database: bool,
    token_length: usize,
) -> Result<CapabilityFlags, CandidateFailure> {
    let mut capabilities = planned.union(CapabilityFlags::PLUGIN_AUTH);
    capabilities = if has_database {
        if !backend.contains(CapabilityFlags::CONNECT_WITH_DB) {
            return Err(CandidateFailure::Handshake);
        }
        capabilities.union(CapabilityFlags::CONNECT_WITH_DB)
    } else {
        capabilities.without(CapabilityFlags::CONNECT_WITH_DB)
    };
    capabilities = if token_length > 250 {
        // Go's `MakeHandshakeResponse` forces the length-encoded auth form
        // for long payloads independently of the backend's advertised mask.
        capabilities.union(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA)
    } else {
        capabilities.without(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA)
    };
    Ok(capabilities)
}

impl Engine {
    fn register_current_metering(&mut self) -> Result<(), MeteringSamplerError> {
        let Some(registry) = &self.metering else {
            self.backend_generation = 1;
            return Ok(());
        };
        if self.backend_generation != 0 {
            return Err(MeteringSamplerError::SourceInvariant);
        }
        let backend = self
            .backend
            .as_ref()
            .ok_or(MeteringSamplerError::SourceInvariant)?;
        let generation = 1;
        let result = registry.register(
            MeteringAttribution {
                connection_id: self.connection_id,
                backend_generation: generation,
                backend_id: backend.id.clone(),
                cluster_name: backend.cluster.clone(),
                keyspace: backend.keyspace.clone(),
                local: backend.local,
                public_endpoint: self.public_endpoint,
            },
            Arc::clone(&backend.counters),
        );
        if result
            .as_ref()
            .is_err_and(|error| !matches!(error, MeteringSamplerError::UnknownAttribution))
        {
            registry.fail_closed();
        }
        result?;
        self.backend_generation = generation;
        Ok(())
    }

    fn finalize_metering_source(
        &self,
        generation: u64,
        inbound: u64,
        outbound: u64,
    ) -> Result<(), MeteringSamplerError> {
        let Some(registry) = &self.metering else {
            return Ok(());
        };
        let result = registry.finalize(self.connection_id, generation, inbound, outbound);
        if result.is_err() {
            registry.fail_closed();
        }
        result
    }

    /// opt#24b: runs from a box. Taking `self` by value made the task future
    /// hold the ~6 KB engine twice (the captured argument and the moved local
    /// the body works on); boxing keeps one heap copy and a pointer in the
    /// future, shrinking every session's engine task by about the engine size.
    async fn run(mut self: Box<Self>) -> EngineExit {
        let end = self.lifecycle().await;
        // A queued no-response command still unwritten at the end of the
        // wire phase leaves with this flush; its raw bytes (and any earlier
        // drained ones no observation covered) are settled here.
        let _ = self.flush_deferred_backend_write_quiet().await;
        self.settle_outbound();
        if let Some(source) = end {
            self.wire_end.get_or_insert(source);
            if self.quit_source == QuitSource::None {
                self.quit_source = coarse_quit_source(source);
            }
        }
        // Drain remaining effects so teardown commands (close/classify)
        // execute even after a wire failure ended the lifecycle early.
        while let Some((cmd, _permit)) = self.cmds.recv().await {
            if matches!(self.handle_cmd(cmd).await, Awaited::Closing) && self.closing {
                // Keep draining: ClassifySessionEnd may still follow.
            }
            // Permit drops here, after the effect is handled.
        }
        self.shutdown_io().await;
        // One exact final load feeds BOTH metering and CLOSED aggregation.
        let (current_backend_in, current_backend_out) =
            self.backend.as_ref().map_or((0, 0), |backend| {
                (backend.counters.inbound(), backend.counters.outbound())
            });
        if self.backend_generation != 0
            && self
                .finalize_metering_source(
                    self.backend_generation,
                    current_backend_in,
                    current_backend_out,
                )
                .is_err()
        {
            self.wire_end = Some(WireErrorSource::Proxy);
            self.quit_source = QuitSource::ProxyError;
        }
        let (backend_id, backend_address, cluster) = self.backend.as_ref().map_or_else(
            || (String::new(), String::new(), String::new()),
            |backend| {
                (
                    backend.id.clone(),
                    backend.address.clone(),
                    backend.cluster.clone(),
                )
            },
        );
        // Make the normal session-end accounting edge explicit. Task aborts
        // still fall back to Engine's field drop, but an orderly exit releases
        // the exact local selector only after I/O shutdown and metering have
        // observed the final backend totals. The route-local drop test protects
        // this from becoming an inert "retained but unread" field.
        release_local_route_lease(&mut self.local_route_lease);
        EngineExit {
            totals: TrafficTotals {
                client_in: self.client_counters.inbound(),
                client_out: self.client_counters.outbound(),
                backend_in: self.retired_backend_in.saturating_add(current_backend_in),
                backend_out: self.retired_backend_out.saturating_add(current_backend_out),
            },
            source: self.wire_end.unwrap_or(WireErrorSource::ClientNetwork),
            quit_source: self.quit_source,
            backend_id,
            backend_address,
            cluster,
            capabilities: u64::from(self.negotiated.bits()),
            namespace: self.log_context.namespace.clone(),
        }
    }

    /// The main wire lifecycle; returns the classified end source when
    /// the wire (not the FSM) ended the session.
    #[allow(clippy::too_many_lines)]
    async fn lifecycle(&mut self) -> Option<WireErrorSource> {
        // Accept is complete by construction (the server admitted us).
        if self
            .events
            .send(SessionEvent::ConnectionAccepted)
            .await
            .is_err()
        {
            return Some(WireErrorSource::Proxy);
        }
        // Scope the permit to exactly the greeting I/O so it is released before
        // the rest of the handshake — it must never leak into the command phase.
        match self.await_effect(SessionEffect::SendProxyGreeting).await {
            EffectAck::Closing => return None,
            EffectAck::Got(_greeting_permit) => {
                if let Err(source) = self.send_greeting().await {
                    return Some(source);
                }
            }
        }

        // PROXY protocol v2 inbound (WIRE-activation B): once the greeting is
        // flushed, run a one-shot probe before the first client packet. A LB's
        // header is already buffered (sent before the greeting) and is consumed
        // here — its source becomes the outbound header's source; a direct
        // client, woken by the greeting, sends its handshake response, whose
        // leading bytes are peeked as non-magic and left intact (so it is never
        // blocked). The header wire bytes are not a MySQL packet and never
        // advance the sequence; a malformed header fails closed.
        if self.proxy_protocol_v2_enabled() {
            // The probe consumes the remaining absolute handshake budget (not a
            // fresh timer): a truncated header, or a client that opens the
            // connection but sends nothing, fails closed at the deadline with no
            // fallback, matching the frozen partial-header contract.
            let budget = self.handshake_budget_remaining();
            let probe = tokio::time::timeout(budget, self.client_io.probe_inbound_proxy_v2());
            let Ok(Ok(source)) = probe.await else {
                self.quit_source = QuitSource::ProxyMalformed;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::ClientNetwork);
            };
            let source_address = source.as_ref().and_then(InboundProxyV2Header::source);
            record_proxy_client_log_source(
                &self.proxy_client_source,
                &mut self.log_context,
                self.endpoints.client_addr,
                source_address,
            );
            self.inbound_proxy_header = source;
        }

        // First client packet after the greeting. A client that sets `SSL`
        // must send a strict 32-byte `SSLRequest` (Go: the pre-TLS capability
        // mask is authoritative); we then upgrade to TLS and read the real
        // handshake response inside the encrypted session. Otherwise the first
        // packet already is the plaintext handshake response.
        let frontend_tls_available = self.frontend_tls_available();
        let payload = match self.read_client_handshake_packet().await {
            Ok(payload) => payload,
            Err(source) => return Some(source),
        };
        let ssl_request_capabilities =
            if leading_capabilities(&payload).contains(CapabilityFlags::SSL) {
                let Ok(ssl_request) = parse_ssl_request(&payload) else {
                    // SSL bit set but not a strict 32-byte SSLRequest: fail closed
                    // rather than falling back to a plaintext handshake response.
                    self.quit_source = QuitSource::ProxyMalformed;
                    let _ = self.events.send(SessionEvent::ClientIoError).await;
                    return Some(WireErrorSource::ClientNetwork);
                };
                if !frontend_tls_available {
                    // We only advertise SSL when a frontend config exists; a client
                    // asking to upgrade against a greeting that withheld SSL is a
                    // protocol violation.
                    self.quit_source = QuitSource::ProxyMalformed;
                    let _ = self.events.send(SessionEvent::ClientIoError).await;
                    return Some(WireErrorSource::ClientNetwork);
                }
                Some(ssl_request.capabilities)
            } else {
                None
            };

        let mut payload = if ssl_request_capabilities.is_some() {
            // FSM: Greeting --ClientSslRequest--> SslRequest (ActivateFrontendTls).
            if self
                .events
                .send(SessionEvent::ClientSslRequest)
                .await
                .is_err()
            {
                return Some(WireErrorSource::Proxy);
            }
            match self.await_effect(SessionEffect::ActivateFrontendTls).await {
                EffectAck::Closing => return None,
                EffectAck::Got(_tls_permit) => {
                    if let Err(source) = self.activate_frontend_tls().await {
                        return Some(source);
                    }
                }
            }
            // FSM: SslRequest --TlsActivated--> Greeting (no effect). The real
            // handshake response arrives inside TLS; its MySQL sequence
            // continues (SSLRequest was seq 1, this is seq 2).
            if self.events.send(SessionEvent::TlsActivated).await.is_err() {
                return Some(WireErrorSource::Proxy);
            }
            match self.read_client_handshake_packet().await {
                Ok(payload) => payload,
                Err(source) => return Some(source),
            }
        } else {
            payload
        };

        // Go parity: when TLS was negotiated, the pre-TLS `SSLRequest` mask is
        // authoritative, so overwrite the in-TLS response's leading capability
        // bytes with it BEFORE parsing. This makes layout-affecting bits
        // (CONNECT_WITH_DB / CONNECT_ATTRS / PLUGIN_AUTH_LENENC / ZSTD) — which
        // decide the response's field layout — come from the trusted mask, not
        // the untrusted second packet, and keeps the stored raw (re-parsed for
        // backend forwarding) consistent with what we negotiated.
        if let Some(ssl_capabilities) = ssl_request_capabilities {
            normalize_leading_capabilities(&mut payload, ssl_capabilities);
        }

        let Ok(parsed) = parse_handshake_response(&payload) else {
            self.quit_source = QuitSource::ProxyMalformed;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Some(WireErrorSource::ClientNetwork);
        };
        // After normalization `parsed.capabilities` is the trusted mask under
        // TLS (and the plaintext client mask otherwise), so it governs both the
        // parsed field layout and the negotiation.
        let negotiation = match negotiate_frontend(
            parsed.capabilities,
            proxy_capabilities(frontend_tls_available),
        ) {
            Ok(negotiation) => negotiation,
            Err(missing) => {
                self.quit_source = QuitSource::ClientHandshake;
                let (code, state, message) = missing.client_response();
                let seq = self.client_io.expected_read_sequence();
                self.client_io.reset_write_sequence(seq);
                // Go's capability failure is the fixed protocol-4.1
                // 1251/08004 packet even though this client omitted the very
                // bit being required. `self.negotiated` is still empty here,
                // so using it would make the encoder reject SQLSTATE and drop
                // the intended client response.
                let _ = self
                    .write_client_error_with_capabilities(
                        code,
                        state,
                        message,
                        CapabilityFlags::PROTOCOL_41,
                    )
                    .await;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::ClientNetwork);
            }
        };
        self.negotiated = negotiation.negotiated();
        self.client_handshake_raw.clone_from(&payload);
        let metadata = HandshakeMetadata {
            user: String::from_utf8_lossy(parsed.username).into_owned(),
            database: parsed
                .database
                .map(|database| String::from_utf8_lossy(database).into_owned())
                .unwrap_or_default(),
            auth_plugin: parsed
                .auth_plugin_name
                .map(|plugin| String::from_utf8_lossy(plugin).into_owned())
                .unwrap_or_default(),
            capability: self.negotiated.bits(),
            collation: u32::from(parsed.collation),
            zstd_level: u32::from(parsed.zstd_level.unwrap_or(0)),
            connection_attributes: std::collections::BTreeMap::default(),
            tls: self.frontend_tls_active,
        };
        let identity_attributes = parsed.attributes.map(|attributes| {
            attributes
                .iter()
                .filter_map(Result::ok)
                .map(|attribute| (attribute.key.to_vec(), attribute.value.to_vec()))
                .collect::<Vec<_>>()
        });
        self.session_identity = Some(SessionIdentity::new(
            parsed.username,
            parsed.database,
            identity_attributes.as_deref(),
        ));
        self.cmd_state = Some(CommandSessionState::new(self.negotiated, parsed.database));
        let routing = negotiation.routing_handshake(&parsed, self.endpoints);
        if self
            .events
            .send(SessionEvent::ClientHandshakeResponse)
            .await
            .is_err()
        {
            return Some(WireErrorSource::Proxy);
        }
        let dial_permit = match self.await_effect(SessionEffect::DialBackend).await {
            EffectAck::Got(permit) => permit,
            EffectAck::Closing => return None,
        };

        // Route + dial + backend greeting + verification + plan.
        let Some(mut seed) = self.route.take() else {
            return Some(WireErrorSource::Proxy);
        };
        let commander = seed.commander.clone();
        let admission_generation = self.seat.snapshot().generation();
        let (resolved_namespace, channel) = if let Some(route_plane) = seed.route_plane.take() {
            // Rust-owner mode resolves the raw client user and opens the exact
            // namespace incarnation locally. No HandshakeResponseEvent,
            // HandshakeDecision, RouteRequest, RouteAssignment, or RouteResult
            // body crosses the control bridge on this path.
            let admission_wait = self
                .handshake_budget_remaining()
                .min(LOCAL_ROUTE_TRANSIENT_WAIT);
            let admission = match route_plane
                .admit_within(&metadata.user, admission_wait)
                .await
            {
                Ok(admission) => admission,
                Err(error) => {
                    if let Some((code, state, message)) = local_admission_client_error(error) {
                        let seq = self.client_io.expected_read_sequence();
                        self.client_io.reset_write_sequence(seq);
                        let _ = self.write_client_error(code, state, message).await;
                    }
                    let _ = self.events.send(SessionEvent::ClientIoError).await;
                    return Some(WireErrorSource::Proxy);
                }
            };
            let resolved_namespace = admission.namespace().to_owned();
            let logical_client = self
                .inbound_proxy_header
                .as_ref()
                .and_then(InboundProxyV2Header::source)
                .unwrap_or(self.endpoints.client_addr)
                .to_string();
            let (channel, commands) = match LocalRouteChannel::new(
                admission,
                self.connection_id,
                logical_client,
                self.endpoints.client_addr.to_string(),
                self.endpoints.listener_addr.port().to_string(),
            ) {
                Ok(channel) => channel,
                Err(error) => {
                    if let Some((code, state, message)) = local_admission_client_error(error) {
                        let seq = self.client_io.expected_read_sequence();
                        self.client_io.reset_write_sequence(seq);
                        let _ = self.write_client_error(code, state, message).await;
                    }
                    let _ = self.events.send(SessionEvent::ClientIoError).await;
                    return Some(WireErrorSource::Proxy);
                }
            };
            if self
                .reports
                .send(EngineReport::LocalRouteCommands(commands))
                .await
                .is_err()
            {
                return Some(WireErrorSource::Proxy);
            }
            (resolved_namespace, SessionRouteChannel::Local(channel))
        } else {
            // Compatibility-only bridge path retained until the later dead-path
            // deletion slice. Every response expectation is armed before the
            // request that provokes it.
            let Some(decision_id) = seed.client.allocate_request_id() else {
                return Some(WireErrorSource::Proxy);
            };
            if seed
                .commander
                .expect_response(decision_id, ResponseKind::HandshakeDecision)
                .await
                .is_err()
            {
                return Some(WireErrorSource::Proxy);
            }
            let event_envelope = ControlEnvelope {
                request_id: decision_id,
                generation: admission_generation,
                priority: Priority::Control.into(),
                body: Some(Body::HandshakeResponse(HandshakeResponseEvent {
                    connection: Some(seed.identity.clone()),
                    handshake: Some(metadata.clone()),
                })),
                ..ControlEnvelope::default()
            };
            if seed.client.send(event_envelope).await.is_err() {
                return Some(WireErrorSource::Proxy);
            }
            let decision = loop {
                let Some(answer) = seed.responses.recv().await else {
                    return Some(WireErrorSource::Proxy);
                };
                if let Some(Body::HandshakeDecision(decision)) = answer.body {
                    break decision;
                }
            };
            if !decision.accept {
                let message = if decision.client_message.is_empty() {
                    "handshake rejected"
                } else {
                    decision.client_message.as_str()
                };
                let seq = self.client_io.expected_read_sequence();
                self.client_io.reset_write_sequence(seq);
                let _ = self.write_client_error(1105, *b"HY000", message).await;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::Proxy);
            }
            let mut resolved_namespace = decision.namespace;
            if resolved_namespace.is_empty() {
                resolved_namespace = seed.namespace;
            }
            let channel = BindingRouteChannel {
                client: seed.client,
                commander: seed.commander,
                responses: seed.responses,
                identity: seed.identity,
                metadata,
                namespace: resolved_namespace.clone(),
                generation: admission_generation,
            };
            (resolved_namespace, SessionRouteChannel::Bridge(channel))
        };
        self.log_context.namespace.clone_from(&resolved_namespace);
        // The dispatcher's per-session record adopts it too, so CLOSED
        // events and reconciliation carry the routing truth on the
        // wire. The commander waits for the applied acknowledgement; a
        // lost acknowledgement means later observers could still see
        // the pre-decision seed, so the session fails closed instead
        // of routing with ambiguous attribution.
        let local_route_owner = matches!(&channel, SessionRouteChannel::Local(_));
        let namespace_applied = if local_route_owner {
            commander
                .set_local_namespace(resolved_namespace.clone())
                .await
        } else {
            commander.set_namespace(resolved_namespace.clone()).await
        };
        if !namespace_applied {
            return Some(WireErrorSource::Proxy);
        }
        let mut route_engine = RouteEngine::new(
            channel,
            ClusterTcpDialer::new(self.metrics.clone()),
            DialSchedule::default(),
            CenteredJitter,
            self.connection_id,
        );
        let acquisition_started = tokio::time::Instant::now();
        let acquired = match route_engine.acquire(Vec::new()).await {
            Ok(acquired) => {
                self.metrics.try_record(Observation::GetBackend {
                    duration: acquisition_started.elapsed(),
                    succeeded: true,
                });
                acquired
            }
            Err(error) => {
                self.metrics.try_record(Observation::GetBackend {
                    duration: acquisition_started.elapsed(),
                    succeeded: false,
                });
                self.quit_source = acquire_quit_source(&error);
                // Go's total connect budget returns the last dial failure when
                // one exists, so it is a backend-handshake refusal; a terminal
                // empty selector is the distinct no-backend vocabulary. A
                // timeout before any assignment/control answer is an internal
                // routing failure and remains silent.
                match &error {
                    AcquireError::NoBackend { .. } | AcquireError::ClusterUnsupported { .. } => {
                        self.write_handshake_failure(FailureKind::NoBackend, false)
                            .await;
                    }
                    AcquireError::BudgetExhausted {
                        last_failure: Some(_),
                    } => {
                        self.quit_source = QuitSource::BackendHandshake;
                        self.write_handshake_failure(FailureKind::BackendHandshake, false)
                            .await;
                    }
                    AcquireError::BudgetExhausted { last_failure: None }
                    | AcquireError::Routing { .. }
                    | AcquireError::MalformedAssignment { .. }
                    | AcquireError::Channel(_) => {}
                }
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(wire_source_of(self.quit_source));
            }
        };
        let (channel, _) = route_engine.into_parts();
        if let SessionRouteChannel::Local(lease) = channel {
            // Successful acquisition moves the exact selector authority out of
            // the short-lived dial engine and into the SQL session owner. It is
            // installed before backend greeting/auth work so a later handshake
            // failure still closes the active ledger entry.
            self.local_route_lease = Some(lease);
        }
        let backend_id = acquired.backend.backend_id.clone();
        let backend_address = acquired.backend.address.clone();
        let backend_cluster = acquired.backend.cluster_name.clone();
        let backend_keyspace = acquired.backend.keyspace.clone();
        let backend_local = acquired.backend.local;
        // A metered session must know its immutable source attribution before
        // consuming or forwarding any backend protocol bytes. RouteEngine has
        // only completed the TCP dial here: the greeting is still unread and
        // the socket has not entered the counted/billable transport. Reject an
        // incomplete assignment locally without poisoning the process-wide
        // registry; this is the session-scoped fail-closed policy.
        if self.metering.is_some() && (backend_id.is_empty() || backend_keyspace.is_empty()) {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::Proxy);
        }
        // Health-appropriate keepalive at dial time (KA-003 family):
        // the snapshot's healthy/unhealthy backend policy follows the
        // router-reported health of this assignment. Mid-session
        // health transitions re-apply with DPL-07's topology feed.
        let backend_healthy = acquired.backend.healthy;
        let keepalive_applied = {
            let config = self.seat.snapshot().raw().config.as_ref();
            let policy = if backend_healthy {
                config.and_then(|config| config.healthy_backend_keepalive)
            } else {
                config.and_then(|config| config.unhealthy_backend_keepalive)
            };
            policy.is_none_or(|policy| {
                proxy_io::socket::apply_keepalive(
                    &acquired.conn,
                    crate::server::snapshot_keepalive(&policy),
                )
                .is_ok()
            })
        };
        // Wrap the raw backend socket in the innermost byte counter now — before
        // the PROXY header, any backend TLS upgrade, and MySQL framing — so the
        // PROXY preamble, TLS records, compressed frames, and plain packets all
        // count once at the bottom of the stack. Keepalive above still ran on
        // the bare `TcpStream`.
        let mut backend_socket = CountedIo::new(acquired.conn);
        let backend_counters = backend_socket.counters();
        // PROXY protocol v2 (WIRE-activation B): when the snapshot enables it,
        // announce the ORIGINAL client address to the backend as a raw preamble
        // that precedes every MySQL byte. Written straight to the socket before
        // it is wrapped in MySQL framing (and before any backend TLS upgrade),
        // matching Go's dial path — the header is a transport preamble, not a
        // MySQL packet, so it must bypass the PacketIo sequence framing.
        let proxy_v2_result = if self.proxy_protocol_v2_enabled() {
            // Source is the original client: the inbound PROXY header's address
            // when the listener consumed one, else this connection's own peer.
            write_backend_proxy_v2_header(
                &mut backend_socket,
                self.inbound_proxy_header.as_ref(),
                self.endpoints.client_addr,
            )
            .await
        } else {
            Ok(())
        };
        if let Err(source) = proxy_v2_result {
            let source = self.end_source(coarse_quit_source(source));
            if source == WireErrorSource::BackendNetwork {
                self.write_handshake_failure(FailureKind::BackendHandshake, false)
                    .await;
            }
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(source);
        }
        let mut backend = BackendIo {
            backend_io: PacketIo::with_read_stream_buffer_size(
                BackendTransport::Plain(backend_socket),
                self.stream_buffer_size,
            ),
            counters: backend_counters,
            id: backend_id.clone(),
            address: backend_address,
            cluster: backend_cluster,
            keyspace: backend_keyspace,
            local: backend_local,
            healthy: backend_healthy,
            keepalive_applied,
        };
        // Backend responses are multi-packet result sets; over-read buffering
        // serves their physical packets from one transport read. The backend
        // only sends solicited bytes, so the window is empty at every command
        // boundary (keeping `reset_layer_sequence`/idle-liveness clean).
        backend.backend_io.enable_read_buffering();
        let greeting_read = backend
            .backend_io
            .read_logical(HANDSHAKE_PAYLOAD_LIMIT)
            .await;
        let greeting_packet = match greeting_read {
            Ok(packet) => packet,
            Err(error) => {
                // A transport break while reading the backend greeting is a
                // backend NETWORK break on both observables — disconnect
                // dominates the handshake phase (Go). Previously this discarded
                // the transport error and hardcoded quit_source=BackendHandshake
                // while the wire said BackendNetwork: the exact A/B divergence
                // this fix removes. A genuine capability/TLS rejection is
                // handled by the verify/plan branches below and stays
                // BackendHandshake.
                let source = classify_packet_io(&error, SideMarker::Backend, SideMarker::Backend);
                let source = self.end_source(source);
                self.write_handshake_failure(FailureKind::BackendHandshake, false)
                    .await;
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(source);
            }
        };
        let greeting_payload = greeting_packet.payload;
        let Ok(backend_greeting) = mysql_wire::parse_initial_handshake(&greeting_payload) else {
            self.quit_source = QuitSource::BackendHandshake;
            let mysql_error = greeting_payload.first() == Some(&0xff);
            if mysql_error {
                let seq = self.client_io.expected_read_sequence();
                self.client_io.reset_write_sequence(seq);
                let _ = self.client_io.write_logical(&greeting_payload, true).await;
            }
            self.write_handshake_failure(FailureKind::BackendHandshake, mysql_error)
                .await;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::BackendNetwork);
        };
        let backend_caps = backend_greeting.capabilities;
        let (require_backend_tls, backend_tls_available) = self.backend_tls_policy();
        if let Err(error) = verify_backend(
            backend_caps,
            self.negotiated,
            proxy_capabilities(self.frontend_tls_available()),
            require_backend_tls,
        ) {
            self.quit_source = QuitSource::BackendHandshake;
            let kind = match error {
                BackendVerificationError::MissingCapabilities(_) => FailureKind::BackendCapability,
                BackendVerificationError::TlsRequired => FailureKind::BackendNoTls,
            };
            self.write_handshake_failure(kind, false).await;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::BackendNetwork);
        }
        let Ok(plan) = plan_backend_handshake(
            &routing,
            backend_caps,
            require_backend_tls,
            backend_tls_available,
        ) else {
            self.quit_source = QuitSource::ProxyError;
            self.write_handshake_failure(FailureKind::ProxyNoTls, false)
                .await;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::Proxy);
        };
        // Backend TLS activates before any credential leaves the proxy: send a
        // plaintext SSLRequest, upgrade the backend transport, then the full
        // handshake response travels inside TLS.
        let backend_tls_result = if matches!(plan.tls, BackendTlsMode::Enabled) {
            self.upgrade_backend_tls(
                &mut backend,
                plan.capabilities,
                self.handshake_budget_remaining(),
            )
            .await
        } else {
            Ok(())
        };
        if let Err(source) = backend_tls_result {
            self.quit_source = QuitSource::BackendHandshake;
            self.write_handshake_failure(FailureKind::BackendProxyProtocol, false)
                .await;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(source);
        }
        self.backend = Some(backend);
        // Health may have changed while the handshake was in flight. Reconcile
        // before exposing the command phase, using the latest complete
        // topology but this session's immutable keepalive values.
        self.refresh_backend_keepalive();
        if self
            .events
            .send(SessionEvent::BackendGreetingReceived)
            .await
            .is_err()
        {
            return Some(WireErrorSource::Proxy);
        }
        // The dial's processing (route + connect + backend greeting/verify)
        // ends here, where the next handshake effect begins; release its permit
        // so it never spans into the command phase.
        drop(dial_permit);
        let fwd_handshake_permit = match self
            .await_effect(SessionEffect::ForwardHandshakeToBackend)
            .await
        {
            EffectAck::Got(permit) => permit,
            EffectAck::Closing => return None,
        };
        // Go's `handshakeFirstTime` rewrite: forward the client's
        // response under the planned capability mask with the plugin
        // replaced by `auth_unknown_plugin` and the original auth data
        // preserved, so the backend re-requests authentication against
        // its own salt (the client's scramble answered the proxy's) and
        // keeps the `using password` semantics on failure.
        let forwarded = {
            let Ok(parsed) = parse_handshake_response(&self.client_handshake_raw) else {
                self.quit_source = QuitSource::ProxyMalformed;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::Proxy);
            };
            let attributes = parsed.attributes.map(|attributes| {
                attributes
                    .into_iter()
                    .filter_map(Result::ok)
                    .collect::<Vec<_>>()
            });
            let Ok(forwarded) = encode_handshake_response(HandshakeResponseParams {
                capabilities: plan.capabilities,
                max_packet_size: parsed.max_packet_size,
                collation: parsed.collation,
                username: parsed.username,
                auth_response: parsed.auth_response,
                database: parsed.database,
                auth_plugin_name: Some(UNKNOWN_AUTH_PLUGIN),
                attributes: attributes.as_deref(),
                // The zstd level is meaningful only when the BACKEND leg
                // negotiated zstd (its caps may differ from the client's). If
                // the backend did not advertise ZSTD, sending a level would
                // make `encode_handshake_response` reject the packet, so drop
                // it — matching Go, which carries the level per negotiated leg.
                zstd_level: if plan
                    .capabilities
                    .contains(CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM)
                {
                    parsed.zstd_level
                } else {
                    None
                },
            }) else {
                self.quit_source = QuitSource::ProxyMalformed;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::Proxy);
            };
            forwarded
        };
        if let Some(backend) = self.backend.as_mut() {
            // Continue the backend channel's connection-phase counter after its
            // greeting. Under backend TLS the writer already advanced past the
            // plaintext SSLRequest (seq 1) to seq 2, so it continues as-is;
            // otherwise the plaintext response is the first write and aligns to
            // the greeting (reader observed seq 0 -> expects 1).
            if !matches!(backend.backend_io.get_ref(), BackendTransport::Tls(_)) {
                let next = backend.backend_io.expected_read_sequence();
                backend.backend_io.reset_write_sequence(next);
            }
            if let Err(error) = backend.backend_io.write_logical(&forwarded, true).await {
                let quit = classify_packet_io(&error, SideMarker::Backend, SideMarker::Backend);
                let source = self.end_source(quit);
                self.write_handshake_failure(FailureKind::BackendHandshake, false)
                    .await;
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(source);
            }
        }

        // The handshake forward is done; release its permit before the auth
        // relay so it does not span into the command phase.
        drop(fwd_handshake_permit);

        // Engine-internal authentication relay; the FSM sees only the
        // terminal outcome.
        // Carry the client's negotiated zstd level into the relay so the
        // auth-OK compression effects select the right codec level (0 = absent).
        let mut relay = AuthRelay::new(
            self.negotiated,
            backend_caps,
            parsed.zstd_level.unwrap_or(0),
        );
        let auth_outcome = loop {
            match relay.turn() {
                AuthTurn::AwaitingBackend => {
                    let payload = match self.backend_read(HANDSHAKE_PAYLOAD_LIMIT).await {
                        Ok(payload) => payload,
                        Err(source) => {
                            // `backend_read` already classified this transport
                            // error through `end_source` (setting `quit_source`
                            // to the winner and returning its wire projection).
                            // A post-hoc `quit_source = BackendHandshake` here
                            // would re-fork the observables: an auth-phase read
                            // disconnect must stay BackendNetwork on BOTH, and a
                            // malformed/non-disconnect read its own class.
                            let _ = self.events.send(SessionEvent::BackendIoError).await;
                            return Some(source);
                        }
                    };
                    let event = match classify_backend_auth_packet(&payload, self.negotiated) {
                        Ok(AuthEvent::BackendError { class, .. }) => AuthEvent::BackendError {
                            class,
                            handler_reconnect: false,
                        },
                        Ok(event) => event,
                        Err(_) => {
                            self.quit_source = QuitSource::BackendHandshake;
                            let _ = self.events.send(SessionEvent::BackendIoError).await;
                            return Some(WireErrorSource::BackendNetwork);
                        }
                    };
                    self.relay_hold = Some(payload);
                    let Ok(step) = relay.on_event(event) else {
                        self.quit_source = QuitSource::BackendHandshake;
                        let _ = self.events.send(SessionEvent::BackendIoError).await;
                        return Some(WireErrorSource::Proxy);
                    };
                    if let Err(source) = self.run_auth_effects(&step.effects).await {
                        return Some(source);
                    }
                    if let Some(outcome) = step.outcome {
                        break outcome;
                    }
                }
                AuthTurn::AwaitingClient => {
                    let payload = match self.client_io.read_logical(HANDSHAKE_PAYLOAD_LIMIT).await {
                        Ok(packet) => packet.payload,
                        Err(error) => return Some(self.client_read_end(&error).await),
                    };
                    self.relay_hold = Some(payload);
                    let Ok(step) = relay.on_event(AuthEvent::ClientAuthResponse) else {
                        self.quit_source = QuitSource::ClientHandshake;
                        let _ = self.events.send(SessionEvent::ClientIoError).await;
                        return Some(WireErrorSource::Proxy);
                    };
                    if let Err(source) = self.run_auth_effects(&step.effects).await {
                        return Some(source);
                    }
                    if let Some(outcome) = step.outcome {
                        break outcome;
                    }
                }
                AuthTurn::AwaitingReconnect | AuthTurn::Finished => {
                    // Reconnect is never approved in this slice, and a
                    // finished relay exits through the outcome above.
                    self.quit_source = QuitSource::BackendHandshake;
                    let _ = self.events.send(SessionEvent::BackendIoError).await;
                    return Some(WireErrorSource::Proxy);
                }
            }
        };
        match auth_outcome {
            AuthOutcome::Success => {
                if self.events.send(SessionEvent::BackendAuthOk).await.is_err() {
                    return Some(WireErrorSource::Proxy);
                }
                let _attach_permit = match self.await_effect(SessionEffect::AttachBackend).await {
                    EffectAck::Got(permit) => permit,
                    EffectAck::Closing => return None,
                };
                // The source becomes billable only after the initial backend
                // is fully authenticated and the FSM attaches it. The same
                // Arc counters have covered dial → preamble/TLS → auth.
                if self.register_current_metering().is_err() {
                    self.quit_source = QuitSource::ProxyError;
                    let _ = self.events.send(SessionEvent::BackendIoError).await;
                    return Some(WireErrorSource::Proxy);
                }
                let _ = commander.set_backend(backend_id.clone()).await;
                let _fwd_auth_permit = match self
                    .await_effect(SessionEffect::ForwardAuthResultToClient)
                    .await
                {
                    EffectAck::Got(permit) => permit,
                    EffectAck::Closing => return None,
                };
                let current = self.backend_traffic();
                self.metrics.try_record(Observation::HandshakeCompleted {
                    backend: self
                        .backend
                        .as_ref()
                        .map_or_else(String::new, |backend| backend.address.clone()),
                    duration: self.accepted_at.elapsed(),
                    traffic: current,
                    local: self.backend.as_ref().is_some_and(|backend| backend.local),
                });
                // The handshake observation carried every byte so far: the
                // shared attribution boundary moves with it, so a session that
                // ends without a command does not settle them a second time.
                self.attributed_outbound = current.outbound_bytes;
                if let Some(backend) = &self.backend {
                    log_session(
                        "connection_ready",
                        &self.log_context,
                        &backend.id,
                        &backend.address,
                        &backend.cluster,
                        u64::from(self.negotiated.bits()),
                        QuitSource::None,
                    );
                }
            }
            AuthOutcome::Failed(kind) => {
                self.quit_source = failure_quit_source(kind);
                let _ = self.events.send(SessionEvent::BackendAuthFailed).await;
                return Some(failure_source(kind));
            }
        }

        // Ready: the command/response phases until the wire or the FSM
        // ends the session.
        self.command_phase().await
    }

    fn refresh_backend_keepalive(&mut self) {
        let latest = Arc::clone(&self.snapshot_updates.borrow());
        let Some(backend) = self.backend.as_ref() else {
            return;
        };
        let healthy = backend_health_in_snapshot(
            &latest.raw().backends,
            &backend.id,
            &backend.address,
            &backend.cluster,
        );
        if healthy == backend.healthy && backend.keepalive_applied {
            return;
        }

        let backend_label = backend.address.clone();
        let policy = self
            .seat
            .snapshot()
            .raw()
            .config
            .as_ref()
            .and_then(|config| {
                if healthy {
                    config.healthy_backend_keepalive
                } else {
                    config.unhealthy_backend_keepalive
                }
            })
            .map(|policy| crate::server::snapshot_keepalive(&policy));
        let succeeded = policy.is_none_or(|policy| {
            self.backend
                .as_ref()
                .and_then(|backend| backend.backend_io.get_ref().as_counted_stream())
                .is_some_and(|stream| {
                    proxy_io::socket::apply_keepalive(stream.get_ref(), policy).is_ok()
                })
        });
        let Some(backend) = self.backend.as_mut() else {
            return;
        };
        backend.healthy = healthy;
        backend.keepalive_applied = succeeded;
        self.metrics
            .try_record(Observation::BackendKeepaliveUpdated {
                backend: backend_label,
                healthy,
                succeeded,
            });
    }

    /// Ready/command/response/infile phases.
    #[allow(clippy::too_many_lines)]
    async fn command_phase(&mut self) -> Option<WireErrorSource> {
        // The compressed command-boundary reset must fire exactly once per
        // command. Control/probe activity loops via `continue` without a new
        // command boundary, so it must NOT re-reset the next command — whose
        // compressed frame `peek_packet` may already have decoded and staged
        // (advancing the shared sequence) when the control arm won the select.
        let mut just_served_control = false;
        let mut snapshot_updates_open = true;
        // opt#13: on a plaintext/TLS client leg, let one transport read stage
        // whatever the client already sent (the same bounded over-read window
        // the backend reader uses). The deferred-write decision below then
        // knows whether the whole next command is present without blocking,
        // and a small command costs one `recvfrom` instead of two. A layered
        // client leg keeps exact reads: its per-command sequence reset must
        // not find staged bytes.
        if !self.client_io.is_layered() {
            self.client_io.enable_read_buffering();
        }
        let first_health_recheck = tokio::time::Instant::now() + BACKEND_HEALTH_RECHECK_INTERVAL;
        let mut health_recheck =
            tokio::time::interval_at(first_health_recheck, BACKEND_HEALTH_RECHECK_INTERVAL);
        health_recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if self.closing {
                return None;
            }
            // Every client command starts a fresh wire exchange at sequence
            // zero, and its response answers at one. On a compressed leg the
            // compressed sequence also resets once per command (Go's
            // `ResetSequence`); the first read/write then slaves the uncompressed
            // sequence to it via the direction hooks. Skip the compressed reset
            // when merely re-entering after control activity, so a staged next
            // command is not rewound; the reset fails closed on in-flight data.
            if !just_served_control && self.client_io.reset_layer_sequence().is_err() {
                self.quit_source = QuitSource::ProxyError;
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::Proxy);
            }
            just_served_control = false;
            self.client_io.reset_read_sequence(0);
            // Between commands: serve control effects and probes while
            // waiting for the next client command. Only the peek is
            // raced — it retains consumed bytes inside the reader, so a
            // losing arm never drops partial-frame progress. Once a
            // header is visible the logical read runs uncontended; a
            // client stalling mid-frame is bounded by the owner's force
            // deadline, like any other mid-command stall.
            // opt#13: a deferred no-response command is still queued for the
            // backend. If the next client command is already here, let it join
            // that write (the select below re-peeks from the staged bytes);
            // otherwise flush now, so the backend never waits on a queued
            // command while the client is idle.
            if self
                .backend
                .as_ref()
                .is_some_and(|backend| backend.backend_io.has_deferred_write())
            {
                let staged = tokio::select! {
                    biased;
                    peeked = self.client_io.peek_packet() => Some(peeked),
                    () = std::future::ready(()) => None,
                };
                // Keep the queue only when the whole next command is staged
                // and can be consumed without another transport read; a
                // header-only or partial command (a client that stalls
                // mid-packet) must not hold the queued command hostage.
                let whole_next_command_staged = match staged {
                    Some(Ok(preview)) => {
                        let payload_length = preview.first_packet_length as usize;
                        payload_length < MAX_PHYSICAL_PAYLOAD_LEN
                            && self.client_io.staged_read_len()
                                >= PHYSICAL_PACKET_HEADER_LEN + payload_length
                    }
                    Some(Err(error)) => {
                        let source = self.client_read_end(&error).await;
                        return Some(source);
                    }
                    None => false,
                };
                if !whole_next_command_staged && !self.flush_deferred_backend_write_quiet().await {
                    let _ = self.events.send(SessionEvent::BackendIoError).await;
                    return Some(WireErrorSource::BackendNetwork);
                }
            }
            let (intake, command_started) = tokio::select! {
                changed = self.snapshot_updates.changed(), if snapshot_updates_open => {
                    if changed.is_err() {
                        snapshot_updates_open = false;
                    } else {
                        drop(self.snapshot_updates.borrow_and_update());
                        self.refresh_backend_keepalive();
                    }
                    just_served_control = true;
                    continue;
                }
                _ = health_recheck.tick() => {
                    self.refresh_backend_keepalive();
                    just_served_control = true;
                    continue;
                }
                cmd = self.cmds.recv() => {
                    let (cmd, _permit) = cmd?;
                    match self.handle_cmd(cmd).await {
                        Awaited::Closing => return None,
                        // Control/probe served — not a new command boundary, so
                        // the next iteration must not reset the compressed layer.
                        Awaited::Got => {
                            just_served_control = true;
                            continue;
                        }
                    }
                }
                peeked = self.client_io.peek_packet() => {
                    let preview = match peeked {
                        Ok(preview) => preview,
                        Err(error) => {
                            let source = self.client_read_end(&error).await;
                            return Some(source);
                        }
                    };
                    // The idle wait ends when the packet header becomes
                    // visible. Match Go's ExecuteCmd timer: include packet
                    // read/dispatch/response work, never connection idle time.
                    let started = tokio::time::Instant::now();
                    match classify_intake(preview.first_byte, preview.first_packet_length) {
                        // An oversized packet whose first byte is not a
                        // command. Go streams it to the backend anyway, but
                        // Rust already refuses unknown commands before
                        // forwarding, and reading the request only to reject it
                        // would reintroduce exactly the allocation this row
                        // exists to remove — so it is refused from the header.
                        PeekDecision::RejectUnknown => {
                            // The peeked prefix is never consumed, so the
                            // session cannot continue: end it explicitly with
                            // the same attribution the layered-sequence guard
                            // used to produce here, instead of re-peeking the
                            // same prefix and answering the error forever.
                            let _ = self
                                .write_client_error(1047, *b"08S01", "Unknown command")
                                .await;
                            self.quit_source = QuitSource::ProxyError;
                            let _ = self.events.send(SessionEvent::ClientIoError).await;
                            return Some(WireErrorSource::Proxy);
                        }
                        PeekDecision::Stream(command) => {
                            (CommandIntake::Streamed(command), started)
                        }
                        PeekDecision::Materialize => {
                            match self.client_io.read_logical(COMMAND_PAYLOAD_LIMIT).await {
                                Ok(packet) => {
                                    (CommandIntake::Materialized(packet.payload), started)
                                }
                                Err(error) => {
                                    let source = self.client_read_end(&error).await;
                                    return Some(source);
                                }
                            }
                        }
                    }
                }
            };
            self.client_io.reset_write_sequence(1);
            // Extract the plan's owned facts before the payload moves:
            // CommandPlan borrows the packet bytes.
            let (payload, streamed, planned) = match intake {
                CommandIntake::Materialized(payload) => {
                    let planned = {
                        let Ok(command_packet) = CommandPacket::decode(&payload) else {
                            let _ = self.events.send(SessionEvent::ClientIoError).await;
                            return Some(WireErrorSource::ClientNetwork);
                        };
                        dispatch(command_packet)
                            .map(|plan| (plan.command, plan.response))
                            .ok()
                    };
                    (payload, false, planned)
                }
                // The request has not been read and must not be. The response
                // shape follows from the command byte alone, and the state
                // effects are recovered from the captured prefix after the
                // relay — which is where Go reads them too.
                CommandIntake::Streamed(command) => (
                    Vec::new(),
                    true,
                    Some((command, command.expected_response())),
                ),
            };
            let Some((command, expected)) = planned else {
                // Unknown command byte: rejected before any forward.
                let _ = self
                    .write_client_error(1047, *b"08S01", "Unknown command")
                    .await;
                continue;
            };
            let event = if command == Command::Quit {
                SessionEvent::ClientCommandQuit
            } else {
                SessionEvent::ClientCommand
            };
            self.pending_command = Some(PendingCommand {
                payload,
                command,
                expected,
                streamed,
                started: command_started,
                since_connection: command_started.saturating_duration_since(self.accepted_at),
                traffic_before: self.backend_traffic(),
            });
            // S2b steady-state fast path: for a plain `ClientCommand` with the
            // gate quiescent (`open && !shutdown && pending == 0` and the FSM in
            // `Ready`), commit the `Ready -> Ready` transition inline under the
            // core lock and hold its in-flight permit across the forward,
            // skipping the `events` -> loop -> `cmd` round-trip. Default-off;
            // `Quit` and every non-whitelisted event always take the slow path.
            // The permit (fast) or the awaited effect's permit (slow) is held to
            // the end of this iteration, keeping `pending >= 1` across the
            // forward exactly as the slow path does.
            let fwd_cmd_permit = if self.fast_path_enabled && event == SessionEvent::ClientCommand {
                match self.core.try_commit_steady(
                    SessionEvent::ClientCommand,
                    SessionEffect::ForwardCommandToBackend,
                    &self.shutdown,
                ) {
                    crate::gate::FastPath::Committed(permit) => permit,
                    crate::gate::FastPath::Fallback => {
                        if self.events.send(event).await.is_err() {
                            return Some(WireErrorSource::Proxy);
                        }
                        match self
                            .await_effect(SessionEffect::ForwardCommandToBackend)
                            .await
                        {
                            EffectAck::Got(permit) => permit,
                            EffectAck::Closing => return None,
                        }
                    }
                }
            } else {
                if self.events.send(event).await.is_err() {
                    return Some(WireErrorSource::Proxy);
                }
                if event == SessionEvent::ClientCommandQuit {
                    // Quit tears down: the FSM goes straight to Closing and
                    // the teardown effects arrive; drain them here.
                    if let Some(pending) = self.pending_command.take() {
                        self.record_command(&pending);
                    }
                    return None;
                }
                match self
                    .await_effect(SessionEffect::ForwardCommandToBackend)
                    .await
                {
                    EffectAck::Got(permit) => permit,
                    EffectAck::Closing => return None,
                }
            };
            let Some(mut pending) = self.pending_command.take() else {
                return Some(WireErrorSource::Proxy);
            };
            // SES-07/MIG-005: hold a transaction-opening BEGIN while a redirect
            // is armed — commit the old backend internally, migrate, then
            // replay the BEGIN exactly once. This is a single non-looping check
            // before the forward, so the replay falls through to the ordinary
            // forward/response below and is therefore never re-held.
            // Go gates the hold on `!streamingForward` as well: a request whose
            // first physical packet is already maximal cannot be the `BEGIN`
            // this path exists to hold, and its bytes are gone downstream by
            // the time the prefix exists.
            if self.redirect_target.is_some()
                && !pending.streamed
                && need_hold_request(
                    pending.command,
                    &pending.payload,
                    self.in_transaction,
                    self.prepared.has_pending(),
                )
            {
                match self.hold_pending_begin(&pending).await {
                    HoldFlow::Answered => {
                        self.record_command(&pending);
                        if self.closing {
                            return None;
                        }
                        continue;
                    }
                    HoldFlow::Dropped => {
                        self.record_command(&pending);
                        return None;
                    }
                    HoldFlow::Fatal(source) => {
                        self.record_command(&pending);
                        return Some(source);
                    }
                    HoldFlow::Replay => {
                        // Re-enter the FSM for the replayed request: it is now
                        // an ordinary command on the (possibly swapped) backend.
                        if self.events.send(SessionEvent::ClientCommand).await.is_err() {
                            self.record_command(&pending);
                            return Some(WireErrorSource::Proxy);
                        }
                        let _fwd_cmd_permit = match self
                            .await_effect(SessionEffect::ForwardCommandToBackend)
                            .await
                        {
                            EffectAck::Got(permit) => permit,
                            EffectAck::Closing => {
                                self.record_command(&pending);
                                return None;
                            }
                        };
                    }
                }
            }
            if pending.command == Command::ChangeUser {
                let response_source = self.change_user_rounds(&pending).await;
                self.record_command(&pending);
                if let Some(source) = response_source {
                    return Some(source);
                }
                if self.closing {
                    return None;
                }
                continue;
            }
            if let Some(source) = self.forward_command_to_backend(&mut pending).await {
                self.record_command(&pending);
                return Some(source);
            }
            if let Some(sync) = self.apply_command_mutations(&pending, false)
                && self.events.send(sync).await.is_err()
            {
                return Some(WireErrorSource::Proxy);
            }

            if !pending.expected.waits_for_backend() {
                if self
                    .events
                    .send(SessionEvent::NoResponseCommandComplete)
                    .await
                    .is_err()
                {
                    return Some(WireErrorSource::Proxy);
                }
                self.record_command(&pending);
                continue;
            }
            let response_source = if pending.expected == ExpectedResponse::Prepare {
                self.prepare_response_rounds(&pending).await
            } else {
                self.response_rounds(&pending, &fwd_cmd_permit).await
            };
            self.record_command(&pending);
            if let Some(source) = response_source {
                return Some(source);
            }
            if self.closing {
                return None;
            }
        }
    }

    /// Rewrites and relays one `COM_CHANGE_USER` authentication exchange.
    ///
    /// The command packet is the only client auth-bearing allocation retained
    /// by the command owner. Auth responses after the backend's fresh challenge
    /// stream directly client-to-backend with zero capture, and no payload is
    /// exposed to logs, control IPC, or error values.
    async fn change_user_rounds(&mut self, pending: &PendingCommand) -> Option<WireErrorSource> {
        let Ok(plan) = plan_change_user(&pending.payload, self.negotiated) else {
            // Go classifies a malformed change-user packet as a proxy
            // protocol failure. Nothing reached the backend, and the
            // original payload remains private to this command owner.
            self.quit_source = QuitSource::ProxyMalformed;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Some(WireErrorSource::Proxy);
        };
        if let Err(source) = self.begin_change_user(&plan).await {
            return Some(source);
        }

        let mut relay = ChangeUserRelay::new();
        loop {
            let result = match relay.turn() {
                ChangeUserTurn::AwaitingBackend => {
                    self.change_user_backend_round(pending, &plan, &mut relay)
                        .await
                }
                ChangeUserTurn::AwaitingClient => self
                    .change_user_client_round(&mut relay)
                    .await
                    .map(|()| ChangeUserRoundProgress::Continue),
                ChangeUserTurn::Finished => Err(WireErrorSource::Proxy),
            };
            match result {
                Ok(ChangeUserRoundProgress::Continue) => {}
                Ok(ChangeUserRoundProgress::Finished) => return None,
                Err(source) => return Some(source),
            }
        }
    }

    async fn begin_change_user(&mut self, plan: &ChangeUserPlan) -> Result<(), WireErrorSource> {
        let Some(backend) = self.backend.as_mut() else {
            return Err(WireErrorSource::Proxy);
        };
        // One layered reset at the command boundary only. Every auth-loop
        // direction reversal below goes through PacketIo's begin-read/write
        // hooks; resetting the compressed layer inside the relay would rewind
        // live frames.
        if backend.backend_io.reset_layer_sequence().is_err() {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::Proxy);
        }
        backend.backend_io.reset_write_sequence(0);
        backend.backend_io.reset_read_sequence(1);
        if backend
            .backend_io
            .write_logical(&plan.rewritten, true)
            .await
            .is_err()
        {
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::BackendNetwork);
        }
        Ok(())
    }

    async fn change_user_backend_round(
        &mut self,
        pending: &PendingCommand,
        plan: &ChangeUserPlan,
        relay: &mut ChangeUserRelay,
    ) -> Result<ChangeUserRoundProgress, WireErrorSource> {
        self.align_change_user_backend_read();
        self.align_change_user_client_write();
        let forwarded = {
            let Some(backend) = self.backend.as_mut() else {
                return Err(WireErrorSource::Proxy);
            };
            PacketIo::forward_packet_to(
                &mut backend.backend_io,
                &mut self.client_io,
                COMMAND_PAYLOAD_LIMIT,
            )
            .await
        };
        let progress = match forwarded {
            Ok(progress) => progress,
            Err(error) => {
                // backend -> client forward: a source-read break is the
                // backend's, a destination-write break is the client's (the
                // IoSide inversion fix — never blindly `BackendNetwork`).
                let source = classify_packet_io(&error, SideMarker::Backend, SideMarker::Client);
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Err(self.end_source(source));
            }
        };
        let payload = progress.captured_prefix();
        if progress.logical_payload_bytes() != payload.len() as u64 {
            // Classification needs the complete bounded auth packet (notably
            // the auth-switch plugin terminator). The wire packet was already
            // forwarded like Go, but retaining a partially classified
            // exchange would be unsafe.
            self.quit_source = QuitSource::ProxyMalformed;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::Proxy);
        }
        let Ok((event, successful, in_transaction)) =
            classify_change_user_response(payload, self.negotiated)
        else {
            self.quit_source = QuitSource::ProxyMalformed;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::BackendNetwork);
        };
        let Ok(step) = relay.on_event(event) else {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::Proxy);
        };
        if step.effects.first() != Some(&ChangeUserEffect::ForwardBackendToClient) {
            return Err(WireErrorSource::Proxy);
        }

        if step
            .effects
            .contains(&ChangeUserEffect::CommitPendingIdentity)
        {
            let Some(identity) = self.session_identity.as_mut() else {
                return Err(WireErrorSource::Proxy);
            };
            identity.apply_change_user(&plan.pending);
        }
        if successful {
            // This is the sole prepared-state clear seam: dispatch owns
            // `ClearAll`, and the synchronization event must reach SES-00
            // before the command-completion event.
            if let Some(sync) = self.apply_command_mutations(pending, true)
                && self.events.send(sync).await.is_err()
            {
                return Err(WireErrorSource::Proxy);
            }
        }
        if let Some(in_transaction) = in_transaction {
            self.in_transaction = in_transaction;
        }
        let Some(event) = step.session_event else {
            return Ok(ChangeUserRoundProgress::Continue);
        };
        if self.events.send(event).await.is_err() {
            return Err(WireErrorSource::Proxy);
        }
        // Hold the forward ack's permit to the end of this change-user round
        // (teardown still finishes the round); do not gate on it, matching the
        // prior behavior.
        let _round_permit = match self
            .await_effect(SessionEffect::ForwardResponseToClient)
            .await
        {
            EffectAck::Got(permit) => Some(permit),
            EffectAck::Closing => None,
        };
        Ok(ChangeUserRoundProgress::Finished)
    }

    async fn change_user_client_round(
        &mut self,
        relay: &mut ChangeUserRelay,
    ) -> Result<(), WireErrorSource> {
        self.align_change_user_client_read();
        self.align_change_user_backend_write();
        let forwarded = {
            let Some(backend) = self.backend.as_mut() else {
                return Err(WireErrorSource::Proxy);
            };
            PacketIo::forward_packet_to(&mut self.client_io, &mut backend.backend_io, 0).await
        };
        if let Err(error) = forwarded {
            // client -> backend forward: source-read break is the client's,
            // destination-write break is the backend's.
            let source = classify_packet_io(&error, SideMarker::Client, SideMarker::Backend);
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(self.end_source(source));
        }
        let Ok(step) = relay.on_event(ChangeUserEvent::ClientAuthResponse) else {
            return Err(WireErrorSource::Proxy);
        };
        if step.effects != vec![ChangeUserEffect::ForwardClientToBackend]
            || step.session_event.is_some()
        {
            return Err(WireErrorSource::Proxy);
        }
        Ok(())
    }

    /// Plain/TLS transports have independent packet read/write trackers and
    /// need explicit direction handoff. Compressed transports MUST use their
    /// centralized `DirectionSync` hook instead: it slaves the packet tracker
    /// to the shared compressed sequence and rejects buffered transitions.
    fn align_change_user_client_read(&mut self) {
        if !matches!(self.client_io.get_ref(), ClientTransport::Compressed(_)) {
            self.client_io
                .reset_read_sequence(self.client_io.next_write_sequence());
        }
    }

    fn align_change_user_client_write(&mut self) {
        if !matches!(self.client_io.get_ref(), ClientTransport::Compressed(_)) {
            self.client_io
                .reset_write_sequence(self.client_io.expected_read_sequence());
        }
    }

    fn align_change_user_backend_read(&mut self) {
        let Some(backend) = self.backend.as_mut() else {
            return;
        };
        if !matches!(
            backend.backend_io.get_ref(),
            BackendTransport::Compressed(_)
        ) {
            backend
                .backend_io
                .reset_read_sequence(backend.backend_io.next_write_sequence());
        }
    }

    fn align_change_user_backend_write(&mut self) {
        let Some(backend) = self.backend.as_mut() else {
            return;
        };
        if !matches!(
            backend.backend_io.get_ref(),
            BackendTransport::Compressed(_)
        ) {
            backend
                .backend_io
                .reset_write_sequence(backend.backend_io.expected_read_sequence());
        }
    }

    /// Forwards one accepted command to the backend on a fresh exchange:
    /// the request restarts at sequence zero and its response lineage
    /// answers from one.
    async fn forward_command_to_backend(
        &mut self,
        pending: &mut PendingCommand,
    ) -> Option<WireErrorSource> {
        let Some(backend) = self.backend.as_mut() else {
            return Some(WireErrorSource::Proxy);
        };
        // Reset the backend compressed sequence once for this command (no-op on
        // a plaintext/TLS backend leg); the direction hooks re-slave the
        // uncompressed sequence on the next write/read. Fails closed on
        // in-flight data.
        if backend.backend_io.reset_layer_sequence().is_err() {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::Proxy);
        }
        let Some(backend) = self.backend.as_mut() else {
            return Some(WireErrorSource::Proxy);
        };
        backend.backend_io.reset_write_sequence(0);
        backend.backend_io.reset_read_sequence(1);
        if !pending.streamed {
            // opt#13: a command that expects no response (STMT_CLOSE,
            // SEND_LONG_DATA) is queued rather than written, so the command
            // that usually follows at once (the next PREPARE/EXECUTE) leaves
            // in the same transport write and the backend wakes once for
            // both — as it does behind Go, whose forwards are back to back.
            // The command loop flushes the queue before it would wait for the
            // client, and every backend write/read path drains it first.
            // Layered (compressed) legs keep the immediate write: their
            // per-command sequence reset cannot run over queued bytes.
            let deferred =
                !pending.expected.waits_for_backend() && !backend.backend_io.is_layered();
            let written = if deferred {
                backend
                    .backend_io
                    .write_logical_deferred(&pending.payload)
                    .await
            } else {
                backend
                    .backend_io
                    .write_logical(&pending.payload, true)
                    .await
            };
            if written.is_err() {
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(WireErrorSource::BackendNetwork);
            }
            return None;
        }
        // PKT-003. Relay the request fragment by fragment straight into the
        // backend, retaining only the bounded prefix. The destination
        // regenerates its own headers, so the client's fragmentation is not
        // what reaches TiDB — only the logical request is.
        let Self {
            client_io, backend, ..
        } = self;
        let Some(backend) = backend.as_mut() else {
            return Some(WireErrorSource::Proxy);
        };
        let progress = match PacketIo::forward_packet_to(
            client_io,
            &mut backend.backend_io,
            STREAMED_COMMAND_CAPTURE,
        )
        .await
        {
            Ok(progress) => progress,
            Err(error) => {
                // The relay reads the client and writes the backend, so the
                // failing side decides the attribution exactly as it does for
                // a materialized command.
                let source = self.client_read_end(&error).await;
                return Some(source);
            }
        };
        pending.payload = progress.captured_prefix().to_vec();
        // The state effects come from the prefix, which is where Go reads them
        // too (`forwardCommand` reuses `ForwardPacketTo`'s return as `request`,
        // "generally, the stmtID is enough"). A prefix that does not decode is
        // a request the session can no longer reason about, and the backend has
        // already seen it: fail closed rather than continue on unknown state.
        if CommandPacket::decode(&pending.payload)
            .ok()
            .and_then(|packet| dispatch(packet).ok())
            .is_none()
        {
            self.quit_source = QuitSource::ProxyMalformed;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Some(WireErrorSource::Proxy);
        }
        None
    }

    /// SES-07/MIG-005 pending-redirect `BEGIN` hold. The caller has already
    /// checked [`need_hold_request`] and an armed redirect at the forward
    /// point. This commits the current backend internally (its OK never
    /// reaches the client), lets the redirect/commit phase resolve, and
    /// reports whether the buffered request replays exactly once, was answered
    /// by a commit error, was dropped by a graceful close, or ended fatally.
    async fn hold_pending_begin(&mut self, _pending: &PendingCommand) -> HoldFlow {
        // Begin the hold; the first effect is always the internal COMMIT,
        // which this runtime issues explicitly below.
        let (mut held, _send_commit) = HeldBegin::start();
        match self.run_internal_commit().await {
            InternalCommitOutcome::Committed { in_transaction } => {
                if held.on_commit_ok().is_err() {
                    return self.poison_hold(WireErrorSource::Proxy).await;
                }
                // The internal COMMIT's OK is authoritative for the engine's
                // own transaction tracker, exactly like a normal response
                // status (Go's `query("COMMIT")` → `updateTxnStatus`). Without
                // this, a commit that closed the transaction leaves
                // `self.in_transaction` stale-true; if the replayed BEGIN then
                // fails with a statusless MySQL ERR (which cannot correct it),
                // a later statusless COM_STMT_PREPARE would read the stale flag
                // in `prepare_session_event` and wrongly reopen the transaction,
                // blocking drain/redirect forever.
                self.in_transaction = in_transaction;
                // A commit that closed the transaction reaches a migration
                // boundary (`InternalResponseTxnDone` → StartRedirectHandshake,
                // then the redirect resolution authorizes the replay). A commit
                // that left it open cannot migrate; the FSM authorizes the
                // replay on the current backend right away (unless draining).
                let event = if in_transaction {
                    SessionEvent::InternalResponseTxnOpen
                } else {
                    SessionEvent::InternalResponseTxnDone
                };
                if self.events.send(event).await.is_err() {
                    return HoldFlow::Fatal(WireErrorSource::Proxy);
                }
            }
            InternalCommitOutcome::MysqlError { packet } => {
                // Go's `IsMySQLError` path: the commit's own error answers the
                // BEGIN, forwarded verbatim exactly once; the request is never
                // executed. The client write sequence is already positioned at
                // the response slot for this command.
                if held.on_commit_error().is_err() {
                    return self.poison_hold(WireErrorSource::Proxy).await;
                }
                let write = self.client_io.write_logical(&packet, true).await;
                if let Err(error) = write {
                    // A write to the client failed: attribute to the client
                    // (a disconnect breaks its network; an Encode framing fault
                    // is a proxy bug, not a network break).
                    let source = classify_packet_io(&error, SideMarker::Client, SideMarker::Client);
                    let _ = self.events.send(SessionEvent::ClientIoError).await;
                    return HoldFlow::Fatal(self.end_source(source));
                }
                if self
                    .events
                    .send(SessionEvent::InternalResponseError)
                    .await
                    .is_err()
                {
                    return HoldFlow::Fatal(WireErrorSource::Proxy);
                }
                return HoldFlow::Answered;
            }
            InternalCommitOutcome::BackendNetwork => {
                self.quit_source = QuitSource::BackendNetwork;
                return self.poison_hold(WireErrorSource::BackendNetwork).await;
            }
            InternalCommitOutcome::ProxyInvariant => {
                self.quit_source = QuitSource::ProxyMalformed;
                return self.poison_hold(WireErrorSource::Proxy).await;
            }
        }

        // Pump control effects until the FSM resolves the hold: the redirect
        // handshake, the atomic swap, the success/failure notification, and
        // finally `ResumeHeldRequest` (which sets `hold_replay_ready`), or a
        // teardown that switches the engine into closing.
        self.held = Some(held);
        self.hold_replay_ready = false;
        let flow = loop {
            let Some((cmd, _permit)) = self.cmds.recv().await else {
                break HoldFlow::Fatal(WireErrorSource::Proxy);
            };
            if matches!(self.handle_cmd(cmd).await, Awaited::Closing) {
                break HoldFlow::Dropped;
            }
            if self.hold_replay_ready {
                self.hold_replay_ready = false;
                break HoldFlow::Replay;
            }
        };
        if matches!(flow, HoldFlow::Dropped)
            && let Some(held) = self.held.as_mut()
        {
            // Go executes the held request only while `closeStatus <
            // statusNotifyClose`; here the close won, so drop it.
            let _ = held.drop_for_close();
        }
        self.held = None;
        flow
    }

    /// Reports the poison event to the FSM and returns a fatal hold flow. The
    /// `wire_end` is recorded so the close log attributes the source.
    async fn poison_hold(&mut self, source: WireErrorSource) -> HoldFlow {
        self.wire_end = Some(source);
        let _ = self.events.send(SessionEvent::BackendIoError).await;
        HoldFlow::Fatal(source)
    }

    /// Issues the proxy-owned internal `COMMIT` on the current backend and
    /// parses its single response. Mirrors [`Self::capture_migration_snapshot`]:
    /// a clean ERR terminator is recoverable (forwarded to the client), any
    /// earlier parser error or IO failure poisons the connection.
    async fn run_internal_commit(&mut self) -> InternalCommitOutcome {
        let limits = InternalLimits::default();
        let query = InternalQuery::Commit;
        let Ok(request) = query.encode(limits) else {
            return InternalCommitOutcome::ProxyInvariant;
        };
        let Ok(mut parser) = query.parser(self.negotiated, limits) else {
            return InternalCommitOutcome::ProxyInvariant;
        };
        let Some(backend) = self.backend.as_mut() else {
            return InternalCommitOutcome::ProxyInvariant;
        };
        // Like every proxy-owned query, the internal COMMIT is a fresh command
        // exchange from compressed sequence zero (Go's `ResetSequence`).
        if let Err(error) = send_proxy_owned_query(&mut backend.backend_io, &request).await {
            return match error {
                ProxyOwnedQueryError::LayeredReset => InternalCommitOutcome::ProxyInvariant,
                ProxyOwnedQueryError::Send => InternalCommitOutcome::BackendNetwork,
            };
        }
        loop {
            let payload = match backend
                .backend_io
                .read_logical(limits.max_result_bytes)
                .await
            {
                Ok(packet) => packet.payload,
                Err(_) => return InternalCommitOutcome::BackendNetwork,
            };
            match parser.consume(&payload) {
                Ok(InternalProgress::Continue) => {}
                Ok(InternalProgress::Complete(InternalResult::Ok(ok))) => {
                    return InternalCommitOutcome::Committed {
                        in_transaction: ok.in_transaction(),
                    };
                }
                Ok(InternalProgress::Complete(InternalResult::SessionStates(_))) => {
                    // A COMMIT can only answer OK or ERR.
                    return InternalCommitOutcome::ProxyInvariant;
                }
                Err(_) if parser.state() == InternalParserState::Complete => {
                    // A backend ERR after a clean terminator: the commit failed
                    // with a server error the client must see verbatim.
                    return InternalCommitOutcome::MysqlError { packet: payload };
                }
                Err(_) => return InternalCommitOutcome::ProxyInvariant,
            }
        }
    }

    /// Streams one command's backend response(s) to the client.
    #[allow(clippy::too_many_lines)] // hot response loop; opt#5 bypass adds a branch
    async fn response_rounds(
        &mut self,
        pending: &PendingCommand,
        fwd_permit: &crate::gate::GatePermit,
    ) -> Option<WireErrorSource> {
        let Ok(mut observer) = ResponseObserver::new(
            pending.expected,
            self.negotiated,
            self.in_transaction,
            DEFAULT_RESPONSE_FLUSH_THRESHOLD,
        ) else {
            return Some(WireErrorSource::Proxy);
        };
        // opt#5: the first packet of a response carries the Command->Response
        // transition and always drives the FSM; only *subsequent* ordinary
        // `Continue` packets (result-set column defs / rows, ~2/3 of tpcc
        // response packets) can bypass the per-packet FSM round-trip.
        let mut first_packet = true;
        // Consecutive bypassed packets since the last scheduling point. A busy
        // backend can keep the read side continuously ready, so `try_recv` alone
        // never lets the FSM/SessionLoop producer task run to *enqueue* a
        // pending close/redirect (starvation risk on a single-threaded runtime).
        // Every this many bypasses we `yield_now`, giving that producer a
        // scheduling *opportunity*
        // (Tokio does not guarantee a specific producer runs next, so this is not
        // a hard packet bound — the hard forced-close bound remains the owner's
        // force-drain deadline; this only keeps that deadline from being starved
        // indefinitely on a continuously readable response).
        let mut bypass_since_yield: u32 = 0;
        loop {
            // opt#19: after the first packet, forward whole staged packets as
            // a run: each packet is classified through the observer (state
            // machine, flush accounting) inside the run without returning to
            // this loop, and forwarded from the backend's over-read window.
            // The run ends at a packet whose disposition is not a plain
            // `Continue` or that needs a flush (that packet is forwarded and
            // its effect is handled below exactly as before), at the yield
            // interval, or when the window holds no whole packet (the
            // per-packet path below then reads through the transport).
            let mut run_effect: Option<ResponseEffect> = None;
            let mut run_forwarded: u32 = 0;
            if !first_packet {
                let Some(backend) = self.backend.as_mut() else {
                    return Some(WireErrorSource::Proxy);
                };
                let run = PacketIo::forward_response_run_buffered(
                    &mut backend.backend_io,
                    &mut self.client_io,
                    BYPASS_YIELD_INTERVAL
                        .saturating_sub(bypass_since_yield)
                        .max(1),
                    RESPONSE_CAPTURE,
                    |staged| {
                        let Ok(packet) = ResponsePacket::from_forwarded(
                            staged.prefix,
                            u64::from(staged.payload_length),
                            staged.payload_length,
                            1,
                        ) else {
                            return RunDecision::StopBefore;
                        };
                        match observer.observe_backend(packet) {
                            Ok(effect) => {
                                let plain =
                                    matches!(effect.disposition, ResponseDisposition::Continue)
                                        && matches!(effect.flush, FlushAction::None);
                                run_effect = Some(effect);
                                if plain {
                                    RunDecision::Continue
                                } else {
                                    RunDecision::StopAfter
                                }
                            }
                            // Left staged: the per-packet path reads it and
                            // reports the observer error as it always did.
                            Err(_) => RunDecision::StopBefore,
                        }
                    },
                )
                .await;
                match run {
                    Ok(outcome) => run_forwarded = outcome.forwarded,
                    Err(error) => {
                        let source =
                            classify_packet_io(&error, SideMarker::Backend, SideMarker::Client);
                        let _ = self.events.send(SessionEvent::BackendIoError).await;
                        return Some(self.end_source(source));
                    }
                }
                if run_forwarded == 0 {
                    run_effect = None;
                }
            }
            let effect = if let Some(effect) = run_effect {
                // Every packet before the last was a bypassed `Continue`.
                bypass_since_yield = bypass_since_yield.saturating_add(run_forwarded - 1);
                effect
            } else {
                let forwarded = {
                    let Some(backend) = self.backend.as_mut() else {
                        return Some(WireErrorSource::Proxy);
                    };
                    PacketIo::forward_response_packet_buffered(
                        &mut backend.backend_io,
                        &mut self.client_io,
                        RESPONSE_CAPTURE,
                    )
                    .await
                };
                let progress = match forwarded {
                    Ok(progress) => progress,
                    Err(error) => {
                        // backend -> client forward: attribute a source-read break
                        // to the backend and a destination-write break to the
                        // client (IoSide inversion fix).
                        let source =
                            classify_packet_io(&error, SideMarker::Backend, SideMarker::Client);
                        let _ = self.events.send(SessionEvent::BackendIoError).await;
                        return Some(self.end_source(source));
                    }
                };
                let first_physical = progress.first_packet_length().unwrap_or(0);
                let Ok(packet) = ResponsePacket::from_forwarded(
                    progress.captured_prefix(),
                    progress.logical_payload_bytes(),
                    first_physical,
                    progress.physical_packets(),
                ) else {
                    return Some(WireErrorSource::Proxy);
                };
                let Ok(effect) = observer.observe_backend(packet) else {
                    let _ = self.events.send(SessionEvent::BackendIoError).await;
                    return Some(WireErrorSource::BackendNetwork);
                };
                effect
            };
            self.in_transaction = effect.in_transaction;
            if !matches!(effect.flush, FlushAction::None) && self.client_io.flush().await.is_err() {
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::ClientNetwork);
            }
            let completes = matches!(
                effect.disposition,
                ResponseDisposition::CompleteSuccess | ResponseDisposition::CompleteRaw
            );
            if completes
                && let Some(sync) = self.apply_command_mutations(pending, true)
                && self.events.send(sync).await.is_err()
            {
                return Some(WireErrorSource::Proxy);
            }
            // Execute/fetch cursor guard: the terminal status decides whether the
            // statement retains an open cursor. Its registry sync must reach the
            // FSM before the command-completion boundary below.
            if completes && let Some(source) = self.observe_prepared_cursor(pending, effect).await {
                return Some(source);
            }
            // opt#5: a non-first ordinary `Continue` packet applies no FSM state
            // change (the FSM would only echo `ForwardResponseToClient`). Skip
            // the per-packet `events.send` + `await_effect` cross-task round-trip
            // and instead drain any pending control non-blocking, so a forced
            // close / redirect is not starved by a long response and the bounded
            // `cmds` channel is drained at this boundary (it can still transiently
            // fill during a single packet's transfer). All other dispositions (first packet,
            // MoreResults, LOCAL INFILE, terminal) keep the full FSM round-trip.
            let bypass_fsm =
                !first_packet && matches!(effect.disposition, ResponseDisposition::Continue);
            first_packet = false;
            if bypass_fsm {
                bypass_since_yield += 1;
                if bypass_since_yield >= BYPASS_YIELD_INTERVAL {
                    bypass_since_yield = 0;
                    // Scheduling opportunity for the FSM/SessionLoop producer
                    // task so a pending close/redirect can be enqueued under a
                    // continuously readable response (Tokio does not guarantee it
                    // runs before the next forward); the next drain observes it.
                    tokio::task::yield_now().await;
                }
                if matches!(self.drain_control_pending().await, Awaited::Closing) {
                    return None;
                }
                continue;
            }
            bypass_since_yield = 0;
            let event = effect.session_event();
            let expected_ack = match effect.disposition {
                ResponseDisposition::LocalInfile => SessionEffect::RequestLocalInfileFromClient,
                _ => SessionEffect::ForwardResponseToClient,
            };
            // S2c response-point fast path: the response packet is already
            // flushed to the client above, so this `events`→loop→`await_effect`
            // round-trip is pure FSM bookkeeping with no I/O. When enabled and
            // the only in-flight registration is this command's own forward
            // permit (`pending == 1`), commit the response transition inline and
            // skip the hop. `LocalInfile` (which expects a different ack and
            // then reads from the client) always takes the slow path. Default-off.
            let committed_inline = self.fast_path_enabled
                && expected_ack == SessionEffect::ForwardResponseToClient
                && self.core.try_inline_commit_excluding(
                    fwd_permit,
                    event,
                    SessionEffect::ForwardResponseToClient,
                    &self.shutdown,
                );
            // On the slow path the ack permit is held to the end of the loop
            // iteration exactly as before (so flag-off timing is byte-identical);
            // on the inline path there is no new permit and `fwd_permit` keeps
            // the floor.
            let _resp_ack_permit = if committed_inline {
                None
            } else {
                if self.events.send(event).await.is_err() {
                    return Some(WireErrorSource::Proxy);
                }
                match self.await_effect(expected_ack).await {
                    EffectAck::Got(permit) => Some(permit),
                    EffectAck::Closing => return None,
                }
            };
            match effect.disposition {
                ResponseDisposition::Continue | ResponseDisposition::MoreResults => {}
                ResponseDisposition::LocalInfile => {
                    if let Some(source) = self.infile_rounds().await {
                        return Some(source);
                    }
                    if self.closing {
                        return None;
                    }
                }
                ResponseDisposition::CompleteSuccess
                | ResponseDisposition::CompleteRaw
                | ResponseDisposition::CompleteError { .. } => {
                    return None;
                }
            }
        }
    }

    /// Applies the prepared-statement cursor guard at a command's completion
    /// boundary. A `COM_STMT_EXECUTE` reporting `SERVER_STATUS_CURSOR_EXISTS`
    /// opens the statement's cursor guard (blocking migration); a
    /// `COM_STMT_FETCH` keeps it open until `SERVER_STATUS_LAST_ROW_SENT`; a
    /// non-cursor execute or a last-row fetch clears it. The guard update is
    /// keyed on `pending.command` (not the ambiguous `Query` response shape),
    /// and the resulting registry sync event reaches the FSM before the
    /// command-completion boundary (the SES-00 ordering).
    ///
    /// Called only from the `response_rounds` success branch: a backend ERR
    /// carries no status and never reaches here, so an execute that failed
    /// after long data keeps its pending guard (PS-003).
    async fn observe_prepared_cursor(
        &mut self,
        pending: &PendingCommand,
        effect: ResponseEffect,
    ) -> Option<WireErrorSource> {
        let code = match pending.command {
            Command::StmtExecute => CommandCode::STMT_EXECUTE,
            Command::StmtFetch => CommandCode::STMT_FETCH,
            _ => return None,
        };
        // Dispatch validated this fixed statement-ID prefix before any backend
        // write, so a parse failure now is an internal invariant violation, not
        // a client error: fail closed rather than silently skip the guard.
        let Ok(statement_id) = PreparedRegistry::statement_id(&pending.payload, code) else {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Some(WireErrorSource::Proxy);
        };
        self.prepared
            .observe_response(pending.command, statement_id, effect);
        if self
            .events
            .send(self.prepared.session_event())
            .await
            .is_err()
        {
            return Some(WireErrorSource::Proxy);
        }
        None
    }

    /// Streams a `COM_STMT_PREPARE` special response to the client.
    ///
    /// The response is the prepare-OK header, then the declared parameter
    /// definitions, then the declared column definitions, with a classic EOF
    /// after each non-empty group unless `DEPRECATE_EOF` was negotiated; a
    /// leading ERR ends it immediately. [`PrepareObserver`] mirrors Go's
    /// `forwardPrepareCmd`: it counts the two metadata groups from the header,
    /// validates each classic EOF, and flushes exactly once at the terminal
    /// boundary. The prepare-OK carries no server status (Go leaves the
    /// transaction state untouched for a prepare), so `self.in_transaction` is
    /// never rewritten here. On success the returned metadata is registered
    /// before the completion event so a queued redirect or drain observes the
    /// fresh (Idle) guard.
    async fn prepare_response_rounds(
        &mut self,
        _pending: &PendingCommand,
    ) -> Option<WireErrorSource> {
        let mut observer = PrepareObserver::new(self.negotiated);
        loop {
            let forwarded = {
                let Some(backend) = self.backend.as_mut() else {
                    return Some(WireErrorSource::Proxy);
                };
                PacketIo::forward_response_packet_buffered(
                    &mut backend.backend_io,
                    &mut self.client_io,
                    RESPONSE_CAPTURE,
                )
                .await
            };
            let progress = match forwarded {
                Ok(progress) => progress,
                Err(error) => {
                    // backend -> client forward: attribute a source-read break
                    // to the backend and a destination-write break to the
                    // client (IoSide inversion fix).
                    let source =
                        classify_packet_io(&error, SideMarker::Backend, SideMarker::Client);
                    let _ = self.events.send(SessionEvent::BackendIoError).await;
                    return Some(self.end_source(source));
                }
            };
            let first_physical = progress.first_packet_length().unwrap_or(0);
            let Ok(packet) = ResponsePacket::from_forwarded(
                progress.captured_prefix(),
                progress.logical_payload_bytes(),
                first_physical,
                progress.physical_packets(),
            ) else {
                return Some(WireErrorSource::Proxy);
            };
            // A malformed prepare header/EOF is a backend protocol violation:
            // fail closed and tear the session down, never silently forward on.
            let Ok(effect) = observer.observe(packet) else {
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(WireErrorSource::BackendNetwork);
            };
            if !matches!(effect.flush, FlushAction::None) && self.client_io.flush().await.is_err() {
                let _ = self.events.send(SessionEvent::ClientIoError).await;
                return Some(WireErrorSource::ClientNetwork);
            }
            // Register before the completion event: reusing a statement ID
            // replaces any stale unknown-ID guard with a fresh Idle state
            // atomically. The registry's sync event must then reach the FSM
            // ahead of the command-completion boundary (the SES-00 ordering),
            // so a queued redirect or drain observes the fresh guard rather
            // than a stale pending one.
            if let PrepareDisposition::CompleteSuccess(metadata) = effect.disposition {
                self.prepared.register(metadata);
                if self
                    .events
                    .send(self.prepared.session_event())
                    .await
                    .is_err()
                {
                    return Some(WireErrorSource::Proxy);
                }
            }
            let event = self.prepare_session_event(effect.disposition);
            if self.events.send(event).await.is_err() {
                return Some(WireErrorSource::Proxy);
            }
            let _fwd_resp_permit = match self
                .await_effect(SessionEffect::ForwardResponseToClient)
                .await
            {
                EffectAck::Got(permit) => permit,
                EffectAck::Closing => return None,
            };
            match effect.disposition {
                PrepareDisposition::Continue => {}
                PrepareDisposition::CompleteSuccess(_)
                | PrepareDisposition::CompleteError { .. } => {
                    return None;
                }
            }
        }
    }

    /// Maps a prepare-response disposition onto the SES-00 FSM completion
    /// event. A prepare carries no server status, so the transaction state is
    /// the retained value (a prepare inside a transaction keeps it open;
    /// outside, done) — mirroring `ResponseEffect::session_event`.
    fn prepare_session_event(&self, disposition: PrepareDisposition) -> SessionEvent {
        match disposition {
            PrepareDisposition::Continue => SessionEvent::BackendResponsePart,
            PrepareDisposition::CompleteError { .. } => SessionEvent::BackendResponseErrorComplete,
            PrepareDisposition::CompleteSuccess(_) => {
                if self.in_transaction {
                    SessionEvent::BackendResponseTxnOpen
                } else {
                    SessionEvent::BackendResponseTxnDone
                }
            }
        }
    }

    /// The client LOCAL INFILE upload until the empty terminator.
    async fn infile_rounds(&mut self) -> Option<WireErrorSource> {
        // The upload continues each side's exchange in lockstep: the
        // client's next chunk follows the forwarded infile request, and
        // the chunks forwarded to the backend follow its request packet.
        let seq = self.client_io.next_write_sequence();
        self.client_io.reset_read_sequence(seq);
        if let Some(backend) = self.backend.as_mut() {
            let seq = backend.backend_io.expected_read_sequence();
            backend.backend_io.reset_write_sequence(seq);
        }
        loop {
            let payload = match self.client_io.read_logical(COMMAND_PAYLOAD_LIMIT).await {
                Ok(packet) => packet.payload,
                Err(error) => {
                    let source = self.client_read_end(&error).await;
                    return Some(source);
                }
            };
            let done = payload.is_empty();
            let event = if done {
                SessionEvent::ClientInfileEnd
            } else {
                SessionEvent::ClientInfileChunk
            };
            let ack = if done {
                SessionEffect::ForwardInfileEndToBackend
            } else {
                SessionEffect::ForwardInfileChunkToBackend
            };
            if self.events.send(event).await.is_err() {
                return Some(WireErrorSource::Proxy);
            }
            let _ack_permit = match self.await_effect(ack).await {
                EffectAck::Got(permit) => permit,
                EffectAck::Closing => return None,
            };
            let Some(backend) = self.backend.as_mut() else {
                return Some(WireErrorSource::Proxy);
            };
            if backend
                .backend_io
                .write_logical(&payload, done)
                .await
                .is_err()
            {
                let _ = self.events.send(SessionEvent::BackendIoError).await;
                return Some(WireErrorSource::BackendNetwork);
            }
            if done {
                // The final backend response continues after the last
                // uploaded chunk on both sides of the relay.
                let seq = backend.backend_io.next_write_sequence();
                backend.backend_io.reset_read_sequence(seq);
                let client_seq = self.client_io.expected_read_sequence();
                self.client_io.reset_write_sequence(client_seq);
                return None;
            }
        }
    }

    /// Waits for one specific effect, handling every other command
    /// inline; teardown-class effects switch the engine into closing.
    async fn await_effect(&mut self, expected: SessionEffect) -> EffectAck {
        loop {
            let Some((cmd, permit)) = self.cmds.recv().await else {
                return EffectAck::Closing;
            };
            match cmd {
                // Hand the permit to the caller: the effect's I/O runs after we
                // return, and the gate must stay non-empty until it completes.
                EngineCmd::Effect(effect) if effect == expected => {
                    return EffectAck::Got(permit);
                }
                other => {
                    // A non-expected cmd is fully handled here; its permit drops
                    // at the end of this iteration, after handle_cmd.
                    if matches!(self.handle_cmd(other).await, Awaited::Closing) {
                        return EffectAck::Closing;
                    }
                }
            }
        }
    }

    /// opt#5: non-blocking, bounded drain of pending engine control commands at
    /// a bypassed-`Continue`-packet boundary. Handles `Probe` / `PrepareRedirect` /
    /// control effects (redirect, `ReleaseBackend`, close) inline so a long
    /// response cannot starve a forced close or redirect, and drains queued
    /// control so a full `cmds` channel does not block the producer's sends (the
    /// channel can still transiently fill during a single packet's transfer).
    /// Bounded by the channel capacity so it always terminates, even under a continuous control
    /// stream (any excess is handled at the next packet boundary). Returns
    /// `Closing` on a teardown effect or a disconnected engine.
    ///
    /// This does NOT provide real-time close mid-packet: a backend stalled part
    /// way through a physical packet is still bounded only by the owner's
    /// existing force-drain deadline, exactly as before opt#5.
    async fn drain_control_pending(&mut self) -> Awaited {
        for _ in 0..ENGINE_CMD_CAPACITY {
            match self.cmds.try_recv() {
                Ok((cmd, _permit)) => {
                    if matches!(self.handle_cmd(cmd).await, Awaited::Closing) {
                        return Awaited::Closing;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Awaited::Closing,
            }
        }
        Awaited::Got
    }

    /// Executes one out-of-band command (control effects, probes).
    /// Writes out a deferred no-response command (opt#13) before anything
    /// that leaves the plain command loop: control effects, probes, and the
    /// paths that swap or read the backend all see a drained queue. Returns
    /// whether the backend writer is still usable; never touches the event
    /// queue, so it is safe while the loop may be waiting on a reply.
    async fn flush_deferred_backend_write_quiet(&mut self) -> bool {
        let Some(backend) = self.backend.as_mut() else {
            return true;
        };
        if !backend.backend_io.has_deferred_write() {
            return true;
        }
        if backend.backend_io.flush().await.is_err() {
            return false;
        }
        self.settle_outbound();
        true
    }

    /// Publishes the raw outbound bytes written since the shared attributed
    /// boundary as a pure traffic observation and advances the boundary.
    /// Used after a flush outside any command window and at session end,
    /// so queued bytes are attributed exactly once even when no command
    /// observation follows (idle disconnect, control command, teardown).
    fn settle_outbound(&mut self) {
        let Some(backend) = self.backend.as_ref() else {
            return;
        };
        let current = backend.counters.outbound();
        let outbound_bytes = current.saturating_sub(self.attributed_outbound);
        self.attributed_outbound = current;
        if outbound_bytes == 0 {
            return;
        }
        self.metrics.try_record(Observation::BackendTrafficSettled {
            backend: backend.address.clone(),
            traffic: BackendTraffic {
                outbound_bytes,
                ..BackendTraffic::default()
            },
            local: backend.local,
        });
    }

    async fn handle_cmd(&mut self, cmd: EngineCmd) -> Awaited {
        // A control command may redirect, probe, or drain the backend: the
        // queued no-response command must reach the current backend first.
        // If it cannot (now or earlier — `wire_failed` is durable, a failed
        // drain empties the queue so a later flush would look clean), the
        // wire is over: answer the request (the loop may be waiting on the
        // reply, so nothing is sent on the event queue before that), run
        // only teardown effects, and abandon the wire phase with the
        // backend-network attribution.
        if self.wire_failed || !self.flush_deferred_backend_write_quiet().await {
            let first_failure = !self.wire_failed;
            self.wire_failed = true;
            self.closing = true;
            self.wire_end.get_or_insert(WireErrorSource::BackendNetwork);
            if self.quit_source == QuitSource::None {
                self.quit_source = QuitSource::BackendNetwork;
            }
            match cmd {
                EngineCmd::Probe(reply) => {
                    let _ = reply.send(false);
                }
                // Only teardown effects still run; anything that would use
                // or swap the backend (redirect handshake, swap, forwards)
                // is refused now that the wire is over.
                EngineCmd::Effect(
                    effect @ (SessionEffect::BeginDrainTimer
                    | SessionEffect::ReleaseBackend
                    | SessionEffect::CloseBackend
                    | SessionEffect::CloseClient
                    | SessionEffect::ClassifySessionEnd),
                ) => {
                    let _ = self.handle_effect(effect).await;
                }
                EngineCmd::PrepareRedirect(_) | EngineCmd::Effect(_) => {}
            }
            // The request is answered, so the loop is free to consume: give
            // the FSM its one termination signal (once), then abandon the wire.
            if first_failure {
                let _ = self.events.send(SessionEvent::BackendIoError).await;
            }
            return Awaited::Closing;
        }
        match cmd {
            EngineCmd::Probe(reply) => {
                let _ = reply.send(self.backend_alive());
                Awaited::Got
            }
            EngineCmd::PrepareRedirect(target) => {
                // The redirect gate admits at most one pending command. Any
                // second preparation would violate that serialization
                // contract; retain the first exact target and let the normal
                // terminal path fail closed if the invariant is ever broken.
                if self.redirect_target.is_none() && self.candidate.is_none() {
                    self.redirect_target = Some(target);
                }
                Awaited::Got
            }
            EngineCmd::Effect(effect) => self.handle_effect(effect).await,
        }
    }

    async fn handle_effect(&mut self, effect: SessionEffect) -> Awaited {
        match effect {
            SessionEffect::BeginDrainTimer => Awaited::Got,
            SessionEffect::StartRedirectHandshake => {
                self.handle_redirect_snapshot().await;
                Awaited::Got
            }
            SessionEffect::NotifyRedirectSucceeded => {
                let _ = self
                    .reports
                    .send(EngineReport::RedirectFinished {
                        succeeded: true,
                        backend_id: self
                            .backend
                            .as_ref()
                            .map(|backend| backend.id.clone())
                            .unwrap_or_default(),
                        code: ErrorCode::Ok,
                    })
                    .await;
                Awaited::Got
            }
            SessionEffect::NotifyRedirectFailed => {
                self.candidate = None;
                self.redirect_target = None;
                let _ = self
                    .reports
                    .send(EngineReport::RedirectFinished {
                        succeeded: false,
                        backend_id: self
                            .backend
                            .as_ref()
                            .map(|backend| backend.id.clone())
                            .unwrap_or_default(),
                        code: ErrorCode::RedirectFailed,
                    })
                    .await;
                Awaited::Got
            }
            SessionEffect::ResumeHeldRequest => {
                // SES-07/MIG-005: the FSM reached a point where a held
                // `BEGIN` may replay (redirect resolved, or an internal
                // COMMIT left the transaction open with no draining). Take the
                // held request for its single replay; a no-op when nothing is
                // held (the effect is emitted unconditionally at those
                // boundaries). A wrong-phase take is a proxy invariant.
                match self.held.as_mut() {
                    None => Awaited::Got,
                    Some(held) => match held.take_for_replay() {
                        Ok(HoldEffect::ReplayHeldRequest) => {
                            self.hold_replay_ready = true;
                            Awaited::Got
                        }
                        Ok(_) | Err(_) => self.close_for_invariant(),
                    },
                }
            }
            SessionEffect::ReleaseBackend => {
                self.closing = true;
                Awaited::Closing
            }
            SessionEffect::CloseBackend => {
                self.closing = true;
                if let Some(backend) = self.backend.as_mut() {
                    let _ = backend.backend_io.flush().await;
                }
                Awaited::Closing
            }
            SessionEffect::CloseClient => {
                self.closing = true;
                let _ = self.client_io.flush().await;
                Awaited::Closing
            }
            SessionEffect::ClassifySessionEnd => {
                self.closing = true;
                self.wire_end.get_or_insert(WireErrorSource::Proxy);
                Awaited::Closing
            }
            SessionEffect::SwapBackend => self.swap_backend(),
            SessionEffect::ActivateFrontendTls
            | SessionEffect::SendProxyGreeting
            | SessionEffect::DialBackend
            | SessionEffect::ForwardHandshakeToBackend
            | SessionEffect::ForwardAuthResultToClient
            | SessionEffect::AttachBackend
            | SessionEffect::ForwardCommandToBackend
            | SessionEffect::ForwardResponseToClient
            | SessionEffect::RequestLocalInfileFromClient
            | SessionEffect::ForwardInfileChunkToBackend
            | SessionEffect::ForwardInfileEndToBackend => {
                // A phase effect the engine did not expect here is an
                // invariant violation: fail closed as a proxy error.
                self.closing = true;
                self.wire_end = Some(WireErrorSource::Proxy);
                Awaited::Closing
            }
        }
    }

    fn swap_backend(&mut self) -> Awaited {
        let Some(candidate) = self.candidate.take() else {
            return self.close_for_invariant();
        };
        let Some(next_generation) = self.backend_generation.checked_add(1) else {
            return self.close_for_invariant();
        };
        let Some(previous) = self.backend.as_ref() else {
            return self.close_for_invariant();
        };
        // Exactly one pair of atomic loads closes the old source and feeds the
        // connection-lifetime CLOSED totals.
        let previous_in = previous.counters.inbound();
        let previous_out = previous.counters.outbound();
        if self
            .finalize_metering_source(self.backend_generation, previous_in, previous_out)
            .is_err()
        {
            self.retired_backend_in = self.retired_backend_in.saturating_add(previous_in);
            self.retired_backend_out = self.retired_backend_out.saturating_add(previous_out);
            self.backend_generation = 0;
            self.backend = None;
            return self.close_for_invariant();
        }
        // The pointer swap is the successful activation boundary. Candidate
        // counters remain unregistered (and unbillable) until it completes.
        let Some(previous) = self.backend.replace(candidate) else {
            self.backend = None;
            return self.close_for_invariant();
        };
        // The candidate's counters already hold its authentication/restore
        // bytes: attribution starts at the activation point, so only command
        // bytes written from here on are ever settled.
        self.attributed_outbound = self
            .backend
            .as_ref()
            .map_or(0, |backend| backend.counters.outbound());
        if let Some(registry) = &self.metering {
            let Some(current) = self.backend.as_ref() else {
                registry.fail_closed();
                return self.close_for_invariant();
            };
            let result = registry.register(
                MeteringAttribution {
                    connection_id: self.connection_id,
                    backend_generation: next_generation,
                    backend_id: current.id.clone(),
                    cluster_name: current.cluster.clone(),
                    keyspace: current.keyspace.clone(),
                    local: current.local,
                    public_endpoint: self.public_endpoint,
                },
                Arc::clone(&current.counters),
            );
            if result.is_err() {
                registry.fail_closed();
                self.retired_backend_in = self.retired_backend_in.saturating_add(previous_in);
                self.retired_backend_out = self.retired_backend_out.saturating_add(previous_out);
                self.backend_generation = 0;
                return self.close_for_invariant();
            }
        }
        self.backend_generation = next_generation;
        self.retired_backend_in = self.retired_backend_in.saturating_add(previous_in);
        self.retired_backend_out = self.retired_backend_out.saturating_add(previous_out);
        self.redirect_target = None;
        // Dropping the previous sole owner closes it only after the restored
        // candidate has been installed atomically.
        drop(previous);
        // The candidate was admitted healthy, but topology may have flipped
        // while its handshake/restore ran. Reconcile immediately after the
        // atomic owner swap rather than waiting for the periodic retry.
        self.refresh_backend_keepalive();
        Awaited::Got
    }

    fn close_for_invariant(&mut self) -> Awaited {
        self.closing = true;
        self.wire_end = Some(WireErrorSource::Proxy);
        Awaited::Closing
    }

    async fn handle_redirect_snapshot(&mut self) {
        // MIG-00 binds the bounded snapshot query to the production socket
        // owner at the FSM safe boundary. MIG-01 consumes the validated
        // token/state only inside this task, builds a fully restored candidate,
        // and exposes it to the FSM only after the restore OK is consumed.
        match self.capture_migration_snapshot().await {
            Ok(snapshot) => {
                if let Some(state) = self.cmd_state.as_mut() {
                    state.replace_current_database_from_snapshot(snapshot.current_database());
                }
                let Some(target) = self.redirect_target.clone() else {
                    let _ = self.events.send(SessionEvent::RedirectBackendFailed).await;
                    return;
                };
                let budget = candidate_budget(target.deadline_unix_millis);
                if budget.is_zero() {
                    // Do not poll the connect future even once after an
                    // absolute deadline has expired: polling may already
                    // initiate target-side I/O before `timeout(0, ..)` wins.
                    let _ = self.events.send(SessionEvent::RedirectBackendFailed).await;
                    return;
                }
                let candidate = tokio::time::timeout(
                    budget,
                    self.establish_migration_candidate(&target, &snapshot),
                )
                .await;
                match candidate {
                    Ok(Ok(candidate)) => {
                        self.candidate = Some(candidate);
                        let _ = self.events.send(SessionEvent::RedirectBackendReady).await;
                    }
                    Ok(Err(_)) | Err(_) => {
                        // The candidate is local to the future and is dropped
                        // on every error/cancellation. The old backend remains
                        // the sole visible owner and stays sequence-aligned.
                        let _ = self.events.send(SessionEvent::RedirectBackendFailed).await;
                    }
                }
            }
            Err(SnapshotFailure::OldBackendUsable) => {
                let _ = self.events.send(SessionEvent::RedirectBackendFailed).await;
            }
            Err(SnapshotFailure::BackendNetwork) => {
                self.wire_end = Some(WireErrorSource::BackendNetwork);
                self.quit_source = QuitSource::BackendNetwork;
                let _ = self.events.send(SessionEvent::BackendIoError).await;
            }
            Err(SnapshotFailure::Desynchronized) => {
                self.wire_end = Some(WireErrorSource::Proxy);
                self.quit_source = QuitSource::ProxyMalformed;
                let _ = self.events.send(SessionEvent::BackendIoError).await;
            }
            Err(SnapshotFailure::ProxyInvariant) => {
                self.wire_end = Some(WireErrorSource::Proxy);
                self.quit_source = QuitSource::ProxyError;
                let _ = self.events.send(SessionEvent::BackendIoError).await;
            }
        }
    }

    /// Runs the single allowlisted MIG-00 query on the attached old backend.
    ///
    /// The engine owns both backend halves, and the FSM emits this effect only
    /// in `RedirectPending`, so no user command can share the exchange. A
    /// parser error after a complete terminator is recoverable; any earlier
    /// error poisons the connection instead of risking sequence confusion.
    async fn capture_migration_snapshot(
        &mut self,
    ) -> Result<SessionStateSnapshot, SnapshotFailure> {
        let limits = InternalLimits::default();
        let query = InternalQuery::ShowSessionStates;
        let request = query
            .encode(limits)
            .map_err(|_| SnapshotFailure::ProxyInvariant)?;
        let mut parser = query
            .parser(self.negotiated, limits)
            .map_err(|_| SnapshotFailure::ProxyInvariant)?;
        let Some(backend) = self.backend.as_mut() else {
            return Err(SnapshotFailure::ProxyInvariant);
        };

        // This proxy-owned `SHOW SESSION_STATES` is a fresh command exchange, so
        // on a compressed backend it must start from compressed sequence zero —
        // Go's `cmd_processor_query.go` calls `ResetSequence()` before every
        // proxy-owned query. The shared `send_proxy_owned_query` seam performs
        // that layered + packet reset and the send; the migration-snapshot
        // regression exercises the same helper.
        send_proxy_owned_query(&mut backend.backend_io, &request)
            .await
            .map_err(|error| match error {
                ProxyOwnedQueryError::LayeredReset => SnapshotFailure::ProxyInvariant,
                ProxyOwnedQueryError::Send => SnapshotFailure::BackendNetwork,
            })?;

        loop {
            let payload = backend
                .backend_io
                .read_logical(limits.max_result_bytes)
                .await
                .map_err(|_| SnapshotFailure::BackendNetwork)?
                .payload;
            match parser.consume(&payload) {
                Ok(InternalProgress::Continue) => {}
                Ok(InternalProgress::Complete(InternalResult::SessionStates(snapshot))) => {
                    return Ok(snapshot);
                }
                Ok(InternalProgress::Complete(InternalResult::Ok(_))) => {
                    return Err(SnapshotFailure::Desynchronized);
                }
                Err(_) if parser.state() == InternalParserState::Complete => {
                    return Err(SnapshotFailure::OldBackendUsable);
                }
                Err(_) => return Err(SnapshotFailure::Desynchronized),
            }
        }
    }

    /// Dials, authenticates, and restores one redirect target without making
    /// it visible to the command path. The caller owns the overall timeout;
    /// cancellation drops the local socket and every secret-bearing buffer.
    async fn establish_migration_candidate(
        &mut self,
        target: &RedirectTarget,
        snapshot: &SessionStateSnapshot,
    ) -> Result<BackendIo, CandidateFailure> {
        if target.backend_id.is_empty()
            || target.backend_address.is_empty()
            || (self.metering.is_some() && target.keyspace.is_empty())
            || !target.backend_healthy
        {
            return Err(CandidateFailure::InvalidTarget);
        }
        let stream = tokio::net::TcpStream::connect(&target.backend_address)
            .await
            .map_err(|_| {
                self.metrics.try_record(Observation::DialBackendFailed {
                    backend: target.backend_address.clone(),
                });
                CandidateFailure::Dial
            })?;
        let mut backend_socket = CountedIo::new(stream);
        let counters = backend_socket.counters();
        if self.proxy_protocol_v2_enabled() {
            write_backend_proxy_v2_header(
                &mut backend_socket,
                self.inbound_proxy_header.as_ref(),
                self.endpoints.client_addr,
            )
            .await
            .map_err(|_| CandidateFailure::Dial)?;
        }
        let mut candidate = BackendIo {
            backend_io: PacketIo::with_read_stream_buffer_size(
                BackendTransport::Plain(backend_socket),
                self.stream_buffer_size,
            ),
            counters,
            id: target.backend_id.clone(),
            address: target.backend_address.clone(),
            cluster: target.cluster_name.clone(),
            keyspace: target.keyspace.clone(),
            local: target.backend_local,
            healthy: target.backend_healthy,
            keepalive_applied: false,
        };
        // Same read-side over-read buffering as the primary connect path.
        candidate.backend_io.enable_read_buffering();
        let greeting = candidate
            .backend_io
            .read_logical(HANDSHAKE_PAYLOAD_LIMIT)
            .await
            .map_err(|_| CandidateFailure::Handshake)?;
        let backend_greeting = mysql_wire::parse_initial_handshake(&greeting.payload)
            .map_err(|_| CandidateFailure::Handshake)?;
        let backend_caps = backend_greeting.capabilities;
        let (require_backend_tls, backend_tls_available) = self.backend_tls_policy();
        verify_backend(
            backend_caps,
            self.negotiated,
            proxy_capabilities(self.frontend_tls_available()),
            require_backend_tls,
        )
        .map_err(|_| CandidateFailure::Handshake)?;

        let identity = self
            .session_identity
            .as_ref()
            .ok_or(CandidateFailure::Handshake)?;
        let plan = plan_backend_migration_handshake(
            self.negotiated,
            identity.attributes().is_some(),
            backend_caps,
            require_backend_tls,
            backend_tls_available,
        )
        .map_err(|_| CandidateFailure::Handshake)?;

        if matches!(plan.tls, BackendTlsMode::Enabled) {
            self.upgrade_backend_tls(
                &mut candidate,
                plan.capabilities,
                candidate_budget(target.deadline_unix_millis),
            )
            .await
            .map_err(|_| CandidateFailure::Handshake)?;
        }

        let capabilities = self
            .authenticate_migration_candidate(
                &mut candidate,
                snapshot,
                plan.capabilities,
                backend_caps,
            )
            .await?;
        self.restore_candidate_state(&mut candidate, snapshot.session_states(), capabilities)
            .await?;
        self.apply_candidate_keepalive(&mut candidate)?;
        Ok(candidate)
    }

    /// Applies the healthy-target policy after every transport upgrade, through
    /// the preserved innermost raw socket.
    fn apply_candidate_keepalive(&self, candidate: &mut BackendIo) -> Result<(), CandidateFailure> {
        let Some(policy) = self
            .seat
            .snapshot()
            .raw()
            .config
            .as_ref()
            .and_then(|config| config.healthy_backend_keepalive)
        else {
            candidate.keepalive_applied = true;
            return Ok(());
        };
        let Some(counted) = candidate.backend_io.get_ref().as_counted_stream() else {
            return Err(CandidateFailure::Handshake);
        };
        candidate.keepalive_applied = proxy_io::socket::apply_keepalive(
            counted.get_ref(),
            crate::server::snapshot_keepalive(&policy),
        )
        .is_ok();
        Ok(())
    }

    /// Sends the fixed session-token handshake and consumes its sole terminal
    /// response. Returns the exact capability mask governing the restored
    /// command channel.
    async fn authenticate_migration_candidate(
        &self,
        candidate: &mut BackendIo,
        snapshot: &SessionStateSnapshot,
        planned_capabilities: CapabilityFlags,
        backend_caps: CapabilityFlags,
    ) -> Result<CapabilityFlags, CandidateFailure> {
        // Go's second handshake uses the signed token as auth data under the
        // fixed `tidb_session_token` plugin. The authoritative current-db from
        // SHOW SESSION_STATES replaces (and may clear) the original database.
        let parsed = parse_handshake_response(&self.client_handshake_raw)
            .map_err(|_| CandidateFailure::Handshake)?;
        let identity = self
            .session_identity
            .as_ref()
            .ok_or(CandidateFailure::Handshake)?;
        let attributes = identity.attributes().map(|attributes| {
            attributes
                .iter()
                .map(|(key, value)| Attribute {
                    key: key.as_slice(),
                    value: value.as_slice(),
                })
                .collect::<Vec<_>>()
        });
        let database = snapshot.current_database().map(str::as_bytes);
        let capabilities = migration_auth_capabilities(
            planned_capabilities,
            backend_caps,
            database.is_some(),
            snapshot.session_token().len(),
        )?;
        let response = encode_handshake_response(HandshakeResponseParams {
            capabilities,
            max_packet_size: parsed.max_packet_size,
            collation: parsed.collation,
            username: identity.username(),
            auth_response: snapshot.session_token().as_bytes(),
            database,
            auth_plugin_name: Some(b"tidb_session_token"),
            attributes: attributes.as_deref(),
            zstd_level: parsed.zstd_level,
        })
        .map_err(|_| CandidateFailure::Handshake)?;
        let response = SensitiveBytes::new(response);
        if !matches!(candidate.backend_io.get_ref(), BackendTransport::Tls(_)) {
            let next = candidate.backend_io.expected_read_sequence();
            candidate.backend_io.reset_write_sequence(next);
        }
        candidate
            .backend_io
            .write_logical(&response.0, true)
            .await
            .map_err(|_| CandidateFailure::Handshake)?;
        candidate
            .backend_io
            .reset_read_sequence(candidate.backend_io.next_write_sequence());
        let auth_result = candidate
            .backend_io
            .read_logical(HANDSHAKE_PAYLOAD_LIMIT)
            .await
            .map_err(|_| CandidateFailure::Handshake)?;
        match classify_backend_auth_packet(&auth_result.payload, capabilities) {
            Ok(AuthEvent::BackendOk) => {}
            Ok(AuthEvent::BackendError { .. }) => {
                return Err(CandidateFailure::Authentication);
            }
            Ok(_) | Err(_) => return Err(CandidateFailure::Handshake),
        }

        // The auth OK is the exact MySQL boundary where the backend leg switches
        // to compressed framing. The restore query that follows must therefore
        // run through the negotiated codec, independently of the client leg.
        if let Some(algorithm) = selection_to_compression_algorithm(compression_selection(
            capabilities,
            parsed.zstd_level.unwrap_or(0),
        )) {
            Self::activate_candidate_backend_compression(candidate, algorithm)?;
        }
        Ok(capabilities)
    }

    /// Activates compression on a fully authenticated migration candidate
    /// without exposing it to the command path. This mirrors the normal backend
    /// auth-OK activation seam, but maps invariant failures to the candidate-only
    /// rollback path so the old backend remains usable.
    fn activate_candidate_backend_compression(
        candidate: &mut BackendIo,
        algorithm: CompressionAlgorithm,
    ) -> Result<(), CandidateFailure> {
        let endpoint = std::mem::replace(
            &mut candidate.backend_io,
            PacketIo::new(BackendTransport::Detached),
        );
        let (transport, upgrade_state, prefix) = endpoint.into_upgrade_parts();
        if !prefix.is_empty()
            || matches!(
                &transport,
                BackendTransport::Detached | BackendTransport::Compressed(_)
            )
        {
            return Err(CandidateFailure::Handshake);
        }
        let compressed = CompressedIo::new(transport, algorithm, CompressionLimits::default())
            .map_err(|_| CandidateFailure::Handshake)?;
        candidate.backend_io = PacketIo::from_upgrade_parts(
            BackendTransport::Compressed(Box::new(compressed)),
            upgrade_state,
        );
        Ok(())
    }

    /// Restores the exact escaped state and consumes a complete OK before the
    /// candidate can become visible. Any error drops only the candidate.
    async fn restore_candidate_state(
        &self,
        candidate: &mut BackendIo,
        session_states: &str,
        capabilities: CapabilityFlags,
    ) -> Result<(), CandidateFailure> {
        let limits = InternalLimits::default();
        let query = InternalQuery::SetSessionStates(session_states);
        let request = SensitiveBytes::new(
            query
                .encode(limits)
                .map_err(|_| CandidateFailure::Restore)?,
        );
        let mut parser = query
            .parser(capabilities, limits)
            .map_err(|_| CandidateFailure::Restore)?;
        send_proxy_owned_query(&mut candidate.backend_io, &request.0)
            .await
            .map_err(|_| CandidateFailure::Restore)?;
        loop {
            let payload = candidate
                .backend_io
                .read_logical(limits.max_result_bytes)
                .await
                .map_err(|_| CandidateFailure::Restore)?
                .payload;
            match parser
                .consume(&payload)
                .map_err(|_| CandidateFailure::Restore)?
            {
                InternalProgress::Continue => {}
                InternalProgress::Complete(InternalResult::Ok(_)) => return Ok(()),
                InternalProgress::Complete(InternalResult::SessionStates(_)) => {
                    return Err(CandidateFailure::Restore);
                }
            }
        }
    }

    /// Applies the plan's session and prepared mutations at their
    /// declared boundary. A prepared-registry change returns its SES-00
    /// synchronization event, which the caller MUST deliver before the
    /// command-completion boundary so a queued drain or redirect never
    /// crosses an unfinished long-data/cursor guard.
    fn apply_command_mutations(
        &mut self,
        pending: &PendingCommand,
        success_stage: bool,
    ) -> Option<SessionEvent> {
        let state = self.cmd_state.as_mut()?;
        // Re-derive the plan's mutations from the held payload: the
        // borrowed plan cannot outlive its packet, so mutations are
        // re-computed at their application point.
        let Ok(packet) = CommandPacket::decode(&pending.payload) else {
            return None;
        };
        let Ok(plan) = dispatch(packet) else {
            return None;
        };
        let effects: CommandStateEffects<'_> = if success_stage {
            plan.after_success
        } else {
            plan.after_forward
        };
        if let Some(mutation) = effects.session {
            state.apply(mutation);
            if matches!(mutation, SessionMutation::MarkQuit) {
                self.wire_end.get_or_insert(WireErrorSource::ClientNetwork);
            }
        }
        effects.prepared.map(|mutation| {
            self.prepared.apply_mutation(mutation);
            self.prepared.session_event()
        })
    }

    async fn run_auth_effects(&mut self, effects: &[AuthEffect]) -> Result<(), WireErrorSource> {
        for effect in effects {
            match effect {
                AuthEffect::ForwardBackendToClient => {
                    let Some(payload) = self.relay_hold.take() else {
                        return Err(WireErrorSource::Proxy);
                    };
                    // Connection-phase relay: both directions continue
                    // the one counter per channel in lockstep.
                    let seq = self.client_io.expected_read_sequence();
                    self.client_io.reset_write_sequence(seq);
                    let write = self.client_io.write_logical(&payload, true).await;
                    if let Err(error) = write {
                        let source =
                            classify_packet_io(&error, SideMarker::Client, SideMarker::Client);
                        let _ = self.events.send(SessionEvent::ClientIoError).await;
                        return Err(self.end_source(source));
                    }
                }
                AuthEffect::ForwardClientToBackend => {
                    let Some(payload) = self.relay_hold.take() else {
                        return Err(WireErrorSource::Proxy);
                    };
                    let Some(backend) = self.backend.as_mut() else {
                        return Err(WireErrorSource::Proxy);
                    };
                    let seq = backend.backend_io.expected_read_sequence();
                    backend.backend_io.reset_write_sequence(seq);
                    if backend
                        .backend_io
                        .write_logical(&payload, true)
                        .await
                        .is_err()
                    {
                        let _ = self.events.send(SessionEvent::BackendIoError).await;
                        return Err(WireErrorSource::BackendNetwork);
                    }
                }
                AuthEffect::ActivateClientCompression(selection) => {
                    // At the auth-OK boundary Go calls setCompress on the client
                    // leg; a `None` selection is a no-op, otherwise wrap the
                    // client transport in compressed framing (WIRE-C).
                    if let Some(algorithm) = selection_to_compression_algorithm(*selection) {
                        self.activate_client_compression(algorithm).await?;
                    }
                }
                AuthEffect::ActivateBackendCompression(selection) => {
                    // The backend leg negotiates independently (client caps
                    // masked by backend caps), so it may pick a different
                    // algorithm or none.
                    if let Some(algorithm) = selection_to_compression_algorithm(*selection) {
                        self.activate_backend_compression(algorithm).await?;
                    }
                }
                AuthEffect::ReconnectBackend => {
                    // Reconnect (session migration) is never approved in this
                    // slice.
                    return Err(WireErrorSource::Proxy);
                }
            }
        }
        Ok(())
    }

    /// Whether this session's snapshot carries a frontend TLS server config.
    /// Governs both the greeting `SSL` advertisement and the strict
    /// SSLRequest-vs-plaintext classification: a client may only upgrade if we
    /// actually advertised (and can serve) TLS.
    fn frontend_tls_available(&self) -> bool {
        self.seat.snapshot().frontend_server_config.is_some()
    }

    /// Remaining handshake budget from the absolute deadline (`accepted_at +
    /// handshake_deadline`), for TLS accept/connect. Saturates at zero so a
    /// blown budget fails closed immediately instead of granting a fresh wait.
    fn handshake_budget_remaining(&self) -> Duration {
        (self.accepted_at + self.handshake_deadline)
            .saturating_duration_since(tokio::time::Instant::now())
    }

    /// Upgrades the client transport to server-side TLS in place, preserving the
    /// `MySQL` sequence trackers and framing counters across the swap. The raw
    /// TLS handshake bytes are not `MySQL` packets, so they never enter the
    /// `PacketIo` framing counters — but they DO cross the wire through the
    /// innermost `CountedIo`, so they count toward the raw traffic totals, since
    /// the upgrade wraps that same counted socket rather than replacing it.
    ///
    /// The `SSLRequest`'s prefetched-but-unread bytes, if any, replay ahead of
    /// the TLS stream so no `ClientHello` byte is lost. Any failure — missing
    /// config, timeout, or handshake error — fails closed: the owner drops the
    /// socket (moved into `accept_frontend`, dropped on error), there is no
    /// plaintext fallback and no detached task.
    async fn activate_frontend_tls(&mut self) -> Result<(), WireErrorSource> {
        let Some(config) = self.seat.snapshot().frontend_server_config.clone() else {
            // Reached only if the snapshot lost its config between advertisement
            // and here; a client SSLRequest we cannot serve fails closed.
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::Proxy);
        };
        let timeout = self.handshake_budget_remaining();
        // Move the endpoint out to perform the value-consuming upgrade; the
        // sequence/counter state travels in the token, the raw socket in the
        // transport. `Detached` holds the field meanwhile and is never polled.
        let endpoint = std::mem::replace(
            &mut self.client_io,
            PacketIo::new(ClientTransport::Detached),
        );
        let (transport, upgrade_state, prefix) = endpoint.into_upgrade_parts();
        let ClientTransport::Plain(stream) = transport else {
            // A second activation on an already-upgraded transport is a proxy
            // invariant violation.
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::Proxy);
        };
        // `stream` was moved into `accept_frontend`; on error it is dropped —
        // the owner drops the socket. No plaintext fallback, no detached task.
        let Ok(frontend) =
            accept_frontend(stream, prefix, config, timeout, DEFAULT_CONN_BUFFER_SIZE).await
        else {
            self.quit_source = QuitSource::ClientHandshake;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::ClientNetwork);
        };
        self.client_io =
            PacketIo::from_upgrade_parts(ClientTransport::Tls(frontend), upgrade_state);
        self.frontend_tls_active = true;
        Ok(())
    }

    /// Wraps the client transport in `MySQL` compressed framing in place after
    /// authentication. Compression is the OUTERMOST transport layer (above any
    /// TLS), so it wraps the whole client transport, preserving the packet
    /// sequence trackers and framing counters across the swap. Activation lands
    /// at a clean command boundary (the auth-OK packet was just forwarded), so
    /// the prefetch prefix must be empty; a non-empty prefix, an already-wrapped
    /// transport, or a codec/config error fails closed.
    async fn activate_client_compression(
        &mut self,
        algorithm: CompressionAlgorithm,
    ) -> Result<(), WireErrorSource> {
        let endpoint = std::mem::replace(
            &mut self.client_io,
            PacketIo::new(ClientTransport::Detached),
        );
        let (transport, upgrade_state, prefix) = endpoint.into_upgrade_parts();
        if !prefix.is_empty()
            || matches!(
                &transport,
                ClientTransport::Detached | ClientTransport::Compressed(_)
            )
        {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::Proxy);
        }
        let Ok(compressed) = CompressedIo::new(transport, algorithm, CompressionLimits::default())
        else {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::Proxy);
        };
        self.client_io = PacketIo::from_upgrade_parts(
            ClientTransport::Compressed(Box::new(compressed)),
            upgrade_state,
        );
        Ok(())
    }

    /// Wraps the backend transport in `MySQL` compressed framing in place, as
    /// [`Self::activate_client_compression`] does for the client leg. The
    /// backend leg negotiates independently, so its algorithm may differ.
    async fn activate_backend_compression(
        &mut self,
        algorithm: CompressionAlgorithm,
    ) -> Result<(), WireErrorSource> {
        let Some(backend) = self.backend.as_mut() else {
            return Err(WireErrorSource::Proxy);
        };
        let endpoint = std::mem::replace(
            &mut backend.backend_io,
            PacketIo::new(BackendTransport::Detached),
        );
        let (transport, upgrade_state, prefix) = endpoint.into_upgrade_parts();
        if !prefix.is_empty()
            || matches!(
                &transport,
                BackendTransport::Detached | BackendTransport::Compressed(_)
            )
        {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::Proxy);
        }
        let Ok(compressed) = CompressedIo::new(transport, algorithm, CompressionLimits::default())
        else {
            self.quit_source = QuitSource::ProxyError;
            let _ = self.events.send(SessionEvent::BackendIoError).await;
            return Err(WireErrorSource::Proxy);
        };
        let Some(backend) = self.backend.as_mut() else {
            return Err(WireErrorSource::Proxy);
        };
        backend.backend_io = PacketIo::from_upgrade_parts(
            BackendTransport::Compressed(Box::new(compressed)),
            upgrade_state,
        );
        Ok(())
    }

    /// The backend TLS policy for this session as `(require, available)`. A
    /// validated snapshot always carries a default backend policy object, so
    /// presence is read from the raw config's `backend_tls` rather than the
    /// validated policy's emptiness.
    fn backend_tls_policy(&self) -> (bool, bool) {
        let snapshot = self.seat.snapshot();
        let config = snapshot.raw().config.as_ref();
        let require = config.is_some_and(|config| config.require_backend_tls);
        let available = config.is_some_and(|config| config.backend_tls.is_some());
        (require, available)
    }

    /// Whether this session's snapshot enables the PROXY protocol v2 preamble
    /// on the backend dial (Go `proxy-protocol = "v2"`). The snapshot validator
    /// only admits `disabled`/`v2`, so any other value is treated as disabled.
    fn proxy_protocol_v2_enabled(&self) -> bool {
        self.seat
            .snapshot()
            .raw()
            .config
            .as_ref()
            .is_some_and(|config| {
                matches!(
                    ProxyProtocolMode::try_from(config.proxy_protocol),
                    Ok(ProxyProtocolMode::V2)
                )
            })
    }

    /// Upgrades the backend transport to client-side TLS in place. Sends the
    /// plaintext `SSLRequest` (seq 1, continuing after the backend greeting),
    /// runs the backend TLS handshake, then reattaches preserving the sequence
    /// trackers and framing counters (the raw TLS handshake bytes are not
    /// `MySQL` packets, so they skip the `PacketIo` framing counters, but they
    /// still count on the innermost `CountedIo` raw traffic totals); the full
    /// handshake response then travels inside TLS at seq 2.
    ///
    /// Any failure — config build, handshake, timeout, or the backend speaking
    /// before TLS — fails closed: the owner drops the socket (moved into
    /// `connect_backend`), no plaintext fallback, no detached task.
    async fn upgrade_backend_tls(
        &self,
        backend: &mut BackendIo,
        capabilities: CapabilityFlags,
        timeout: Duration,
    ) -> Result<(), WireErrorSource> {
        // The SSLRequest mirrors the client's max packet size and collation —
        // the same values the forwarded handshake response carries.
        let Ok(client) = parse_handshake_response(&self.client_handshake_raw) else {
            return Err(WireErrorSource::Proxy);
        };
        // Align the writer to continue after the greeting (reader observed seq
        // 0 -> expects 1) and send the plaintext SSLRequest at seq 1.
        let next = backend.backend_io.expected_read_sequence();
        backend.backend_io.reset_write_sequence(next);
        let ssl_request =
            encode_ssl_request(capabilities, client.max_packet_size, client.collation);
        if backend
            .backend_io
            .write_logical(&ssl_request, true)
            .await
            .is_err()
        {
            return Err(WireErrorSource::BackendNetwork);
        }
        let Ok(config) = build_backend_config(&self.seat.snapshot().backend_tls) else {
            return Err(WireErrorSource::Proxy);
        };
        let server_name = backend_server_name(&backend.address);
        // Move the endpoint out for the value-consuming upgrade; the
        // sequence/counter state travels in the token, the raw socket in the
        // transport. `Detached` holds the field meanwhile and is never polled.
        let io = std::mem::replace(
            &mut backend.backend_io,
            PacketIo::new(BackendTransport::Detached),
        );
        let (transport, upgrade_state, prefix) = io.into_upgrade_parts();
        let BackendTransport::Plain(stream) = transport else {
            return Err(WireErrorSource::Proxy);
        };
        if !prefix.is_empty() {
            // The backend sent bytes before TLS started (it must not); fail
            // closed rather than dropping them or feeding them into TLS.
            return Err(WireErrorSource::BackendNetwork);
        }
        let Ok(backend_tls) = connect_backend(
            stream,
            &server_name,
            config,
            timeout,
            DEFAULT_CONN_BUFFER_SIZE,
        )
        .await
        else {
            return Err(WireErrorSource::BackendNetwork);
        };
        backend.backend_io =
            PacketIo::from_upgrade_parts(BackendTransport::Tls(backend_tls), upgrade_state);
        Ok(())
    }

    async fn send_greeting(&mut self) -> Result<(), WireErrorSource> {
        fill_salt(&mut self.salt);
        // Rust-owner mode reads the current default router's retained backend
        // version. Before the first successful health publication (or when the
        // default namespace is absent), use the static Go/pnet-compatible value
        // retained in the serving snapshot; the historical Rust constant is
        // only the compatibility-test fallback.
        let server_version = self
            .route
            .as_ref()
            .and_then(|seed| seed.route_plane.as_ref())
            .and_then(RoutePlaneHandle::default_server_version)
            .or_else(|| {
                self.seat
                    .snapshot()
                    .raw()
                    .config
                    .as_ref()
                    .map(|config| config.server_version.trim().to_owned())
                    .filter(|version| !version.is_empty())
            });
        let params = build_greeting(
            proxy_capabilities(self.frontend_tls_available()),
            &self.salt,
            server_version
                .as_deref()
                .map_or(SERVER_VERSION, str::as_bytes),
            self.connection_id,
            45,
            StatusFlags::from_bits_retain(0),
        );
        let Ok(encoded) = encode_initial_handshake(params) else {
            return Err(WireErrorSource::Proxy);
        };
        let write = self.client_io.write_logical(&encoded, true).await;
        if let Err(error) = write {
            return Err(self.end_source(classify_packet_io(
                &error,
                SideMarker::Client,
                SideMarker::Client,
            )));
        }
        Ok(())
    }

    /// Reads the one client packet whose declared length is trusted only after
    /// the Go-compatible pre-handshake gate. Peeking is essential here: a
    /// hostile peer may advertise an oversized payload and send only the first
    /// byte needed by the protocol peek, so routing the packet through the
    /// generic draining read would hold the session until its deadline instead
    /// of rejecting from that bounded prefix.
    async fn read_client_handshake_packet(&mut self) -> Result<Vec<u8>, WireErrorSource> {
        let preview = match self.client_io.peek_packet().await {
            Ok(preview) => preview,
            Err(error) => return Err(self.client_read_end(&error).await),
        };
        let declared = usize::try_from(preview.first_packet_length).unwrap_or(usize::MAX);
        if check_handshake_packet_size(declared).is_err() {
            self.quit_source = QuitSource::ClientHandshake;
            self.write_handshake_failure(FailureKind::PacketTooLarge, false)
                .await;
            let _ = self.events.send(SessionEvent::ClientIoError).await;
            return Err(WireErrorSource::ClientNetwork);
        }
        match self
            .client_io
            .read_logical(mysql_wire::limits::MAX_PRE_HANDSHAKE_PACKET_LEN)
            .await
        {
            Ok(packet) => Ok(packet.payload),
            Err(error) => Err(self.client_read_end(&error).await),
        }
    }

    async fn write_client_error(
        &mut self,
        code: u16,
        state: [u8; 5],
        message: &str,
    ) -> Result<(), WireErrorSource> {
        self.write_client_error_with_capabilities(code, state, message, self.negotiated)
            .await
    }

    async fn write_client_error_with_capabilities(
        &mut self,
        code: u16,
        state: [u8; 5],
        message: &str,
        capabilities: CapabilityFlags,
    ) -> Result<(), WireErrorSource> {
        let Ok(encoded) = encode_error_packet(code, Some(state), message.as_bytes(), capabilities)
        else {
            return Err(WireErrorSource::Proxy);
        };
        let write = self.client_io.write_logical(&encoded, true).await;
        if let Err(error) = write {
            return Err(self.end_source(classify_packet_io(
                &error,
                SideMarker::Client,
                SideMarker::Client,
            )));
        }
        Ok(())
    }

    /// Applies Go's `ErrToClient` allowlist to one typed handshake failure.
    ///
    /// Classification is deliberately owned by the caller: Go may wrap the
    /// same client-visible sentinel around either a handshake rejection or a
    /// side-attributed transport break, and disconnect precedence must still
    /// decide the final source label. This helper only emits the approved
    /// fixed response (if any); it never derives a message from runtime detail.
    async fn write_handshake_failure(&mut self, kind: FailureKind, mysql_error: bool) {
        let descriptor = FailureDescriptor {
            kind: Some(kind),
            mysql_error,
            ..FailureDescriptor::default()
        };
        let Some(response) = client_response(&descriptor) else {
            return;
        };
        let (code, state, message) = match response {
            ClientResponse::Fixed(response) => {
                (response.code, response.sql_state, response.message)
            }
            ClientResponse::Approved(response) => {
                (response.code(), response.sql_state(), response.message())
            }
        };
        let seq = self.client_io.expected_read_sequence();
        self.client_io.reset_write_sequence(seq);
        let capabilities = self.negotiated.union(CapabilityFlags::PROTOCOL_41);
        let _ = self
            .write_client_error_with_capabilities(code, state, message, capabilities)
            .await;
    }

    /// Records a classified session end from a single [`QuitSource`]: sets the
    /// log observable (`quit_source`; first classification wins, matching the
    /// `run()` finalizer) and returns the wire observable projected from the
    /// SAME winning value via [`wire_source_of`]. Projecting the *stored* winner
    /// — not the just-passed `source` — is essential: if an earlier
    /// classification already set `quit_source`, returning `wire_source_of` of a
    /// later, different `source` would re-open the A/B divergence (first-wins
    /// log + last-call wire). Both observables therefore always derive from one
    /// classification.
    fn end_source(&mut self, source: QuitSource) -> WireErrorSource {
        let (winner, wire) = resolve_end_source(self.quit_source, source);
        self.quit_source = winner;
        wire
    }

    async fn client_read_end(&mut self, error: &PacketIoError) -> WireErrorSource {
        // A client read failure: the client stream is the only stream in this
        // transfer, so both IoSide directions resolve to the client. The clean
        // EOF vs. hard error distinction only selects the telemetry event; the
        // classification (network break, malformed framing, or proxy fault)
        // comes from the descriptor.
        let source = classify_packet_io(error, SideMarker::Client, SideMarker::Client);
        let event = if is_clean_eof(error) {
            SessionEvent::ClientEof
        } else {
            SessionEvent::ClientIoError
        };
        let _ = self.events.send(event).await;
        self.end_source(source)
    }

    async fn backend_read(&mut self, limit: usize) -> Result<Vec<u8>, WireErrorSource> {
        let result = {
            let Some(backend) = self.backend.as_mut() else {
                return Err(WireErrorSource::Proxy);
            };
            backend.backend_io.read_logical(limit).await
        };
        match result {
            Ok(packet) => Ok(packet.payload),
            Err(error) => {
                // A backend read failure: the backend stream is this transfer's
                // only stream, so the error attributes to the backend endpoint
                // (network break) or to the proxy (malformed framing).
                let source = classify_packet_io(&error, SideMarker::Backend, SideMarker::Backend);
                Err(self.end_source(source))
            }
        }
    }

    fn backend_alive(&mut self) -> bool {
        let Some(backend) = self.backend.as_mut() else {
            return false;
        };
        // Over-read/prefetch bytes staged above the transport mean the backend
        // sent data outside a command (or a full response was not drained) —
        // the same "data while idle" condition the raw-socket probe treats as
        // not idle-healthy, so report it before probing the socket.
        if backend.backend_io.has_buffered_read() {
            return false;
        }
        let Some(counted) = backend.backend_io.get_ref().as_counted_stream() else {
            // Detached only during an in-progress TLS upgrade, before the
            // backend is exposed to probes; treat as alive rather than dead.
            return true;
        };
        // The probe reads the raw socket beneath `CountedIo`, but through its
        // count-aware `probe_try_read`, so any consumed byte is accounted on the
        // same seam before the session tears the connection down — Go counts it
        // too, since its liveness `Peek(1)` reads through `basicReadWriter`.
        let mut probe = [0_u8; 1];
        match counted.probe_try_read(&mut probe) {
            // Data outside a command or a clean EOF both mean the backend is not
            // idle-healthy (a consumed byte was already counted).
            Ok(_) => false,
            Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
        }
    }

    async fn shutdown_io(&mut self) {
        let _ = self.client_io.flush().await;
        if let Some(backend) = self.backend.as_mut() {
            let _ = backend.backend_io.flush().await;
        }
    }

    fn backend_traffic(&self) -> BackendTraffic {
        // Bytes from the raw socket counters; packet counts stay from the
        // `PacketIo` framing layer — mirroring Go's split of raw wire bytes vs
        // MySQL physical-packet counts.
        self.backend
            .as_ref()
            .map_or(BackendTraffic::default(), |backend| BackendTraffic {
                inbound_bytes: backend.counters.inbound(),
                inbound_packets: backend.backend_io.in_packets(),
                outbound_bytes: backend.counters.outbound(),
                outbound_packets: backend.backend_io.out_packets(),
            })
    }

    fn record_command(&mut self, pending: &PendingCommand) {
        let current = self.backend_traffic();
        // Outbound bytes start at the later of this command's own baseline
        // and the shared attributed boundary, so bytes a settlement already
        // published (a control flush between intake and completion) are not
        // counted again; everything written up to now is attributed after.
        let outbound_from = pending
            .traffic_before
            .outbound_bytes
            .max(self.attributed_outbound);
        self.attributed_outbound = current.outbound_bytes;
        let traffic = BackendTraffic {
            inbound_bytes: current
                .inbound_bytes
                .saturating_sub(pending.traffic_before.inbound_bytes),
            inbound_packets: current
                .inbound_packets
                .saturating_sub(pending.traffic_before.inbound_packets),
            outbound_bytes: current.outbound_bytes.saturating_sub(outbound_from),
            outbound_packets: current
                .outbound_packets
                .saturating_sub(pending.traffic_before.outbound_packets),
        };
        let Some(backend) = &self.backend else {
            return;
        };
        self.metrics.try_record(Observation::CommandCompleted {
            backend: backend.address.clone(),
            command: pending.command,
            duration: pending.started.elapsed(),
            since_connection: pending.since_connection,
            traffic,
            local: backend.local,
        });
    }
}

/// Maps an auth failure to the wire error source.
const fn failure_source(kind: FailureKind) -> WireErrorSource {
    match kind {
        FailureKind::ClientHandshake
        | FailureKind::ClientCapability
        | FailureKind::PacketTooLarge
        | FailureKind::AuthenticationFailed => WireErrorSource::ClientNetwork,
        FailureKind::BackendHandshake
        | FailureKind::BackendCapability
        | FailureKind::BackendNoTls
        | FailureKind::BackendProxyProtocol
        | FailureKind::NoBackend => WireErrorSource::BackendNetwork,
        FailureKind::ProxyNoTls | FailureKind::ProxyInternal | FailureKind::ControlPlane => {
            WireErrorSource::Proxy
        }
        FailureKind::Shutdown => WireErrorSource::Shutdown,
    }
}

const fn failure_quit_source(kind: FailureKind) -> QuitSource {
    match kind {
        FailureKind::AuthenticationFailed => QuitSource::ClientAuthFail,
        FailureKind::ClientHandshake
        | FailureKind::ClientCapability
        | FailureKind::PacketTooLarge => QuitSource::ClientHandshake,
        FailureKind::BackendHandshake
        | FailureKind::BackendCapability
        | FailureKind::BackendNoTls
        | FailureKind::BackendProxyProtocol => QuitSource::BackendHandshake,
        FailureKind::NoBackend => QuitSource::ProxyNoBackend,
        FailureKind::ProxyNoTls | FailureKind::ProxyInternal | FailureKind::ControlPlane => {
            QuitSource::ProxyError
        }
        FailureKind::Shutdown => QuitSource::ProxyQuit,
    }
}

const fn acquire_quit_source(error: &AcquireError) -> QuitSource {
    match error {
        AcquireError::NoBackend { .. } | AcquireError::ClusterUnsupported { .. } => {
            QuitSource::ProxyNoBackend
        }
        AcquireError::BudgetExhausted {
            last_failure: Some(_),
        } => QuitSource::BackendHandshake,
        AcquireError::BudgetExhausted { last_failure: None }
        | AcquireError::Routing { .. }
        | AcquireError::MalformedAssignment { .. }
        | AcquireError::Channel(_) => QuitSource::ProxyError,
    }
}

const fn coarse_quit_source(source: WireErrorSource) -> QuitSource {
    match source {
        WireErrorSource::ClientNetwork => QuitSource::ClientNetwork,
        WireErrorSource::BackendNetwork => QuitSource::BackendNetwork,
        WireErrorSource::Shutdown => QuitSource::ProxyQuit,
        WireErrorSource::BackendSql => QuitSource::ClientSqlError,
        WireErrorSource::Proxy | WireErrorSource::Control => QuitSource::ProxyError,
        WireErrorSource::Unspecified => QuitSource::None,
    }
}

/// Projects the Go-parity 11-variant [`QuitSource`] classification onto the
/// coarser 7-variant wire [`WireErrorSource`]. This is the SINGLE forward
/// mapping used by every classified failure site, so the log observable
/// (`quit_source`) and the wire observable (`ConnectionEvent.error_source`)
/// are two projections of ONE classification — eliminating the pre-fix A/B
/// divergence where the two were assigned independently. The per-kind pairs
/// match Go's `Error2Source` grouping used by [`failure_source`] /
/// [`failure_quit_source`]; malformed and non-disconnect proxy faults resolve
/// to the proxy component rather than being mis-attributed as a network break.
const fn wire_source_of(source: QuitSource) -> WireErrorSource {
    match source {
        QuitSource::None => WireErrorSource::Unspecified,
        // Client-side handshake/capability/auth failures collapse to the
        // client-network wire component, matching the existing kind table.
        QuitSource::ClientNetwork | QuitSource::ClientHandshake | QuitSource::ClientAuthFail => {
            WireErrorSource::ClientNetwork
        }
        QuitSource::ClientSqlError => WireErrorSource::BackendSql,
        QuitSource::ProxyQuit => WireErrorSource::Shutdown,
        // Malformed packets/sequence violations and other proxy-internal
        // faults are proxy bugs (Go: "we assume clients and TiDB are right"),
        // NOT network breaks.
        QuitSource::ProxyMalformed | QuitSource::ProxyError => WireErrorSource::Proxy,
        QuitSource::ProxyNoBackend | QuitSource::BackendNetwork | QuitSource::BackendHandshake => {
            WireErrorSource::BackendNetwork
        }
    }
}

/// Resolves the winning end classification and its wire projection from the
/// already-stored `quit_source` and a newly-classified `source`. First
/// classification wins for the log observable; the wire observable is the
/// projection of that SAME winner, so the two never diverge regardless of which
/// site called last. Split out from [`SessionEngine::end_source`] purely so the
/// winner-projection invariant is unit-testable without an engine instance.
const fn resolve_end_source(
    stored: QuitSource,
    source: QuitSource,
) -> (QuitSource, WireErrorSource) {
    let winner = match stored {
        QuitSource::None => source,
        _ => stored,
    };
    (winner, wire_source_of(winner))
}

/// Builds the Go-parity [`FailureDescriptor`] for a packet-I/O error observed
/// on a transfer whose read stream belongs to `source_end` and whose write
/// stream belongs to `dest_end`. For a single-stream read helper, pass the same
/// marker for both. The [`IoSide`] carried by [`PacketIoError::Io`] selects
/// which endpoint the failure attributes to — this is what fixes the [`IoSide`]
/// inversion (a destination-write break on a `backend -> client` forward is the
/// client's break, not the backend's).
fn descriptor_for_packet_io(
    error: &PacketIoError,
    source_end: SideMarker,
    dest_end: SideMarker,
) -> FailureDescriptor<'static> {
    match error {
        PacketIoError::Io { side, source, .. } => {
            let endpoint = match side {
                IoSide::Source => source_end,
                IoSide::Destination => dest_end,
            };
            if is_disconnect_io(source) {
                // Disconnect + side dominates phase (Go `Error2Source`): the
                // broken connection's side wins, whichever operation was in
                // flight.
                FailureDescriptor {
                    disconnect: DisconnectState::Attributed(endpoint),
                    ..FailureDescriptor::default()
                }
            } else {
                // A non-disconnect transport fault (InvalidData, WriteZero,
                // Other): a proxy-side error, NOT a network break — Go reaches
                // the `ErrProxyErr` default branch for these.
                FailureDescriptor {
                    kind: Some(FailureKind::ProxyInternal),
                    ..FailureDescriptor::default()
                }
            }
        }
        // Framing decode/encode and compressed-sequence violations are proxy
        // bugs (`SrcProxyMalformed`).
        PacketIoError::Decode(_) | PacketIoError::Encode(_) => FailureDescriptor {
            malformed_or_sequence: true,
            ..FailureDescriptor::default()
        },
        // A bounded materializing read observed an over-limit logical payload.
        // This is the GENERIC `read_logical(limit)` transport cap — backend
        // responses and post-handshake command reads hit it too — NOT Go's
        // handshake-only `ErrPacketTooLarge` sentinel (which Go produces only in
        // the pre-handshake 1 MiB client path). Mapping it to
        // `FailureKind::PacketTooLarge`/ClientHandshake would mis-attribute a
        // backend oversized response to the client and mislabel a command-phase
        // over-limit as a handshake failure. We instead preserve the pre-fix
        // behavior: attribute the over-limit termination to the over-sending
        // endpoint (the read source) as a transport break, so a backend
        // oversized response stays BackendNetwork and a client one stays
        // ClientNetwork. A finer Go `ErrPacketTooLarge`(ClientHandshake)
        // distinction needs handshake-phase context from the call site and is a
        // tracked follow-up (see PR scope note).
        PacketIoError::LogicalPayloadTooLarge { .. } => FailureDescriptor {
            disconnect: DisconnectState::Attributed(source_end),
            ..FailureDescriptor::default()
        },
        // Accounting-invariant violations are proxy-internal faults.
        PacketIoError::CounterOverflow { .. } | PacketIoError::ForwardAlreadyComplete => {
            FailureDescriptor {
                kind: Some(FailureKind::ProxyInternal),
                ..FailureDescriptor::default()
            }
        }
    }
}

/// Classifies a packet-I/O error into the Go-parity [`QuitSource`], attributing
/// disconnects to the endpoint selected by the error's [`IoSide`] and the
/// transfer's `source_end`/`dest_end`.
fn classify_packet_io(
    error: &PacketIoError,
    source_end: SideMarker,
    dest_end: SideMarker,
) -> QuitSource {
    QuitSource::classify(&descriptor_for_packet_io(error, source_end, dest_end))
}

/// Whether a packet read error is a clean peer EOF at a packet
/// boundary.
fn is_clean_eof(error: &PacketIoError) -> bool {
    matches!(
        error,
        PacketIoError::Io { source, .. }
            if source.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

/// Fills the greeting salt from OS entropy.
fn fill_salt(salt: &mut [u8; 20]) {
    let mut buffer = [0_u8; 20];
    if getrandom::getrandom(&mut buffer).is_ok() {
        *salt = buffer;
        // MySQL salts avoid NUL bytes (NUL-terminated on the wire).
        for byte in salt.iter_mut() {
            if *byte == 0 {
                *byte = 1;
            }
        }
    } else {
        // Entropy failure: derive a non-constant fallback rather than a
        // fixed salt.
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.subsec_nanos());
        for (index, byte) in salt.iter_mut().enumerate() {
            *byte = (seed
                .wrapping_add(u32::try_from(index).unwrap_or(0))
                .wrapping_mul(2_654_435_761)
                >> 24) as u8;
            if *byte == 0 {
                *byte = 1;
            }
        }
    }
}

#[cfg(test)]
mod gated_event_routing_tests {
    use super::{EventRx, SessionEvent, SessionEventSource};

    /// The engine's `EventRx` hands its gated channel to the loop, so events
    /// arrive with admission permits (the only fast-path-eligible path).
    #[test]
    fn engine_event_source_routes_to_the_gated_entry() {
        let (_tx, rx) = crate::gate::channel::<SessionEvent>(crate::gate::Gate::new(), 1);
        let source = EventRx { events: rx };
        assert!(
            source.into_gated_event_channel().is_ok(),
            "EventRx must expose a gated channel"
        );
    }

    /// A generic source keeps the default `Err(self)`, so the loop reads it
    /// ungated (pump/plain) and the fast path cannot engage on it.
    #[test]
    fn generic_source_stays_ungated() {
        struct Generic;
        impl SessionEventSource for Generic {
            async fn next_event(&mut self) -> Option<SessionEvent> {
                None
            }
        }
        assert!(
            Generic.into_gated_event_channel().is_err(),
            "a generic source must not expose a gated channel"
        );
    }
}

#[cfg(test)]
mod error_classification_tests {
    use super::{
        DisconnectState, IoSide, PacketIoError, QuitSource, SideMarker, WireErrorSource,
        acquire_quit_source, classify_packet_io, coarse_quit_source, descriptor_for_packet_io,
        resolve_end_source, wire_source_of,
    };
    use mysql_wire::DecodeError;
    use std::io::{Error as IoError, ErrorKind};

    /// The Go `IsDisconnectError` set, portably mapped to `ErrorKind`.
    const DISCONNECT_KINDS: &[ErrorKind] = &[
        ErrorKind::UnexpectedEof,
        ErrorKind::BrokenPipe,
        ErrorKind::ConnectionReset,
        ErrorKind::ConnectionAborted,
        ErrorKind::TimedOut,
    ];

    fn io(side: IoSide, kind: ErrorKind) -> PacketIoError {
        PacketIoError::Io {
            side,
            operation: "test",
            source: IoError::from(kind),
        }
    }

    #[test]
    fn backend_read_disconnect_is_backend_network_on_both_observables() {
        // A backend read: the backend stream is both source and destination, so
        // every disconnect kind is a backend network break — the log observable
        // (quit) and wire observable agree.
        for &kind in DISCONNECT_KINDS {
            let quit = classify_packet_io(
                &io(IoSide::Source, kind),
                SideMarker::Backend,
                SideMarker::Backend,
            );
            assert_eq!(quit, QuitSource::BackendNetwork, "{kind:?}");
            assert_eq!(
                wire_source_of(quit),
                WireErrorSource::BackendNetwork,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn client_read_disconnect_is_client_network_on_both_observables() {
        for &kind in DISCONNECT_KINDS {
            let quit = classify_packet_io(
                &io(IoSide::Source, kind),
                SideMarker::Client,
                SideMarker::Client,
            );
            assert_eq!(quit, QuitSource::ClientNetwork, "{kind:?}");
            assert_eq!(
                wire_source_of(quit),
                WireErrorSource::ClientNetwork,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn destination_write_break_attributes_the_write_endpoint_not_the_read_leg() {
        // THE IoSide inversion regression guard. A `backend -> client` forward
        // (source=Backend, dest=Client) whose DESTINATION write breaks is the
        // CLIENT's network break, never the backend's.
        for &kind in DISCONNECT_KINDS {
            let quit = classify_packet_io(
                &io(IoSide::Destination, kind),
                SideMarker::Backend,
                SideMarker::Client,
            );
            assert_eq!(
                quit,
                QuitSource::ClientNetwork,
                "backend->client dst-write {kind:?} is the client's break"
            );
            assert_eq!(wire_source_of(quit), WireErrorSource::ClientNetwork);
        }
        // Symmetrically, a `client -> backend` forward whose destination
        // (backend) write breaks is the backend's.
        for &kind in DISCONNECT_KINDS {
            let quit = classify_packet_io(
                &io(IoSide::Destination, kind),
                SideMarker::Client,
                SideMarker::Backend,
            );
            assert_eq!(
                quit,
                QuitSource::BackendNetwork,
                "client->backend dst-write {kind:?} is the backend's break"
            );
        }
    }

    #[test]
    fn non_disconnect_io_is_a_proxy_fault_not_a_network_break() {
        // InvalidData / NotFound / Other / WriteZero are NOT connection breaks:
        // they fall through to the proxy-error class, never network.
        for kind in [
            ErrorKind::InvalidData,
            ErrorKind::NotFound,
            ErrorKind::Other,
            ErrorKind::WriteZero,
        ] {
            let error = io(IoSide::Source, kind);
            let quit = classify_packet_io(&error, SideMarker::Backend, SideMarker::Backend);
            assert_eq!(
                quit,
                QuitSource::ProxyError,
                "{kind:?} is not a network break"
            );
            assert_eq!(wire_source_of(quit), WireErrorSource::Proxy);
            let descriptor =
                descriptor_for_packet_io(&error, SideMarker::Backend, SideMarker::Backend);
            assert_eq!(
                descriptor.disconnect,
                DisconnectState::NotDisconnect,
                "{kind:?} must not be an attributed disconnect"
            );
        }
    }

    #[test]
    fn framing_decode_error_is_proxy_malformed_not_network() {
        // Go treats a framing/sequence error as a proxy bug (SrcProxyMalformed),
        // never a client/backend network break — even on a client read leg.
        let quit = classify_packet_io(
            &PacketIoError::Decode(DecodeError::EmptyCommandPacket),
            SideMarker::Client,
            SideMarker::Client,
        );
        assert_eq!(quit, QuitSource::ProxyMalformed);
        assert_eq!(wire_source_of(quit), WireErrorSource::Proxy);
    }

    #[test]
    fn oversized_logical_payload_attributes_to_the_over_sending_endpoint() {
        // The generic read_logical(limit) transport cap is NOT Go's handshake
        // ErrPacketTooLarge sentinel: it fires on backend responses and
        // post-handshake command reads too. It must attribute to the over-sending
        // read source, so a backend oversized response stays BackendNetwork (not
        // mis-attributed to the client) and a client one stays ClientNetwork.
        let oversized = || PacketIoError::LogicalPayloadTooLarge {
            limit: 16,
            observed: 32,
        };
        // Backend read (source=Backend): a backend oversized response.
        let backend = classify_packet_io(&oversized(), SideMarker::Backend, SideMarker::Backend);
        assert_eq!(
            backend,
            QuitSource::BackendNetwork,
            "backend oversized must NOT mis-attribute to the client"
        );
        assert_eq!(wire_source_of(backend), WireErrorSource::BackendNetwork);
        // Client read (source=Client): a client oversized (handshake or command).
        let client = classify_packet_io(&oversized(), SideMarker::Client, SideMarker::Client);
        assert_eq!(client, QuitSource::ClientNetwork);
        assert_eq!(wire_source_of(client), WireErrorSource::ClientNetwork);
    }

    #[test]
    fn end_source_projects_the_stored_winner_not_the_last_call() {
        // Blocker regression: once an earlier classification stores a log
        // source, a later end_source(new) must keep that winner AND return its
        // wire projection — never first-wins-log + last-call-wire (A/B split).
        // Fresh: the new source wins and both observables agree.
        assert_eq!(
            resolve_end_source(QuitSource::None, QuitSource::BackendNetwork),
            (QuitSource::BackendNetwork, WireErrorSource::BackendNetwork)
        );
        // Preexisting: the stored winner is kept, and the wire is its
        // projection — NOT wire_source_of(the later ClientNetwork).
        assert_eq!(
            resolve_end_source(QuitSource::BackendHandshake, QuitSource::ClientNetwork),
            (
                QuitSource::BackendHandshake,
                WireErrorSource::BackendNetwork
            ),
            "stored winner drives BOTH observables; a mutation projecting the \
             later source would return ClientNetwork here"
        );
    }

    #[test]
    fn accounting_invariant_faults_are_proxy_errors() {
        for error in [
            PacketIoError::CounterOverflow { field: "inbound" },
            PacketIoError::ForwardAlreadyComplete,
        ] {
            let quit = classify_packet_io(&error, SideMarker::Backend, SideMarker::Client);
            assert_eq!(quit, QuitSource::ProxyError);
            assert_eq!(wire_source_of(quit), WireErrorSource::Proxy);
        }
    }

    #[test]
    fn wire_and_log_observables_are_one_classification() {
        // Projecting quit -> wire -> quit is stable for the coarse network and
        // shutdown variants, proving the two observables never diverge for the
        // classes this fix targets.
        for quit in [
            QuitSource::ClientNetwork,
            QuitSource::BackendNetwork,
            QuitSource::ProxyQuit,
        ] {
            assert_eq!(coarse_quit_source(wire_source_of(quit)), quit);
        }
    }

    #[test]
    fn wrapped_disconnect_is_still_a_network_break() {
        // Go's errors.Is unwraps: a disconnect nested in a wrapper still
        // classifies as a network break attributed to the observed endpoint.
        let wrapped = IoError::other(IoError::from(ErrorKind::TimedOut));
        let quit = classify_packet_io(
            &PacketIoError::Io {
                side: IoSide::Source,
                operation: "test",
                source: wrapped,
            },
            SideMarker::Backend,
            SideMarker::Backend,
        );
        assert_eq!(quit, QuitSource::BackendNetwork);
    }

    #[test]
    fn route_timeout_preserves_go_last_failure_semantics() {
        assert_eq!(
            acquire_quit_source(&super::AcquireError::BudgetExhausted {
                last_failure: Some(crate::route::DialFailure::Timeout),
            }),
            QuitSource::BackendHandshake,
            "Go returns the last typed dial failure when the connect budget expires"
        );
        assert_eq!(
            acquire_quit_source(&super::AcquireError::BudgetExhausted { last_failure: None }),
            QuitSource::ProxyError,
            "a routing/control wait that never produced a backend is not a false no-backend label"
        );
    }
}

#[cfg(test)]
mod tls_wiring_tests {
    use super::{
        backend_health_in_snapshot, backend_server_name, candidate_budget, leading_capabilities,
        local_admission_client_error, migration_auth_capabilities, normalize_leading_capabilities,
        proxy_capabilities, proxy_client_log_address, record_proxy_client_log_source,
    };
    use crate::observability::SessionLogContext;
    use control_proto::v1::BackendSnapshot;
    use control_router::RouteError;
    use mysql_wire::{
        CapabilityFlags, HandshakeResponseParams, encode_handshake_response, encode_ssl_request,
        parse_handshake_response, parse_ssl_request,
    };
    use std::sync::OnceLock;

    #[test]
    fn proxy_capabilities_advertise_ssl_only_when_available() {
        assert!(
            proxy_capabilities(true).contains(CapabilityFlags::SSL),
            "a frontend TLS config advertises SSL"
        );
        assert!(
            !proxy_capabilities(false).contains(CapabilityFlags::SSL),
            "no frontend TLS config strips SSL so advertisement matches capability"
        );
        // Everything except SSL is identical across the two, so only the SSL
        // bit is governed per snapshot.
        assert_eq!(
            proxy_capabilities(true).without(CapabilityFlags::SSL),
            proxy_capabilities(false),
        );
    }

    #[test]
    fn local_namespace_rejection_uses_the_approved_mysql_1105_message() {
        assert_eq!(
            local_admission_client_error(RouteError::NamespaceMissing),
            Some((1105, *b"HY000", "failed to find a namespace"))
        );
        assert_eq!(
            local_admission_client_error(RouteError::InvalidConfig),
            Some((1105, *b"HY000", "invalid namespace configuration"))
        );
        assert_eq!(
            local_admission_client_error(RouteError::ControlUnavailable),
            None,
            "transient owner drift is retried/fails closed, never mislabeled as namespace missing"
        );
    }

    #[test]
    fn leading_capabilities_reads_the_ssl_bit() {
        // A strict 32-byte SSLRequest with SSL set classifies as SSL.
        let ssl_request = encode_ssl_request(
            CapabilityFlags::PROTOCOL_41 | CapabilityFlags::SSL,
            0x0100_0000,
            45,
        );
        assert!(leading_capabilities(&ssl_request).contains(CapabilityFlags::SSL));
        assert!(parse_ssl_request(&ssl_request).is_ok());

        // A plaintext-first packet without the SSL bit does not.
        let plain = encode_ssl_request(CapabilityFlags::PROTOCOL_41, 0x0100_0000, 45);
        assert!(!leading_capabilities(&plain).contains(CapabilityFlags::SSL));

        // A truncated leading window carries no SSL bit and falls through to
        // the (fail-closed) handshake parser.
        assert!(!leading_capabilities(&[0xff, 0xff]).contains(CapabilityFlags::SSL));
        assert!(!leading_capabilities(&[]).contains(CapabilityFlags::SSL));
    }

    #[test]
    fn normalize_leading_capabilities_governs_layout_affecting_parse()
    -> Result<(), Box<dyn std::error::Error>> {
        // The real (trusted) client asked for CONNECT_WITH_DB, so the response
        // is laid out with a database field, encoded here per the trusted mask.
        let trusted = CapabilityFlags::PROTOCOL_41
            | CapabilityFlags::SECURE_CONNECTION
            | CapabilityFlags::CONNECT_WITH_DB
            | CapabilityFlags::PLUGIN_AUTH;
        let encoded = encode_handshake_response(HandshakeResponseParams {
            capabilities: trusted,
            max_packet_size: 0x0100_0000,
            collation: 45,
            username: b"alice",
            auth_response: b"\x01\x02\x03",
            database: Some(b"shop"),
            auth_plugin_name: Some(b"mysql_native_password"),
            attributes: None,
            zstd_level: None,
        })?;

        // Simulate a hostile in-TLS second packet whose leading mask drops
        // CONNECT_WITH_DB — a layout-affecting mismatch. Parsing the SAME bytes
        // under that untrusted mask misreads the layout: no database, and the
        // "shop" bytes are consumed as the auth plugin name.
        let untrusted = trusted.without(CapabilityFlags::CONNECT_WITH_DB);
        let mut hostile = encoded.clone();
        hostile[0..4].copy_from_slice(&untrusted.bits().to_le_bytes());
        assert_eq!(leading_capabilities(&hostile), untrusted);
        let misread = parse_handshake_response(&hostile)?;
        assert_eq!(
            misread.database, None,
            "untrusted layout drops the database"
        );
        assert_eq!(
            misread.auth_plugin_name,
            Some(b"shop".as_ref()),
            "untrusted layout misreads the database bytes as the plugin name"
        );

        // Normalizing the leading bytes back to the trusted mask restores the
        // real layout: the database and plugin parse correctly.
        normalize_leading_capabilities(&mut hostile, trusted);
        assert_eq!(leading_capabilities(&hostile), trusted);
        let fixed = parse_handshake_response(&hostile)?;
        assert_eq!(fixed.capabilities, trusted);
        assert_eq!(fixed.database, Some(b"shop".as_ref()));
        assert_eq!(
            fixed.auth_plugin_name,
            Some(b"mysql_native_password".as_ref())
        );
        assert_eq!(fixed.username, b"alice");
        // A short payload is left untouched (it fails the subsequent parse).
        let mut short = [1_u8, 2, 3];
        normalize_leading_capabilities(&mut short, trusted);
        assert_eq!(short, [1, 2, 3]);
        Ok(())
    }

    #[test]
    fn backend_server_name_is_the_host_without_port_or_brackets() {
        assert_eq!(
            backend_server_name("tidb.example.com:4000"),
            "tidb.example.com"
        );
        assert_eq!(backend_server_name("127.0.0.1:4000"), "127.0.0.1");
        assert_eq!(backend_server_name("[::1]:4000"), "::1");
        // A bare host with no port is used verbatim.
        assert_eq!(backend_server_name("localhost"), "localhost");
    }

    #[test]
    fn live_backend_health_requires_the_exact_backend_identity() {
        let backend = BackendSnapshot {
            backend_id: "tidb-1".to_owned(),
            address: "127.0.0.1:4000".to_owned(),
            cluster_name: "cluster-a".to_owned(),
            healthy: true,
            ..BackendSnapshot::default()
        };
        assert!(backend_health_in_snapshot(
            std::slice::from_ref(&backend),
            "tidb-1",
            "127.0.0.1:4000",
            "cluster-a"
        ));
        assert!(!backend_health_in_snapshot(
            std::slice::from_ref(&backend),
            "tidb-1",
            "127.0.0.1:4001",
            "cluster-a"
        ));
        assert!(!backend_health_in_snapshot(
            std::slice::from_ref(&backend),
            "tidb-1",
            "127.0.0.1:4000",
            "cluster-b"
        ));
        assert!(!backend_health_in_snapshot(
            &[],
            "tidb-1",
            "127.0.0.1:4000",
            "cluster-a"
        ));

        let unhealthy = BackendSnapshot {
            healthy: false,
            ..backend
        };
        assert!(!backend_health_in_snapshot(
            &[unhealthy],
            "tidb-1",
            "127.0.0.1:4000",
            "cluster-a"
        ));
    }

    #[test]
    fn proxy_client_log_address_prefers_inbound_inet_source_and_falls_back_to_peer() {
        let peer = std::net::SocketAddr::from(([10, 0, 0, 8], 3306));
        let source = std::net::SocketAddr::from(([203, 0, 113, 9], 45678));

        assert_eq!(
            proxy_client_log_address(peer, Some(source)),
            "203.0.113.9:45678"
        );
        assert_eq!(proxy_client_log_address(peer, None), "10.0.0.8:3306");
    }

    #[test]
    fn proxy_client_log_source_is_shared_with_the_force_close_owner() {
        let peer = std::net::SocketAddr::from(([10, 0, 0, 8], 3306));
        let source = std::net::SocketAddr::from(([203, 0, 113, 9], 45678));
        let shared = OnceLock::new();
        let mut context = SessionLogContext {
            connection_id: 7,
            listener: "127.0.0.1:6000".to_owned(),
            client_address: peer.to_string(),
            proxy_client_address: peer.to_string(),
            namespace: "default".to_owned(),
            generation: 9,
        };

        record_proxy_client_log_source(&shared, &mut context, peer, Some(source));

        assert_eq!(context.client_address, "10.0.0.8:3306");
        assert_eq!(context.proxy_client_address, "203.0.113.9:45678");
        assert_eq!(shared.get().copied(), Some(source));
    }

    #[test]
    fn migration_candidate_deadline_is_always_bounded() {
        assert_eq!(candidate_budget(0), super::DialSchedule::default().total);
        assert!(
            candidate_budget(1).is_zero(),
            "an already-expired absolute deadline cannot start candidate I/O"
        );
    }

    #[test]
    fn migration_token_length_governs_lenenc_independent_of_backend_advertisement()
    -> Result<(), super::CandidateFailure> {
        let planned = CapabilityFlags::PROTOCOL_41
            | CapabilityFlags::CONNECT_WITH_DB
            | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA;
        let backend = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::CONNECT_WITH_DB;

        let short = migration_auth_capabilities(planned, backend, true, 250)?;
        assert!(short.contains(CapabilityFlags::PLUGIN_AUTH));
        assert!(short.contains(CapabilityFlags::CONNECT_WITH_DB));
        assert!(
            !short.contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA),
            "the Go-compatible boundary uses secure-connection encoding at 250 bytes"
        );

        let long = migration_auth_capabilities(planned, backend, true, 251)?;
        assert!(long.contains(CapabilityFlags::PLUGIN_AUTH));
        assert!(long.contains(CapabilityFlags::CONNECT_WITH_DB));
        assert!(
            long.contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA),
            "Go forces length-encoded auth data above 250 bytes"
        );
        Ok(())
    }

    #[test]
    fn migration_database_capability_follows_the_authoritative_snapshot()
    -> Result<(), super::CandidateFailure> {
        let planned = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::CONNECT_WITH_DB;
        let backend = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::CONNECT_WITH_DB;

        let cleared = migration_auth_capabilities(planned, backend, false, 32)?;
        assert!(!cleared.contains(CapabilityFlags::CONNECT_WITH_DB));

        let unsupported = migration_auth_capabilities(
            planned,
            backend.without(CapabilityFlags::CONNECT_WITH_DB),
            true,
            32,
        );
        assert!(
            unsupported.is_err(),
            "a restored database fails closed when the candidate cannot encode it"
        );
        Ok(())
    }
}

#[cfg(test)]
mod intake_tests {
    use super::{Command, MAX_PHYSICAL_PAYLOAD_LEN, PeekDecision, classify_intake};

    /// PKT-003 intake decision, driven from the header alone.
    ///
    /// The threshold and the `COM_CHANGE_USER` exemption are the two facts an
    /// end-to-end test cannot reach cheaply: a 16 MiB change-user would have
    /// to be sent to observe the exemption at all. Driving the decision
    /// directly keeps both discriminable.
    #[test]
    fn oversized_headers_stream_unless_the_command_must_be_rewritten() {
        let maximal =
            u32::try_from(MAX_PHYSICAL_PAYLOAD_LEN).unwrap_or_else(|_| unreachable!("u24 fits"));

        // Below the threshold nothing streams, whatever the command is.
        for byte in [
            Command::Query.as_byte(),
            Command::ChangeUser.as_byte(),
            Command::StmtSendLongData.as_byte(),
            0xfe,
        ] {
            assert_eq!(
                classify_intake(Some(byte), maximal - 1),
                PeekDecision::Materialize,
                "command {byte:#04x} below the threshold"
            );
        }

        // At and above it, an ordinary command streams.
        for length in [maximal, maximal.saturating_add(1)] {
            assert_eq!(
                classify_intake(Some(Command::Query.as_byte()), length),
                PeekDecision::Stream(Command::Query)
            );
            assert_eq!(
                classify_intake(Some(Command::StmtSendLongData.as_byte()), length),
                PeekDecision::Stream(Command::StmtSendLongData),
                "long data is the command this path exists for"
            );
            // Go exempts it and so must this: the packet is rewritten before
            // it is forwarded, so it cannot be relayed unread at any size.
            assert_eq!(
                classify_intake(Some(Command::ChangeUser.as_byte()), length),
                PeekDecision::Materialize,
                "COM_CHANGE_USER is never streamed"
            );
            // Not a command: refused from the header instead of read first.
            assert_eq!(
                classify_intake(Some(0xfe), length),
                PeekDecision::RejectUnknown
            );
            // An empty physical packet has no command byte to classify.
            assert_eq!(classify_intake(None, length), PeekDecision::Materialize);
        }
    }
}
