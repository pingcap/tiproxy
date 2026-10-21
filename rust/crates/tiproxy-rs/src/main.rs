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

//! `TiProxy` Rust dataplane executable.

#![forbid(unsafe_code)]

mod config_composition;
mod health;
mod metrics_http;
mod native_meter;
mod startup;
mod tls_material;
mod topology_composition;
mod vip_network;
mod vip_owner;

use std::env;
use std::fmt::Display;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use config_composition::{
    ConfigServingAdapter, RustConfigComposer, ServingCandidateValidator, control_config,
};
use control_config::{
    ConfigModule, ConfigModuleHandle, ConfigModuleOptions, ConfigNamespaceSource,
    HealthCheckConfig, NamespaceConfig, TopologyRuntimeIdentity,
};
use control_etcd::ElectionConfig;
use control_external::{EtcdClientConfig, EtcdTlsConfig};
use control_plane::logging::{self as process_logging, LogFileSettings};
use control_plane::{
    ConfigSource, ControlModule, ControlModuleSet, ControlRuntime as InProcessControlRuntime,
    JsonStderrSink, LifecyclePhase, OwnershipRegistry, ShutdownReason,
};
use control_proto::CONTROL_PROTOCOL_V1;
use control_proto::control_transport::ClientConfig;
use control_proto::control_transport::ControlClient;
use control_proto::snapshot::SnapshotStore;
use control_proto::v1::{ControlCapability, Hello, Role};
use control_router::{RouteCandidateValidator, RoutePlane};
use control_topology::{
    AdvertiseEndpointResolver, InterfaceAdvertiseResolver, MetricCollector, TopologyModule,
    TopologyModuleHandle,
};
use dataplane::GenerationStatusSnapshot;
use dataplane::control_runtime::{
    ControlRuntime, spawn_control_runtime_with_client_and_handler, spawn_local_control_runtime,
};
use dataplane::metering::{MeteringSamplerError, MeteringSourceRegistry};
use dataplane::session::SessionLoopConfig;
use dataplane::session_engine::EngineSessionOwner;
use dataplane::{
    BoundSessionHandler, ControlCommandHandler, DEFAULT_OBSERVATION_CAPACITY,
    DataplaneServingHandle, DataplaneSnapshotConsumer, DispatchConnectionHandler, MeteringLedger,
    MetricsExporter, MetricsRecorder, MetricsRegistry, MigrationMetrics, PlaneMigrationState,
    ServerError, SystemMemoryProbe, SystemTimeMonitor, install_session_log_writer,
    spawn_metrics_exporter, spawn_system_time_monitor,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use topology_composition::{
    ArtifactClusterFactory, CompositeCandidateValidator, MetricServerConnectionAcceptor,
    TopologyCandidateValidator, interface_advertise_candidates, metric_owner_bind_address,
};
use vip_network::{LinuxNetwork, NetworkOperation};
use vip_owner::{VipHandle, VipModule};

const VERSION: &str = env!("TIPROXY_BUILD_VERSION");
const COMMIT: &str = env!("TIPROXY_BUILD_COMMIT");
const BUILD_TIME: &str = env!("TIPROXY_BUILD_TIME");

const CONTROL_SOCKET_ENV: &str = "TIPROXY_CONTROL_SOCKET";
const CONTROL_UID_ENV: &str = "TIPROXY_CONTROL_UID";
const TLS_ROOTS_ENV: &str = "TIPROXY_TLS_ROOTS";
const CONFIG_FILE_ENV: &str = "TIPROXY_CONFIG";

/// Bounds every pre-listener control-module readiness wait. A producer that
/// stops making progress therefore reaches the armed startup rollback seam with
/// a module-qualified diagnostic instead of freezing process startup forever.
const CONTROL_STARTUP_READY_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_MODULE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound for `--drain-grace-seconds` (30 days, the drain
/// subsystem's shared deadline cap): far above any real grace and small
/// enough that deadline arithmetic on `Instant` can never overflow.
const MAX_DRAIN_GRACE_SECONDS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, PartialEq, Eq)]
struct Options {
    config_file: PathBuf,
    standalone: bool,
    control_socket: PathBuf,
    routing_shadow_socket: Option<PathBuf>,
    control_uid: u32,
    tls_roots: Vec<PathBuf>,
    drain_grace: Option<Duration>,
    health_port: u16,
    metrics_addr: Option<SocketAddr>,
    admin_addr: Option<SocketAddr>,
    log_file: Option<PathBuf>,
}

enum Command {
    Run(Box<Options>),
    Version,
    Help,
    IntegrationCapabilities,
}

/// The integration harness's capability contract (DPL-07): only what
/// this binary truthfully provides today. TLS (frontend `SSLRequest` +
/// backend TLS, WIRE-activation A1), the PROXY v2 backend preamble
/// (WIRE-activation B), and `MySQL` compression (zlib + zstd, WIRE-activation
/// C) are all wired, so `tls`, `proxy-v2`, `zlib`, and `zstd` are advertised
/// and the topology preflight admits plain, tls, proxy, and compressed
/// variants.
const INTEGRATION_CAPABILITIES: &str = "in-process-control-runtime,control-bridge-v1,rust-route-owner,rust-meter-owner,rust-api-owner,mysql-listener,health-endpoint,graceful-shutdown,tls,proxy-v2,zlib,zstd";

#[tokio::main]
async fn main() -> ExitCode {
    let command = match parse_options(env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}\n\n{}", usage());
            return ExitCode::from(2);
        }
    };
    match command {
        Command::Version => {
            println!("{}", version_output());
            ExitCode::SUCCESS
        }
        Command::Help => {
            println!("{}", usage());
            ExitCode::SUCCESS
        }
        Command::IntegrationCapabilities => {
            println!("{INTEGRATION_CAPABILITIES}");
            ExitCode::SUCCESS
        }
        Command::Run(options) => match run(*options).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("tiproxy-rs stopped: {error}");
                ExitCode::from(1)
            }
        },
    }
}

/// The metering sampler task plus the signal that stops it, owned together so
/// the startup guard can stop and join it as one resource.
struct MeteringSampler {
    task: JoinHandle<Result<(), MeteringSamplerError>>,
    shutdown: watch::Sender<bool>,
}

/// A resource torn down in two explicit phases: a synchronous `stop` (signal the
/// task to end) followed by an awaited `join` (wait for it to finish). Splitting
/// the phases lets a fake exercise the exact production teardown sequence, so a
/// dropped stop or join is caught by a test rather than silently detaching a
/// task or hanging on a lease until its TTL.
trait StopJoin: Send + 'static {
    /// Signals the resource to stop.
    fn stop(&self);
    /// Joins the stopped resource to completion.
    fn join(self) -> startup::TeardownFuture;
}

impl<T: StopJoin> startup::Teardown for T {
    fn teardown(self) -> startup::TeardownFuture {
        Box::pin(async move {
            self.stop();
            self.join().await;
        })
    }
}

impl StopJoin for ControlRuntime {
    fn stop(&self) {
        self.shutdown();
    }
    fn join(self) -> startup::TeardownFuture {
        Box::pin(async move {
            let _ = ControlRuntime::join(self).await;
        })
    }
}

impl StopJoin for MetricsExporter {
    fn stop(&self) {
        self.shutdown();
    }
    fn join(self) -> startup::TeardownFuture {
        Box::pin(MetricsExporter::join(self))
    }
}

impl StopJoin for MeteringSampler {
    fn stop(&self) {
        self.shutdown.send_replace(true);
    }
    fn join(self) -> startup::TeardownFuture {
        Box::pin(async move {
            let _ = self.task.await;
        })
    }
}

impl StopJoin for SystemTimeMonitor {
    fn stop(&self) {
        SystemTimeMonitor::stop(self);
    }
    fn join(self) -> startup::TeardownFuture {
        Box::pin(async move {
            let _ = SystemTimeMonitor::join(self).await;
        })
    }
}

impl StopJoin for JoinHandle<()> {
    fn stop(&self) {
        self.abort();
    }
    fn join(self) -> startup::TeardownFuture {
        Box::pin(async move {
            let _ = self.await;
        })
    }
}

/// The resources the steady-state supervisor takes ownership of after a
/// successful startup, produced by [`StartupGuard::commit`].
struct RunningProcess<R, E, S, H> {
    modules: ControlModuleSet,
    runtime: R,
    metrics_exporter: E,
    metering_sampler: S,
    health_task: Option<H>,
    metrics_http_task: Option<JoinHandle<()>>,
    system_time_monitor: Option<SystemTimeMonitor>,
    log_reload_task: Option<JoinHandle<()>>,
    admin_task: Option<AdminTask>,
    routing_shadow: Option<legacy_router_shadow::consumer::Task>,
}

/// Owns every resource acquired after the first control module is spawned, so a
/// startup failure stops and joins them in reverse order — each exactly once —
/// instead of leaking abruptly aborted tasks (a registrar leak would hold a
/// topology lease until its TTL).
///
/// The optional resources are generic over [`startup::Teardown`] so production
/// wires the real handles and a test wires recording fakes into the *same*
/// rollback code. The guard is armed on creation, before the first module is
/// spawned. Every fallible startup step routes its error through
/// [`StartupGuard::rollback`], the single failure exit; a successful startup
/// ends with [`StartupGuard::commit`], which hands the resources to the
/// supervisor. The embedded arm token makes dropping a half-constructed startup
/// that did neither a loud bug.
struct StartupGuard<R, E, S, H> {
    arm: startup::ArmToken,
    in_process: Arc<InProcessControlRuntime>,
    modules: ControlModuleSet,
    runtime: Option<R>,
    metrics_exporter: Option<E>,
    metering_sampler: Option<S>,
    health_task: Option<H>,
    /// Native `/metrics` responder; a concrete task because no test fakes it.
    metrics_http_task: Option<JoinHandle<()>>,
    system_time_monitor: Option<SystemTimeMonitor>,
    /// Applies `log.log-file.*` reloads to the process log output.
    log_reload_task: Option<JoinHandle<()>>,
    /// Management-plane HTTP server (CP-ADMIN), stopped gracefully at exit.
    admin_task: Option<AdminTask>,
    routing_shadow: Option<legacy_router_shadow::consumer::Task>,
}

impl<R, E, S, H> StartupGuard<R, E, S, H>
where
    R: startup::Teardown,
    E: startup::Teardown,
    S: startup::Teardown,
    H: startup::Teardown,
{
    /// Arms the guard around the owner and the module set that already holds the
    /// first (config owner) module.
    fn arm(in_process: Arc<InProcessControlRuntime>, modules: ControlModuleSet) -> Self {
        Self {
            arm: startup::ArmToken::armed(),
            in_process,
            modules,
            runtime: None,
            metrics_exporter: None,
            metering_sampler: None,
            health_task: None,
            metrics_http_task: None,
            system_time_monitor: None,
            log_reload_task: None,
            admin_task: None,
            routing_shadow: None,
        }
    }

    /// Registers and starts one more control module under the guard.
    fn spawn_module<M: ControlModule>(&mut self, module: M) -> Result<(), String> {
        self.modules
            .spawn(module)
            .map_err(|error| error.to_string())
    }

    fn set_runtime(&mut self, runtime: R) {
        if self.runtime.replace(runtime).is_some() {
            unreachable!("the legacy runtime was set twice");
        }
    }

    fn set_metrics_exporter(&mut self, exporter: E) {
        if self.metrics_exporter.replace(exporter).is_some() {
            unreachable!("the metrics exporter was set twice");
        }
    }

    fn set_metering_sampler(&mut self, sampler: S) {
        if self.metering_sampler.replace(sampler).is_some() {
            unreachable!("the metering sampler was set twice");
        }
    }

    fn set_health_task(&mut self, health_task: H) {
        if self.health_task.replace(health_task).is_some() {
            unreachable!("the health task was set twice");
        }
    }

    fn set_metrics_http_task(&mut self, task: JoinHandle<()>) {
        if self.metrics_http_task.replace(task).is_some() {
            unreachable!("the metrics HTTP task was set twice");
        }
    }

    fn set_system_time_monitor(&mut self, monitor: SystemTimeMonitor) {
        if self.system_time_monitor.replace(monitor).is_some() {
            unreachable!("the system time monitor was set twice");
        }
    }

    fn set_admin_task(&mut self, task: AdminTask) {
        if self.admin_task.replace(task).is_some() {
            unreachable!("the admin task was set twice");
        }
    }

    fn set_log_reload_task(&mut self, task: JoinHandle<()>) {
        if self.log_reload_task.replace(task).is_some() {
            unreachable!("the log reload task was set twice");
        }
    }

    /// Stops and joins every acquired resource in reverse order and returns the
    /// original error unchanged.
    async fn rollback(mut self, error: String) -> String {
        // Registered in acquisition order; `run_teardowns_in_reverse` runs them
        // latest-first. A resource absent at the failure point (its `Option` is
        // `None`) is simply skipped.
        let mut steps: Vec<(&'static str, startup::TeardownFuture)> = Vec::new();
        if let Some(observer) = self.routing_shadow.take() {
            steps.push(("routing_shadow", startup::Teardown::teardown(observer)));
        }
        if let Some(runtime) = self.runtime.take() {
            steps.push(("legacy_runtime", runtime.teardown()));
        }
        if let Some(exporter) = self.metrics_exporter.take() {
            steps.push(("metrics_exporter", exporter.teardown()));
        }
        if let Some(sampler) = self.metering_sampler.take() {
            steps.push(("metering_sampler", sampler.teardown()));
        }
        if let Some(health_task) = self.health_task.take() {
            steps.push(("health_task", health_task.teardown()));
        }
        if let Some(task) = self.metrics_http_task.take() {
            steps.push(("metrics_http_task", startup::Teardown::teardown(task)));
        }
        if let Some(monitor) = self.system_time_monitor.take() {
            steps.push(("system_time_monitor", startup::Teardown::teardown(monitor)));
        }
        if let Some(task) = self.log_reload_task.take() {
            steps.push(("log_reload_task", startup::Teardown::teardown(task)));
        }
        if let Some(task) = self.admin_task.take() {
            steps.push(("admin_task", startup::Teardown::teardown(task)));
        }
        let _order = startup::run_teardowns_in_reverse(steps).await;
        // The owner and its modules were acquired first, so they retire last.
        // Make shutdown mandatory and advance to Stopping (so a topology module
        // retires its registration at Stopping), then join every module and
        // finish the owner.
        self.in_process.fail("startup", "startup_failed");
        let _ = self.in_process.advance_shutdown(LifecyclePhase::Draining);
        let _ = self.in_process.advance_shutdown(LifecyclePhase::Stopping);
        let _ = join_modules(&mut self.modules).await;
        let _ = self.in_process.finish();
        self.arm.disarm();
        error
    }

    /// Hands every resource to the steady-state supervisor and disarms the guard.
    ///
    /// The required slots are taken and validated *before* the arm token is
    /// disarmed, so a missing slot (a wiring bug) panics while the guard is still
    /// armed — the drop bomb then also fires — rather than after disarming, which
    /// would silently drop the remaining handles.
    fn commit(self) -> RunningProcess<R, E, S, H> {
        let StartupGuard {
            mut arm,
            in_process: _,
            modules,
            runtime,
            metrics_exporter,
            metering_sampler,
            health_task,
            metrics_http_task,
            system_time_monitor,
            log_reload_task,
            admin_task,
            routing_shadow,
        } = self;
        let runtime = runtime.unwrap_or_else(|| unreachable!("commit before the runtime was set"));
        let metrics_exporter = metrics_exporter
            .unwrap_or_else(|| unreachable!("commit before the metrics exporter was set"));
        let metering_sampler = metering_sampler
            .unwrap_or_else(|| unreachable!("commit before the metering sampler was set"));
        arm.disarm();
        RunningProcess {
            modules,
            runtime,
            metrics_exporter,
            metering_sampler,
            health_task,
            metrics_http_task,
            system_time_monitor,
            log_reload_task,
            admin_task,
            routing_shadow,
        }
    }
}

/// Waits for one startup producer under an explicit deadline and returns a
/// module-qualified diagnostic for both channel closure and timeout. The caller
/// owns the armed rollback guard and routes either error through that one seam.
async fn wait_module_ready<E: Display>(
    module: &str,
    budget: Duration,
    ready: impl Future<Output = Result<(), E>>,
) -> Result<(), String> {
    match tokio::time::timeout(budget, ready).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("initialize {module}: {error}")),
        Err(_) => Err(format!(
            "initialize {module}: readiness_timeout after {}s",
            budget.as_secs()
        )),
    }
}

// This is the executable's composition root: keeping the startup resources,
// three-way supervisor, and reverse-order shutdown visible in one function
// makes the fail-closed ownership order auditable.
#[allow(clippy::too_many_lines)]
async fn run(options: Options) -> Result<(), String> {
    // CP-001 owns the process-local lifecycle/config/TLS/log/metrics
    // foundation. The legacy protobuf client below is an outer adapter for
    // responsibilities that still live in Go; its messages never become this
    // runtime's domain model.
    let process_started_unix_millis: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let process_id = format!("tiproxy-rs-{}", std::process::id());
    let config_owner = load_config_owner(&options, &process_id)?;
    // Process log output (B0): honour `log.log-file.*` before the first
    // lifecycle event, and route the dataplane's session logs through the
    // same writer so a rotating file receives every line.
    let initial_log_file = log_file_settings(
        options.log_file.as_deref(),
        config_owner.handle.source().current().as_ref(),
    );
    {
        // Line contract (slice 4a): the Go header of the configured encoder
        // on every line, and the configured level filter. `log.encoder` and
        // `log.simple` are restart-required; `log.level` follows the config
        // reload below. Like Go's `BuildLogger`, a level zap rejects fails
        // startup.
        let effective = config_owner.handle.source().current();
        let level = process_logging::Level::parse(effective.effective().log_level())
            .map_err(|error| format!("build logger: {error}"))?;
        process_logging::set_format(
            process_logging::Encoder::from_config(effective.effective().log_encoder()),
            effective.effective().log_simple(),
        );
        process_logging::set_level(level);
    }
    process_logging::configure(initial_log_file.as_ref())?;
    install_session_log_writer(session_log_line);
    let routing_shadow_socket = options.routing_shadow_socket.clone().or_else(|| {
        config_owner
            .handle
            .source()
            .current()
            .effective()
            .routing_shadow_socket()
            .map(Path::to_path_buf)
    });
    if routing_shadow_socket.as_ref() == Some(&options.control_socket) {
        return Err("routing shadow socket must differ from control socket".to_owned());
    }
    let initial_config = control_config(
        config_owner.handle.source().current().as_ref(),
        options.health_port,
        &config_owner.tls_roots,
        options.drain_grace,
    )?;
    let in_process_registry = OwnershipRegistry::new();
    let in_process = Arc::new(
        InProcessControlRuntime::claim_process(
            &in_process_registry,
            process_id.clone(),
            initial_config,
            Arc::new(JsonStderrSink),
        )
        .map_err(|error| format!("start in-process control runtime: {error}"))?,
    );
    let in_process_config = in_process.handle().config().current();
    let capabilities = vec![
        ControlCapability::RustApiOwner as u64,
        ControlCapability::RustMeterOwner as u64,
        ControlCapability::RustConfigNamespace as u64,
        ControlCapability::RustRouteOwner as u64,
    ];
    let hello = Hello {
        role: Role::RustDataplane as i32,
        process_id,
        process_started_unix_millis,
        supported_versions: vec![u32::from(CONTROL_PROTOCOL_V1)],
        capabilities: capabilities.clone(),
        max_frame_bytes: control_proto::DEFAULT_MAX_FRAME_BYTES,
        build_version: VERSION.to_owned(),
        build_commit: COMMIT.to_owned(),
    };
    let dispatch_handler = ControlCommandHandler::native_meter_owner();
    // Go `NewDrainIssuer` fails the bridge when the incarnation nonce cannot
    // be read; the Rust owner refuses to start the same way.
    dispatch_handler.local_drain_issuer_ready()?;
    let mut client =
        ClientConfig::with_defaults(options.control_socket.clone(), options.control_uid, hello);
    client.required_capabilities = capabilities;
    let store = config_owner.snapshots.clone();

    // DPL-04: the real session owner replaces DPL-03's parked handler.
    // Sessions share the control client with the runtime; the drain and
    // session-shutdown watches drive the coordinated local shutdown.
    let shared_client = Arc::new(ControlClient::new(client).map_err(|error| error.to_string())?);
    let (drain_tx, drain_rx) = watch::channel(None::<Duration>);
    let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
    let (metering_shutdown_tx, _) = watch::channel(false);
    let loop_config = session_loop_config(in_process_config.drain_grace());
    let (metrics, observations) = MetricsRecorder::channel(DEFAULT_OBSERVATION_CAPACITY);
    let metrics_registry = Arc::new(MetricsRegistry::new());
    let runtime_handle = in_process.handle();
    let modules = ControlModuleSet::new(&runtime_handle);
    // Arm the startup guard before the first module is spawned. From here every
    // fallible acquisition routes its error through `guard.rollback` — the single
    // failure exit — so a failure stops and joins everything already started
    // (rather than leaking an aborted registrar that would hold a topology lease
    // until its TTL); a successful startup ends with `guard.commit`.
    let mut guard = StartupGuard::arm(Arc::clone(&in_process), modules);
    // STARTUP-GUARD:ARMED
    // Go starts this with the metrics manager, before anything control-plane
    // related, and it is the only producer behind
    // tiproxy_monitor_time_jump_back_total and tiproxy_monitor_keep_alive_total.
    // Start it as soon as the guard can own it: a process still waiting on its
    // control peer is exactly when a clock step matters, and Go counts there
    // too. The guard stops and joins it on rollback and on shutdown.
    guard.set_system_time_monitor(spawn_system_time_monitor(Arc::clone(&metrics_registry)));
    guard.routing_shadow = routing_shadow_socket.map(legacy_router_shadow::consumer::Task::spawn);
    if let Err(error) = guard.spawn_module(config_owner.module) {
        return Err(guard
            .rollback(format!("start config owner module: {error}"))
            .await);
    }
    // Persistent `/config` is part of generation one. Do not let the legacy
    // bridge open SQL listeners against the file-only base while the initial
    // linearizable relist is still outstanding.
    if let Err(error) = wait_module_ready(
        "config owner",
        CONTROL_STARTUP_READY_TIMEOUT,
        config_owner.handle.wait_ready(),
    )
    .await
    {
        return Err(guard.rollback(error).await);
    }
    if let Err(error) = config_owner.handle.source().bootstrap_namespaces_if_empty(
        vec![NamespaceConfig {
            namespace: "default".to_owned(),
            ..NamespaceConfig::default()
        }],
        &config_owner.current_dir,
    ) {
        return Err(guard
            .rollback(format!("bootstrap default namespace: {error}"))
            .await);
    }
    let meter_config = config_owner.handle.source().current();
    let meter_paths = Path::new(meter_config.effective().workdir()).join("run");
    let wal_path = if options.standalone {
        meter_paths.join("rust-metering.wal")
    } else {
        let mut wal_name = options.control_socket.as_os_str().to_os_string();
        wal_name.push(".metering.wal");
        PathBuf::from(wal_name)
    };
    let _meter_lock = match native_meter::lock_state(&meter_paths) {
        Ok(lock) => lock,
        Err(error) => return Err(guard.rollback(error).await),
    };
    let _wal_lock = match native_meter::lock_wal(&wal_path) {
        Ok(lock) => lock,
        Err(error) => return Err(guard.rollback(error).await),
    };
    let ledger = match MeteringLedger::open_persistent(wal_path) {
        Ok(ledger) => ledger,
        Err(error) => return Err(guard.rollback(format!("open metering WAL: {error}")).await),
    };
    let metering = match MeteringSourceRegistry::new(ledger.process_generation()) {
        Ok(registry) => registry,
        Err(error) => {
            return Err(guard
                .rollback(format!("create metering registry: {error}"))
                .await);
        }
    };
    // CP-TOPO self-registration and discovery publication come online before any
    // SQL admission: register this instance's SQL topology and publish the
    // initial discovery generation, then wait for both to be installed. "Ready"
    // here means the registration children are installed and the initial
    // discovery set is published, not that PD has acknowledged the registration or
    // that any topology has yet been fetched through the discovery handle.
    let topology_identity = TopologyRuntimeIdentity {
        version: Arc::from(VERSION),
        git_hash: Arc::from(COMMIT),
        deploy_path: env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_path_buf))
            .unwrap_or_else(|| PathBuf::from(".")),
        start_timestamp: i64::try_from(process_started_unix_millis / 1000).unwrap_or(i64::MAX),
    };
    // The health-check policy is a restart-pinned PROCESS input, not a
    // configuration generation. TiProxy exposes no user-facing health-check
    // config (Go builds it from `NewDefaultHealthCheckConfig()` directly), so the
    // module is handed the Go-compatible default once at construction; the checked
    // constructor validates it (an invalid policy rolls back through the armed
    // startup guard) and owns it immutably thereafter — there is no runtime
    // reload path.
    let advertise_resolver = Arc::new(InterfaceAdvertiseResolver::new(Arc::new(
        interface_advertise_candidates,
    )));
    let initial_topology = match config_owner.handle.source().current().topology() {
        Ok(topology) => topology,
        Err(error) => {
            return Err(guard
                .rollback(format!("project metric owner endpoint: {error}"))
                .await);
        }
    };
    let metric_advertise_host = match advertise_resolver.resolve(&initial_topology) {
        Ok(host) => host,
        Err(error) => {
            return Err(guard
                .rollback(format!("resolve metric owner endpoint: {error}"))
                .await);
        }
    };
    // Go still owns VIP in the two-process configuration. Only the pure-Rust
    // standalone process may start the new election, so the two owners cannot
    // silently compete for the same address during the migration period.
    let vip_plan = if options.standalone {
        let vip = match config_owner.handle.source().current().effective().vip() {
            Ok(vip) => vip,
            Err(error) => return Err(guard.rollback(format!("project VIP config: {error}")).await),
        };
        if let Some(vip) = vip {
            let roots = tls_material::open_tls_roots(&config_owner.tls_roots);
            let tls = match etcd_tls(&vip.cluster_tls, &roots) {
                Ok(tls) => tls,
                Err(error) => {
                    return Err(guard.rollback(format!("prepare VIP TLS: {error}")).await);
                }
            };
            let endpoints = vip
                .pd_addrs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let client = EtcdClientConfig::new(endpoints, tls).and_then(|client| {
                client.with_timeouts(
                    Duration::from_secs(2),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Duration::from_secs(30),
                )
            });
            match client {
                Ok(client) => Some((vip, client)),
                Err(error) => {
                    return Err(guard
                        .rollback(format!("prepare VIP etcd client: {error}"))
                        .await);
                }
            }
        } else {
            None
        }
    } else {
        None
    };
    let metric_bind_address = metric_owner_bind_address(&initial_topology, &metric_advertise_host);
    let (topology_module, mut topology_handle) = match TopologyModule::new(
        Arc::new(config_owner.handle.source().clone()),
        Box::new(ArtifactClusterFactory),
        advertise_resolver,
        topology_identity,
        HealthCheckConfig::default(),
    ) {
        Ok(parts) => parts,
        Err(error) => {
            return Err(guard
                .rollback(format!("invalid pinned health config: {error}"))
                .await);
        }
    };
    let topology_module = match topology_module.with_metrics() {
        Ok(module) => module,
        Err(error) => {
            return Err(guard
                .rollback(format!("invalid pinned metric config: {error}"))
                .await);
        }
    };
    if let Err(error) = guard.spawn_module(topology_module) {
        return Err(guard
            .rollback(format!("start topology module: {error}"))
            .await);
    }
    if let Err(error) = wait_module_ready(
        "topology module",
        CONTROL_STARTUP_READY_TIMEOUT,
        topology_handle.wait_ready(),
    )
    .await
    {
        return Err(guard.rollback(error).await);
    }
    // Bind the same-process owner-history endpoint before its control module can
    // campaign. It uses the topology advertise host plus the actual bind result
    // (ephemeral for configured port zero), and applies the exact server-HTTP
    // TLS identity retained by each accepted config generation. Candidate
    // validation keeps this listener distinct from Go's SQL and API ports.
    let (metric_collector, metric_overlay) = match MetricCollector::bind_for_routing_endpoint(
        topology_handle.metric_source(),
        metric_bind_address,
        metric_advertise_host.as_ref(),
        Arc::new(MetricServerConnectionAcceptor::new(Arc::new(
            config_owner.handle.source().clone(),
        ))),
    )
    .await
    {
        Ok(parts) => parts,
        Err(error) => {
            return Err(guard
                .rollback(format!("bind metric owner endpoint: {error}"))
                .await);
        }
    };
    // Installed before the collector runs, so no election worker can win
    // without somewhere to report it. `server_owner` then renders from the
    // same history at scrape.
    metric_overlay.set_owner_history(topology_handle.owner_history());
    metrics_registry.set_owner_state_source(topology_handle.owner_history());
    if let Err(error) = guard.spawn_module(metric_collector) {
        return Err(guard
            .rollback(format!("start metric collector module: {error}"))
            .await);
    }
    // The local route plane must bind every namespace's exact incarnation to a
    // retained topology source before any serving snapshot can open listeners.
    // T2 consumes that ready handle for local initial routing; the bridge route
    // path remains available only to compatibility tests.
    let route_config_source: Arc<dyn ConfigNamespaceSource> =
        Arc::new(config_owner.handle.source().clone());
    // CP-ADMIN reads the owner history bytes through the same collector.
    let backend_metrics = metric_overlay.backend_metrics_reader();
    let (mut route_plane, mut route_plane_handle) = RoutePlane::new(
        Arc::clone(&route_config_source),
        topology_handle.clone(),
        Some(metric_overlay),
    );
    // The router records migrations but owns no registry; publish them onto
    // the same non-blocking recorder the SQL path uses, before the plane runs
    // so no router incarnation is built without a sink.
    route_plane.set_migration_sink(Arc::new(MigrationMetrics::new(metrics.clone())));
    // The three migration families are served from router and process state
    // at scrape time, not accumulated from those notifications.
    metrics_registry.set_migration_state_source(Arc::new(PlaneMigrationState::new(
        route_plane_handle.clone(),
    )));
    // `backend_metric` from the store the resource and health factors write.
    metrics_registry.set_backend_metric_state_source(route_plane_handle.backend_metrics());
    // `b_score` likewise, from the store the balance rounds write into.
    metrics_registry.set_score_state_source(route_plane_handle.score_history());
    // The three backend health families are served the same way, from the
    // history the topology health child and the SQL probes write into.
    metrics_registry.set_health_state_source(topology_handle.health_history());
    // CP-ADMIN's debug redirect sweeps every current router.
    let redirect_plane = route_plane_handle.clone();
    if let Err(error) = guard.spawn_module(route_plane) {
        return Err(guard
            .rollback(format!("start route-plane module: {error}"))
            .await);
    }
    if let Err(error) = wait_module_ready(
        "route-plane module",
        CONTROL_STARTUP_READY_TIMEOUT,
        route_plane_handle.wait_ready(),
    )
    .await
    {
        return Err(guard.rollback(error).await);
    }
    // Only now compose the SQL session owner: every production session receives
    // the ready process-local route plane, so no listener can observe the
    // compatibility bridge acquisition path.
    let owner: Arc<dyn BoundSessionHandler> = Arc::new(
        EngineSessionOwner::new(
            Arc::clone(&shared_client),
            "default",
            session_shutdown_rx.clone(),
            drain_rx,
            loop_config,
        )
        .with_metrics(metrics.clone())
        .with_metering(metering.clone())
        .with_route_plane(route_plane_handle.clone()),
    );
    let (connection_handler, installer) = DispatchConnectionHandler::new("default", owner);
    let composer = Arc::new(
        RustConfigComposer::new(config_owner.handle.source().clone(), options.drain_grace)
            .with_topology(topology_handle.clone()),
    );
    let (consumer, serving) = DataplaneSnapshotConsumer::new_with_composer(
        Arc::new(SystemMemoryProbe::new()),
        Arc::new(connection_handler),
        composer.clone(),
    );
    // Forced shutdown lets each session owner finish its bounded terminal work
    // before the abort backstop fires.
    let consumer =
        consumer.with_force_join_grace(loop_config.cleanup_deadline + Duration::from_secs(1));
    let (runtime, meter_ready, mut local_consumer) = if options.standalone {
        let (ready, _) = watch::channel(None);
        (
            spawn_local_control_runtime(
                Arc::clone(&shared_client),
                Duration::from_millis(100),
                dispatch_handler,
            ),
            ready,
            Some(consumer),
        )
    } else {
        let (consumer, ready) = native_meter::ReadyConsumer::new(consumer);
        (
            spawn_control_runtime_with_client_and_handler(
                Arc::clone(&shared_client),
                Duration::from_millis(100),
                8,
                store.clone(),
                consumer,
                dispatch_handler,
            ),
            ready,
            None,
        )
    };
    // Take the dispatch handles and stats before the runtime moves into the
    // guard; from then on the guard owns it (and tears it down on any later
    // failure).
    let install_handle = runtime.handle();
    let admin_dispatch = runtime.handle();
    let runtime_stats = runtime.stats();
    guard.set_runtime(runtime);
    if !installer.install(install_handle) {
        meter_ready.send_replace(Some(false));
        return Err(guard
            .rollback("install control dispatch handle exactly once".to_owned())
            .await);
    }
    // Operator-facing listeners (health, metrics, management plane) bind
    // before the control peer is awaited: a bad address fails fast and rolls
    // back, and the health probe stays reachable while native metering
    // recovers (it answers not-ready until the SQL gate opens).
    let meter_slot: Arc<std::sync::OnceLock<Arc<control_meter::service::Service>>> =
        Arc::new(std::sync::OnceLock::new());
    // LB readiness must turn off before stop-accept without changing the
    // process lifecycle: Quiescing also revokes route admission authority.
    let lb_readiness = LbReadiness::new(in_process.handle());
    // Readiness probe for the integration topology: answers 503 until
    // the first applied generation, 200 after. Bound before serving so
    // a bad port fails fast; the task is owned and aborted at exit.
    match spawn_health(
        in_process_config.health_port(),
        serving.clone(),
        route_plane_handle,
        route_config_source,
        topology_handle.clone(),
        Arc::clone(&runtime_stats),
        HealthGates {
            lb_readiness: lb_readiness.clone(),
            meter: Arc::clone(&meter_slot),
        },
    )
    .await
    {
        Ok(Some(health_task)) => guard.set_health_task(health_task),
        Ok(None) => {}
        Err(error) => {
            meter_ready.send_replace(Some(false));
            return Err(guard.rollback(error).await);
        }
    }
    // Native Prometheus exposition (B0): bound before ready so a bad address
    // fails fast; the task is owned by the guard and aborted at exit.
    match spawn_metrics_http(options.metrics_addr, Arc::clone(&metrics_registry)).await {
        Ok(Some(task)) => guard.set_metrics_http_task(task),
        Ok(None) => {}
        Err(error) => {
            meter_ready.send_replace(Some(false));
            return Err(guard.rollback(error).await);
        }
    }
    guard.set_log_reload_task(tokio::spawn(run_log_reload(
        options.log_file.clone(),
        config_owner.handle.source().subscribe(),
        initial_log_file,
    )));
    // Management plane (CP-ADMIN): bound before ready so a bad address fails
    // fast; its readiness gate opens together with the process, like Go's
    // `ready` toggle at the end of `NewServer`. Under `RUST_API_OWNER` the
    // Rust process is the only API server, so it binds the configured
    // `api.addr` unless `--admin-addr` overrides it.
    let admin_address = options.admin_addr.map_or_else(
        || {
            config_owner
                .handle
                .source()
                .current()
                .effective()
                .api_addr()
                .to_owned()
        },
        |address| address.to_string(),
    );
    let admin_app = match spawn_admin(
        admin_address,
        admin_hooks(
            lb_readiness.clone(),
            &config_owner.handle,
            &serving,
            &metrics_registry,
            admin_dispatch,
            options.log_file.clone(),
            AdminPlaneSeams {
                backend_metrics,
                route_plane: redirect_plane,
            },
        ),
        admin_tls_source(config_owner.handle.source().clone()),
    )
    .await
    {
        Ok(Some((app, task))) => {
            guard.set_admin_task(task);
            Some(app)
        }
        Ok(None) => None,
        Err(error) => {
            meter_ready.send_replace(Some(false));
            return Err(guard.rollback(error).await);
        }
    };
    if !options.standalone
        && let Err(error) = wait_module_ready(
            "native meter peer",
            CONTROL_STARTUP_READY_TIMEOUT,
            native_meter::wait_peer(shared_client.subscribe_state()),
        )
        .await
    {
        meter_ready.send_replace(Some(false));
        return Err(guard.rollback(error).await);
    }
    // Bridge mode requires negotiated native ownership before opening either
    // consumer or outbox. Local mode itself is the native owner.
    let native_meter = match control_meter::service::Service::open(
        meter_config.effective().metering(),
        meter_paths.join("rust-metering-consumer.json"),
        meter_paths.join("metering-outbox.json"),
        in_process.handle().module_context().owner().clone(),
    )
    .await
    {
        Ok(meter) => meter,
        Err(error) => {
            meter_ready.send_replace(Some(false));
            return Err(guard.rollback(format!("open native meter: {error}")).await);
        }
    };
    let _ = meter_slot.set(Arc::clone(&native_meter));
    let ledger =
        match dataplane::metering::recover_native_metering(ledger, Arc::clone(&native_meter)).await
        {
            Ok(ledger) => ledger,
            Err(error) => {
                meter_ready.send_replace(Some(false));
                return Err(guard
                    .rollback(format!("recover native meter WAL: {error}"))
                    .await);
            }
        };
    guard.set_metrics_exporter(spawn_metrics_exporter(
        Arc::clone(&shared_client),
        serving.clone(),
        Arc::clone(&runtime_stats),
        &metrics,
        observations,
        Duration::from_secs(1),
        Arc::clone(&metrics_registry),
    ));
    let native_shutdown = metering_shutdown_tx.clone();
    guard.set_metering_sampler(MeteringSampler {
        task: tokio::spawn(native_meter::run(
            metering,
            ledger,
            Arc::clone(&native_meter),
            native_shutdown,
            Duration::from_secs(1),
        )),
        shutdown: metering_shutdown_tx.clone(),
    });

    // Local mode opens SQL only after native metering has recovered and its
    // sampler is supervised. The first serving generation is composed from the
    // Rust config/topology source; no Go snapshot or socket is involved.
    if let Some(mut consumer) = local_consumer.take() {
        let local_source = composer.local_source_snapshot();
        let now = control_proto::snapshot::UnixTime::since_unix_epoch(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        );
        if let Err(error) = consumer.bootstrap_local(&store, &local_source, now).await {
            meter_ready.send_replace(Some(false));
            return Err(guard
                .rollback(format!("bind local SQL serving generation: {error}"))
                .await);
        }
    }
    if let Err(error) = guard.spawn_module(ConfigServingAdapter::new(
        serving.clone(),
        store.clone(),
        Arc::clone(&in_process),
        options.health_port,
        config_owner.tls_roots,
        options.drain_grace,
        Arc::clone(&composer),
    )) {
        meter_ready.send_replace(Some(false));
        return Err(guard
            .rollback(format!("start config serving adapter: {error}"))
            .await);
    }

    let mut vip_handle = None;
    if let Some((vip, etcd)) = vip_plan {
        let network = match LinuxNetwork::new(&vip) {
            Ok(network) => Arc::new(network),
            Err(error) => {
                return Err(guard
                    .rollback(format!(
                        "initialize VIP network: {} / {}",
                        error.operation, error.class
                    ))
                    .await);
            }
        };
        // Match Go Start: clear a stale address before the process becomes
        // ready or the dedicated election can be won.
        if let Err(error) = network.delete_ip().await {
            return Err(guard
                .rollback(format!(
                    "clear stale VIP: {} / {}",
                    error.operation, error.class
                ))
                .await);
        }
        let host = metric_advertise_host.as_ref();
        let member_id = if host.contains(':') {
            format!("[{host}]:{}", initial_topology.sql_port)
        } else {
            format!("{host}:{}", initial_topology.sql_port)
        };
        let process_id = runtime_handle.lifecycle().owner_id;
        let (module, handle) = match VipModule::new(
            &vip,
            member_id,
            process_id.as_ref(),
            etcd,
            network,
            topology_handle.owner_history(),
        ) {
            Ok(parts) => parts,
            Err(error) => return Err(guard.rollback(error).await),
        };
        if let Err(error) = guard.spawn_module(module) {
            return Err(guard.rollback(format!("start VIP module: {error}")).await);
        }
        vip_handle = Some(handle);
    }

    if let Err(error) = in_process.mark_ready() {
        meter_ready.send_replace(Some(false));
        let vip_close = close_vip(&mut vip_handle).await.err();
        let failure = match vip_close {
            Some(vip_close) => {
                format!("mark in-process control runtime ready: {error}; {vip_close}")
            }
            None => format!("mark in-process control runtime ready: {error}"),
        };
        return Err(guard.rollback(failure).await);
    }
    meter_ready.send_replace(Some(true));
    if let Some(app) = &admin_app {
        app.mark_ready();
    }
    // STARTUP-GUARD:COMMIT
    let RunningProcess {
        mut modules,
        runtime,
        metrics_exporter,
        metering_sampler:
            MeteringSampler {
                task: metering_sampler,
                shutdown: _,
            },
        health_task,
        metrics_http_task,
        system_time_monitor,
        log_reload_task,
        admin_task,
        routing_shadow,
    } = guard.commit();

    // Supervise control and metering together. Either task disappearing must
    // wake this owner: otherwise a sampler panic could leave SQL serving
    // forever, or a control failure could leave the sampler waiting forever.
    // The termination branch joins every session before asking the sampler for
    // its final durable snapshot, so shutdown bytes and final source markers
    // cannot race the sampler's exit.
    let mut control_runtime = tokio::spawn(runtime.join());
    let mut metering_sampler = metering_sampler;
    let mut termination = Box::pin(wait_for_termination_signal());
    // The management plane is supervised too: a listener failure must not
    // silently leave the process without its operator surface (Go only logs
    // that; the Rust owner fails closed and drains).
    // The select borrows the join handle; the exit path hands it back to the
    // one teardown helper, which never polls a handle that already finished.
    let (mut admin_join, admin_shutdown) = match admin_task {
        Some(AdminTask { task, shutdown }) => (Some(task), Some(shutdown)),
        None => (None, None),
    };
    let mut admin_exit = Box::pin(async {
        match admin_join.as_mut() {
            Some(task) => task.await,
            None => std::future::pending().await,
        }
    });
    let (control_result, sampler_result, serving_result, module_result, vip_close_result) = tokio::select! {
        control = &mut control_runtime => {
            let control = match control {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("control runtime supervisor panicked".to_owned()),
            };
            let vip_close_result = close_vip(&mut vip_handle).await;
            if control.is_err() {
                in_process.fail("legacy_bridge", "runtime_failure");
            } else {
                in_process
                    .begin_shutdown(ShutdownReason::ModuleExit)
                    .map_err(|error| format!("begin module-exit shutdown: {error}"))?;
            }
            let serving_result = stop_drain_and_join_sessions(
                &in_process,
                &serving,
                &drain_tx,
                &session_shutdown_tx,
            ).await;
            metering_shutdown_tx.send_replace(true);
            let sampler = match metering_sampler.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("metering sampler panicked".to_owned()),
            };
            (control, sampler, serving_result, Ok(()), vip_close_result)
        }
        sampler = &mut metering_sampler => {
            let sampler = match sampler {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("metering sampler panicked".to_owned()),
            };
            let vip_close_result = close_vip(&mut vip_handle).await;
            if sampler.is_err() {
                in_process.fail("metering_sampler", "runtime_failure");
            } else {
                in_process
                    .begin_shutdown(ShutdownReason::ModuleExit)
                    .map_err(|error| format!("begin module-exit shutdown: {error}"))?;
            }
            // Readiness becomes false at stop_accepting, before the grace
            // period. Existing sessions then drain and finally receive the
            // force signal. The control shutdown wakes its supervisor.
            let serving_result = stop_drain_and_join_sessions(
                &in_process,
                &serving,
                &drain_tx,
                &session_shutdown_tx,
            ).await;
            metering_shutdown_tx.send_replace(true);
            shared_client.shutdown();
            let control = match control_runtime.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("control runtime supervisor panicked".to_owned()),
            };
            (control, sampler, serving_result, Ok(()), vip_close_result)
        }
        () = &mut termination => {
            // Close readiness as soon as SIGTERM wins the select, even if an
            // in-flight config apply briefly delays the applied-snapshot read.
            lb_readiness.mark_unhealthy();
            let vip_close_result = close_vip(&mut vip_handle).await;
            let lb_wait = serving.graceful_wait_before_shutdown().await;
            let event = async {
                tokio::select! {
                    control = &mut control_runtime => OrderedWaitExit::Control(match control {
                        Ok(Ok(())) => Err("control runtime exited during load-balancer wait".to_owned()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("control runtime supervisor panicked".to_owned()),
                    }),
                    sampler = &mut metering_sampler => OrderedWaitExit::Sampler(match sampler {
                        Ok(Ok(())) => Err("metering sampler exited during load-balancer wait".to_owned()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("metering sampler panicked".to_owned()),
                    }),
                    admin = &mut admin_exit => OrderedWaitExit::Admin(match admin {
                        Ok(()) => "control admin server exited during load-balancer wait".to_owned(),
                        Err(_) => "control admin server panicked".to_owned(),
                    }),
                    module = modules.join_next() => OrderedWaitExit::Module(match module {
                        Some(exit) => match exit.result {
                            Ok(()) => format!("control module {} exited during load-balancer wait", exit.module),
                            Err(error) => format!("control module {} failed: {error}", exit.module),
                        },
                        None => "control module executor became empty during load-balancer wait".to_owned(),
                    }),
                }
            };
            let (serving_result, interruption) = signal_stop_drain_and_join_sessions(
                &in_process,
                &serving,
                &lb_readiness,
                sleep_lb_window(lb_wait),
                event,
                &drain_tx,
                &session_shutdown_tx,
            ).await?;
            let mut early_control = None;
            let mut early_sampler = None;
            let mut module_result = Ok(());
            match interruption {
                None => {}
                Some(OrderedWaitExit::Control(control)) => {
                    early_control = Some(control);
                }
                Some(OrderedWaitExit::Sampler(sampler)) => {
                    early_sampler = Some(sampler);
                }
                Some(OrderedWaitExit::Admin(failure) | OrderedWaitExit::Module(failure)) => {
                    module_result = Err(failure);
                }
            }
            metering_shutdown_tx.send_replace(true);
            let sampler = match early_sampler {
                Some(result) => result,
                None => match metering_sampler.await {
                    Ok(result) => result.map_err(|error| error.to_string()),
                    Err(_) => Err("metering sampler panicked".to_owned()),
                },
            };
            shared_client.shutdown();
            let control = match early_control {
                Some(result) => result,
                None => match control_runtime.await {
                    Ok(result) => result.map_err(|error| error.to_string()),
                    Err(_) => Err("control runtime supervisor panicked".to_owned()),
                },
            };
            (control, sampler, serving_result, module_result, vip_close_result)
        }
        admin = &mut admin_exit => {
            let failure = match admin {
                Ok(()) => "control admin server exited unexpectedly".to_owned(),
                Err(_) => "control admin server panicked".to_owned(),
            };
            let vip_close_result = close_vip(&mut vip_handle).await;
            in_process.fail("control_admin", "runtime_failure");
            let serving_result = stop_drain_and_join_sessions(
                &in_process,
                &serving,
                &drain_tx,
                &session_shutdown_tx,
            ).await;
            metering_shutdown_tx.send_replace(true);
            shared_client.shutdown();
            let sampler = match metering_sampler.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("metering sampler panicked".to_owned()),
            };
            let control = match control_runtime.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("control runtime supervisor panicked".to_owned()),
            };
            (control, sampler, serving_result, Err(failure), vip_close_result)
        }
        module = modules.join_next() => {
            let failure = match module {
                Some(exit) => match exit.result {
                    Ok(()) => format!("control module {} exited unexpectedly", exit.module),
                    Err(error) => format!("control module {} failed: {error}", exit.module),
                },
                None => "control module executor became empty unexpectedly".to_owned(),
            };
            let vip_close_result = close_vip(&mut vip_handle).await;
            in_process.fail("control_module", "runtime_failure");
            let serving_result = stop_drain_and_join_sessions(
                &in_process,
                &serving,
                &drain_tx,
                &session_shutdown_tx,
            ).await;
            metering_shutdown_tx.send_replace(true);
            shared_client.shutdown();
            let sampler = match metering_sampler.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("metering sampler panicked".to_owned()),
            };
            let control = match control_runtime.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("control runtime supervisor panicked".to_owned()),
            };
            (control, sampler, serving_result, Err(failure), vip_close_result)
        }
    };
    if let Some(observer) = routing_shadow {
        observer.shutdown().await;
    }
    let module_executor_result = join_modules(&mut modules).await;
    if let Some(task) = health_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = metrics_http_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(monitor) = system_time_monitor {
        monitor.stop();
        let _ = monitor.join().await;
    }
    if let Some(task) = log_reload_task {
        task.abort();
        let _ = task.await;
    }
    drop(admin_exit);
    if let (Some(task), Some(shutdown)) = (admin_join.take(), admin_shutdown) {
        startup::Teardown::teardown(AdminTask { task, shutdown }).await;
    }
    metrics_exporter.shutdown();
    metrics_exporter.join().await;
    let finish_result = in_process
        .finish()
        .map_err(|error| format!("finish in-process control runtime: {error}"));
    // When control fails first, stopping/joining sessions deliberately asks
    // the sampler for one final durable snapshot after its dispatch owner has
    // already disappeared. Preserve the control failure as the root cause;
    // the expected secondary DispatchUnavailable must not mask it. When the
    // sampler fails first, control shutdown is clean and this still returns
    // the sampler's closed-catalog failure below.
    prefer_control_failure(control_result, sampler_result)?;
    serving_result?;
    module_result?;
    vip_close_result?;
    module_executor_result?;
    finish_result?;
    Ok(())
}

fn prefer_control_failure(
    control: Result<(), String>,
    sampler: Result<(), String>,
) -> Result<(), String> {
    control?;
    sampler
}

async fn close_vip(handle: &mut Option<VipHandle>) -> Result<(), String> {
    let Some(handle) = handle.as_mut() else {
        return Ok(());
    };
    handle.pre_close().await.map_err(|error| {
        format!(
            "remove VIP before SQL drain: {} / {}",
            error.operation, error.class
        )
    })
}

enum OrderedWaitExit {
    Control(Result<(), String>),
    Sampler(Result<(), String>),
    Admin(String),
    Module(String),
}

/// Coordinates the two shutdown clocks on the actual signal path. Readiness
/// closes first while lifecycle stays Ready, then the applied LB wait runs;
/// only after that do Quiescing, listener stop, and session drain begin.
async fn signal_stop_drain_and_join_sessions<
    W: Future<Output = ()>,
    F: Future<Output = OrderedWaitExit>,
>(
    runtime: &InProcessControlRuntime,
    serving: &DataplaneServingHandle,
    lb_readiness: &LbReadiness,
    lb_wait: W,
    event: F,
    drain: &watch::Sender<Option<Duration>>,
    session_shutdown: &watch::Sender<bool>,
) -> Result<(Result<(), String>, Option<OrderedWaitExit>), String> {
    lb_readiness.mark_unhealthy();
    let interruption = wait_for_lb_removal_or(lb_wait, event).await;
    match &interruption {
        None => runtime
            .begin_shutdown(ShutdownReason::Signal)
            .map_err(|error| format!("begin signal shutdown: {error}"))?,
        Some(OrderedWaitExit::Control(_)) => runtime.fail("legacy_bridge", "runtime_failure"),
        Some(OrderedWaitExit::Sampler(_)) => runtime.fail("metering_sampler", "runtime_failure"),
        Some(OrderedWaitExit::Admin(_)) => runtime.fail("control_admin", "runtime_failure"),
        Some(OrderedWaitExit::Module(_)) => runtime.fail("control_module", "runtime_failure"),
    }
    let serving_result =
        stop_drain_and_join_sessions(runtime, serving, drain, session_shutdown).await;
    Ok((serving_result, interruption))
}

/// The orderly signal path keeps its SQL listener open until the LB removal
/// deadline, unless a supervised owner exits. Dropping the timer on that exit
/// lets the caller stop admission immediately.
async fn wait_for_lb_removal_or<W: Future<Output = ()>, F: Future>(
    wait: W,
    event: F,
) -> Option<F::Output> {
    tokio::select! {
        () = wait => None,
        event = event => Some(event),
    }
}

async fn sleep_lb_window(mut remaining: Duration) {
    // The wire field is u64 milliseconds. Split extreme, but valid, values
    // so Tokio never adds an overflowing duration to its monotonic clock.
    while !remaining.is_zero() {
        let chunk = remaining.min(Duration::from_secs(24 * 60 * 60));
        tokio::time::sleep(chunk).await;
        remaining -= chunk;
    }
}

async fn join_modules(modules: &mut ControlModuleSet) -> Result<(), String> {
    join_modules_with_timeout(modules, CONTROL_MODULE_SHUTDOWN_TIMEOUT).await
}

async fn join_modules_with_timeout(
    modules: &mut ControlModuleSet,
    timeout: Duration,
) -> Result<(), String> {
    if let Ok(result) = tokio::time::timeout(timeout, async {
        let mut result = Ok(());
        while let Some(exit) = modules.join_next().await {
            if let Err(error) = exit.result {
                result = Err(format!("control module {} failed: {error}", exit.module));
            }
        }
        result
    })
    .await
    {
        return result;
    }

    // A retained route incarnation normally disappears when the last SQL
    // session releases its lease before Stopping. This deadline is the final
    // process-level assertion that a leaked lease or stuck module cannot hang
    // shutdown indefinitely.
    modules.abort_all();
    while modules.join_next().await.is_some() {}
    Err(format!(
        "control module shutdown exceeded {}ms",
        timeout.as_millis()
    ))
}

/// Stops admission, lets the existing per-session graceful timers run, then
/// forces and joins every session. The metering sampler is deliberately not
/// stopped here: its owner signals it only after this future completes, so
/// its last snapshot sees every final raw counter and final source marker.
async fn stop_drain_and_join_sessions(
    runtime: &InProcessControlRuntime,
    serving: &DataplaneServingHandle,
    drain: &watch::Sender<Option<Duration>>,
    session_shutdown: &watch::Sender<bool>,
) -> Result<(), String> {
    runtime
        .advance_shutdown(LifecyclePhase::Draining)
        .map_err(|error| format!("enter control runtime drain phase: {error}"))?;
    serving.stop_accepting().await;
    // Read after admission stops so a successfully committed dynamic update
    // is the sole process-shutdown deadline. The launch-time snapshot must
    // not silently pin this reloadable Go field for the process lifetime.
    let grace = runtime.handle().config().current().drain_grace();
    drain.send_replace(Some(grace));
    tokio::time::sleep(grace).await;
    session_shutdown.send_replace(true);
    let serving_result = serving
        .shutdown()
        .await
        .map_err(|error: ServerError| format!("shut down SQL listeners: {error}"));
    runtime
        .advance_shutdown(LifecyclePhase::Stopping)
        .map_err(|error| format!("enter control runtime stop phase: {error}"))?;
    serving_result
}

/// Binds the optional integration readiness endpoint before its serving task
/// starts, so a port conflict fails the composition synchronously.
async fn spawn_health(
    port: u16,
    serving: DataplaneServingHandle,
    routes: control_router::RoutePlaneHandle,
    config: Arc<dyn ConfigNamespaceSource>,
    topology: TopologyModuleHandle,
    dispatch_stats: Arc<dataplane::control_dispatch::DispatchStats>,
    gates: HealthGates,
) -> Result<Option<JoinHandle<()>>, String> {
    if port == 0 {
        return Ok(None);
    }
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        // The operator diagnostic names the exact port: a bind conflict
        // must be traceable to the address that caused it.
        .map_err(|error| format!("bind health endpoint 127.0.0.1:{port}: {error}"))?;
    let ledger_routes = routes.clone();
    let topology_status = topology.status();
    let routing = topology.routing_handle();
    let HealthGates {
        lb_readiness,
        meter,
    } = gates;
    Ok(Some(tokio::spawn(health::serve(
        listener,
        serving,
        // Before the native meter opens the SQL gate is closed anyway.
        Arc::new(move || lb_readiness.ready() && meter.get().is_none_or(|meter| meter.healthy())),
        Arc::new(move || routes.route_input_evidence()),
        Arc::new(move || ledger_routes.route_ledger_evidence()),
        Arc::new(move || {
            let config = config.current();
            let revision = config.source_revision();
            let status = *topology_status.borrow();
            let routing = routing.current();
            health::SourceGenerationEvidence {
                config_generation: config.generation(),
                config_file_revision: revision.file_revision,
                config_etcd_revision: revision.etcd_revision,
                topology_observed_generation: status.observed_generation,
                topology_applied_generation: status.applied_generation,
                routing_generation: routing.as_ref().map_or(0, |source| source.generation),
                routing_client_epoch: routing.as_ref().map_or(0, |source| source.client_epoch),
            }
        }),
        Arc::new(move || dispatch_stats.drain_watermark.load(Ordering::Relaxed)),
    ))))
}

#[derive(Clone)]
struct LbReadiness {
    lifecycle: control_plane::RuntimeHandle,
    unhealthy: Arc<AtomicBool>,
}

struct HealthGates {
    lb_readiness: LbReadiness,
    meter: Arc<std::sync::OnceLock<Arc<control_meter::service::Service>>>,
}

impl LbReadiness {
    fn new(lifecycle: control_plane::RuntimeHandle) -> Self {
        Self {
            lifecycle,
            unhealthy: Arc::new(AtomicBool::new(false)),
        }
    }

    fn mark_unhealthy(&self) {
        self.unhealthy.store(true, Ordering::SeqCst);
    }

    fn ready(&self) -> bool {
        !self.unhealthy.load(Ordering::SeqCst)
            && self.lifecycle.lifecycle().phase == LifecyclePhase::Ready
    }

    fn closing(&self) -> bool {
        self.unhealthy.load(Ordering::SeqCst)
            || !matches!(
                self.lifecycle.lifecycle().phase,
                LifecyclePhase::Starting | LifecyclePhase::Ready
            )
    }
}

/// ONE grace lineage: the CLI's validated drain grace IS the
/// per-session FSM drain deadline, so the coordinator's absolute grace
/// and the loop's own force timer never diverge.
fn session_loop_config(drain_grace: Duration) -> SessionLoopConfig {
    SessionLoopConfig {
        drain_deadline: drain_grace,
        ..SessionLoopConfig::default()
    }
}

struct ConfigOwner {
    module: ConfigModule,
    handle: ConfigModuleHandle,
    snapshots: SnapshotStore,
    tls_roots: Vec<PathBuf>,
    current_dir: PathBuf,
}

fn load_config_owner(options: &Options, process_id: &str) -> Result<ConfigOwner, String> {
    let current_dir = env::current_dir().map_err(|_| "resolve current directory".to_owned())?;
    let base = ConfigModuleOptions {
        config_file: Some(options.config_file.clone()),
        advertise_addr: None,
        current_dir: current_dir.clone(),
        etcd: None,
        election: None,
        persistence_factory: None,
    };
    let (_, bootstrap) = ConfigModule::load(base.clone())
        .map_err(|error| format!("load initial Rust config source: {error}"))?;
    let initial = bootstrap.source().current();

    let mut tls_roots = options.tls_roots.clone();
    tls_roots.extend(
        initial
            .effective()
            .rust_tls_allowed_roots()
            .iter()
            .map(PathBuf::from),
    );
    tls_roots.sort();
    tls_roots.dedup();
    // Shared allowed TLS roots, each opened once here into a frozen directory
    // capability so every later TLS-material read (topology cluster clients and
    // config-persistence) is confined beneath it.
    let allowed_tls_roots = Arc::new(tls_material::open_tls_roots(&tls_roots));
    let snapshots = SnapshotStore::new(tls_roots.clone())
        .map_err(|error| format!("create snapshot store: {error}"))?;
    let (etcd, election) = persistence_options(initial.as_ref(), process_id, &allowed_tls_roots)?;
    let persistence_roots = Arc::clone(&allowed_tls_roots);
    let module_options = ConfigModuleOptions {
        etcd,
        election,
        persistence_factory: Some(Arc::new(
            move |effective: &control_config::EffectiveConfig| {
                config_persistence_client(effective, &persistence_roots)
            },
        )),
        ..base
    };
    // Every accepted generation funnels through one composite validator in a
    // fixed order: serving first (fails a bad TLS/protocol candidate before any
    // topology material is read), then topology (prepares the per-cluster etcd
    // clients that ride the published snapshot as its opaque artifact).
    let serving = Arc::new(ServingCandidateValidator::new(
        snapshots.clone(),
        options.drain_grace,
    ));
    let validator = Arc::new(CompositeCandidateValidator::new(
        serving,
        Arc::new(RouteCandidateValidator),
        Arc::new(TopologyCandidateValidator::new(Arc::clone(
            &allowed_tls_roots,
        ))),
    ));
    let (module, handle) = ConfigModule::load_with_validator(module_options, validator)
        .map_err(|error| format!("load validated Rust config owner: {error}"))?;
    if handle.source().current().config_checksum() != initial.config_checksum() {
        return Err("config file changed while initializing Rust config owner".to_owned());
    }
    Ok(ConfigOwner {
        module,
        handle,
        snapshots,
        tls_roots,
        current_dir,
    })
}

fn persistence_options(
    snapshot: &control_config::ConfigNamespaceSnapshot,
    process_id: &str,
    allowed_tls_roots: &tls_material::TlsRoots,
) -> Result<(Option<EtcdClientConfig>, Option<ElectionConfig>), String> {
    let client = config_persistence_client(snapshot.effective(), allowed_tls_roots)?;
    if client.is_none() {
        return Ok((None, None));
    }
    let election = ElectionConfig::new(
        "/tiproxy/config/owner",
        process_id,
        format!("/tiproxy/config/session/{process_id}"),
        30,
    )
    .map_err(|error| format!("validate config election: {error}"))?;
    Ok((client, Some(election)))
}

fn config_persistence_client(
    effective: &control_config::EffectiveConfig,
    allowed_tls_roots: &tls_material::TlsRoots,
) -> Result<Option<EtcdClientConfig>, String> {
    let Some(persistence) = effective.config_persistence() else {
        return Ok(None);
    };
    let endpoints = persistence
        .pd_addrs
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let tls = etcd_tls(&persistence.cluster_tls, allowed_tls_roots)?;
    let client = EtcdClientConfig::new(endpoints, tls)
        .map_err(|error| format!("validate config persistence client: {error}"))?;
    Ok(Some(client))
}

fn etcd_tls(
    config: &control_config::ClientTlsConfig,
    allowed_tls_roots: &tls_material::TlsRoots,
) -> Result<Option<EtcdTlsConfig>, String> {
    if config.skip_ca_verification {
        return Err("config persistence does not support skip-ca-verification".to_owned());
    }
    let configured = config.ca_path.is_some()
        || config.certificate_path.is_some()
        || config.private_key_path.is_some();
    if !configured {
        return Ok(None);
    }
    let ca_path = config
        .ca_path
        .as_deref()
        .ok_or_else(|| "config persistence TLS requires a CA".to_owned())?;
    let ca = tls_material::read_tls_material(ca_path, allowed_tls_roots)
        .map_err(|_| "read config persistence TLS CA".to_owned())?;
    let certificate = read_optional_tls(config.certificate_path.as_deref(), allowed_tls_roots)?;
    let key = read_optional_tls(config.private_key_path.as_deref(), allowed_tls_roots)?;
    EtcdTlsConfig::new(
        Some(ca),
        certificate,
        key,
        None,
        control_external::EtcdTlsPolicy::default(),
    )
    .map(Some)
    .map_err(|error| format!("validate config persistence TLS: {error}"))
}

fn read_optional_tls(
    path: Option<&Path>,
    allowed_tls_roots: &tls_material::TlsRoots,
) -> Result<Option<Vec<u8>>, String> {
    path.map(|path| {
        tls_material::read_tls_material(path, allowed_tls_roots)
            .map_err(|reason| format!("read config persistence TLS material: {reason}"))
    })
    .transpose()
}

/// Resolves on SIGTERM or SIGINT.
async fn wait_for_termination_signal() {
    let Ok(mut sigterm) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        return std::future::pending::<()>().await;
    };
    let Ok(mut sigint) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
    else {
        return std::future::pending::<()>().await;
    };
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
}

/// Dataplane session lifecycle lines are `INFO` records of the process log.
fn session_log_line(line: &str) {
    process_logging::emit(process_logging::Level::Info, line);
}

/// Projects `log.log-file.*` from the committed config into the process log
/// output settings; an empty file name keeps the standard stream.
/// Combines the process's own `--log-file` path with the committed
/// `log.log-file.*` rotation limits. While the Go control process still owns
/// `log.log-file.filename`, the Rust process must never share that file (two
/// rotating writers on one path), so the Rust file name comes only from the
/// CLI; without it the process keeps its standard stream.
fn log_file_settings(
    log_file: Option<&Path>,
    snapshot: &control_config::ConfigNamespaceSnapshot,
) -> Option<LogFileSettings> {
    let filename = log_file?;
    let log = snapshot.effective().log_online();
    Some(LogFileSettings {
        filename: filename.to_path_buf(),
        max_size_mb: log.log_file_max_size_mb(),
        max_days: log.log_file_max_days(),
        max_backups: log.log_file_max_backups(),
    })
}

/// Applies every committed `log.log-file.*` rotation-limit change to the
/// process log output. A file that cannot be reopened is reported on the
/// current output and the previous output stays in place, exactly like the
/// Go logger's rebuild.
async fn run_log_reload(
    log_file: Option<PathBuf>,
    mut updates: watch::Receiver<Arc<control_config::ConfigNamespaceSnapshot>>,
    mut current: Option<LogFileSettings>,
) {
    while updates.changed().await.is_ok() {
        let snapshot = updates.borrow_and_update().clone();
        // Go `updateLoggerCfg`: rebuild the file output first; only when that
        // succeeded parse and apply `log.level`, and a level zap rejects
        // leaves the running level untouched (the failure is logged).
        let next = log_file_settings(log_file.as_deref(), snapshot.as_ref());
        if next != current {
            match process_logging::configure(next.as_ref()) {
                Ok(()) => current = next,
                Err(error) => {
                    process_logging::emit(
                        process_logging::Level::Error,
                        &format!(
                            "{{\"component\":\"tiproxy-rs\",\"event\":\"log_file_reload_rejected\",\"error\":\"{}\"}}",
                            error.replace('\\', "\\\\").replace('"', "\\\"")
                        ),
                    );
                    continue;
                }
            }
        }
        match process_logging::Level::parse(snapshot.effective().log_level()) {
            Ok(level) => process_logging::set_level(level),
            Err(error) => process_logging::emit(
                process_logging::Level::Error,
                &format!(
                    "{{\"component\":\"tiproxy-rs\",\"event\":\"log_level_reload_rejected\",\"error\":\"{}\"}}",
                    error.to_string().replace('\\', "\\\\").replace('"', "\\\"")
                ),
            ),
        }
    }
}

/// Binds the native `/metrics` listener when an address was configured.
/// Management-plane server task plus its graceful stop signal.
struct AdminTask {
    task: JoinHandle<()>,
    shutdown: watch::Sender<bool>,
}

impl startup::Teardown for AdminTask {
    /// The single stop/cleanup helper for the admin server, used by the
    /// startup rollback and by the supervisor exit path. It tolerates a join
    /// handle whose completion was already observed (the supervisor branch
    /// that noticed the server exiting): a finished handle is never polled
    /// again.
    fn teardown(self) -> startup::TeardownFuture {
        Box::pin(async move {
            let AdminTask { mut task, shutdown } = self;
            // Stop accepting and let in-flight requests finish within the
            // server's own grace period; abort only if it overruns that.
            shutdown.send_replace(true);
            if task.is_finished() {
                return;
            }
            if tokio::time::timeout(ADMIN_SHUTDOWN_TIMEOUT, &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        })
    }
}

/// Upper bound on the admin server's graceful stop at process exit.
const ADMIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);

async fn spawn_admin(
    address: String,
    hooks: control_admin::AdminHooks,
    tls: control_admin::TlsConfigSource,
) -> Result<Option<(Arc<control_admin::AdminApp>, AdminTask)>, String> {
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| format!("bind admin endpoint {address}: {error}"))?;
    let app = Arc::new(control_admin::AdminApp::new(
        hooks,
        control_admin::HealthState::new(),
    ));
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(control_admin::serve(
        listener,
        control_admin::full_router(Arc::clone(&app)),
        control_admin::plaintext_router(Arc::clone(&app)),
        tls,
        shutdown_rx,
        control_admin::ServeOptions::default(),
    ));
    Ok(Some((app, AdminTask { task, shutdown })))
}

/// `control_admin::ConfigAdmin` over the config owner: namespaces persist
/// below `/config` through the owner-fenced handle, a configuration `PUT` is
/// the owner's `SetTOMLConfig` equivalent, and a commit waits until the SQL
/// serving side has composed the current config generation.
struct OwnerConfigAdmin {
    handle: ConfigModuleHandle,
    /// CP-CFG generations the SQL serving side actually installed (initial
    /// bind, bridge apply, recomposition). Each value is read from the
    /// validated view that was installed, so nothing composed, staged,
    /// skipped, or rejected can confirm a generation.
    applied_config: watch::Receiver<u64>,
}

/// Longest a namespace commit waits for the serving side to catch up.
const ADMIN_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

impl control_admin::ConfigAdmin for OwnerConfigAdmin {
    fn list_namespaces(&self) -> Vec<NamespaceConfig> {
        self.handle.source().current().namespaces().to_vec()
    }

    fn get_namespace(&self, name: &str) -> Option<NamespaceConfig> {
        self.handle
            .source()
            .current()
            .namespaces()
            .iter()
            .find(|namespace| namespace.namespace == name)
            .cloned()
    }

    fn set_namespace(
        &self,
        value: NamespaceConfig,
    ) -> control_admin::AdminFuture<'_, Result<(), control_config::ConfigMutationError>> {
        Box::pin(self.handle.set_namespace(value))
    }

    fn delete_namespace(
        &self,
        name: String,
    ) -> control_admin::AdminFuture<'_, Result<(), control_config::ConfigMutationError>> {
        Box::pin(async move {
            // Go's B-tree delete of an absent key is a no-op.
            if self.get_namespace(&name).is_none() {
                return Ok(());
            }
            self.handle.delete_namespace(name).await
        })
    }

    fn commit_namespaces(
        &self,
        names: Vec<String>,
    ) -> control_admin::AdminFuture<'_, Result<(), control_admin::CommitError>> {
        Box::pin(async move {
            let snapshot = self.handle.source().current();
            if names
                .iter()
                .any(|name| !snapshot.namespaces().iter().any(|ns| ns.namespace == *name))
            {
                return Err(control_admin::CommitError::Missing);
            }
            // Namespaces become serving through the config watch; "commit"
            // is complete once serving has applied this CP-CFG generation.
            let target = snapshot.generation();
            let mut applied = self.applied_config.clone();
            let barrier = async {
                loop {
                    if *applied.borrow_and_update() >= target {
                        return Ok(());
                    }
                    if applied.changed().await.is_err() {
                        return Err(control_admin::CommitError::Reload);
                    }
                }
            };
            tokio::time::timeout(ADMIN_COMMIT_TIMEOUT, barrier)
                .await
                .unwrap_or(Err(control_admin::CommitError::Reload))
        })
    }

    fn config_toml(&self) -> Option<String> {
        self.handle
            .source()
            .current()
            .effective()
            .to_toml_string()
            .ok()
    }

    fn config_json(&self) -> Option<String> {
        self.handle
            .source()
            .current()
            .effective()
            .to_json_string()
            .ok()
    }

    fn put_config_toml(
        &self,
        data: Vec<u8>,
    ) -> control_admin::AdminFuture<'_, Result<(), control_config::ConfigMutationError>> {
        Box::pin(self.handle.apply_local_toml(data))
    }
}

/// `control_admin::DrainAdmin` over the dispatch owner: the operator label
/// is the local issuer's caller id; outcomes keep Go's error classes and
/// the status projects the gate's progress/terminal.
struct DispatchDrainAdmin {
    dispatch: dataplane::control_dispatch::ControlDispatchHandle,
}

impl control_admin::DrainAdmin for DispatchDrainAdmin {
    fn start(
        &self,
        request: control_admin::DrainRequest,
    ) -> control_admin::AdminFuture<'_, Result<(), control_admin::DrainStartError>> {
        Box::pin(async move {
            let outcome = self
                .dispatch
                .local_drain(dataplane::LocalDrainRequest {
                    caller_id: request.drain_id,
                    listener_names: request.listener_names,
                    backend_ids: request.backend_ids,
                    graceful_wait: request.graceful_wait,
                    force_timeout: request.force_timeout,
                })
                .await
                .ok_or(control_admin::DrainStartError::NoSession)?;
            match outcome {
                dataplane::LocalDrainOutcome::Accepted { .. } => Ok(()),
                dataplane::LocalDrainOutcome::SnapshotNotReady => {
                    Err(control_admin::DrainStartError::SnapshotNotReady)
                }
                dataplane::LocalDrainOutcome::DrainInProgress { .. } => {
                    Err(control_admin::DrainStartError::InProgress)
                }
                dataplane::LocalDrainOutcome::ForeignDrainActive { .. } => {
                    Err(control_admin::DrainStartError::ForeignActive)
                }
                dataplane::LocalDrainOutcome::Rejected { code, detail } => {
                    Err(control_admin::DrainStartError::Other(format!(
                        "{}: {detail}",
                        code.as_str_name()
                    )))
                }
            }
        })
    }

    fn status(
        &self,
        drain_id: String,
    ) -> control_admin::AdminFuture<'_, Option<control_admin::DrainProgress>> {
        Box::pin(async move {
            let status = self.dispatch.local_drain_status(drain_id).await.flatten()?;
            Some(control_admin::DrainProgress {
                active_connections: status.result.active_connections,
                gracefully_closed: status.result.gracefully_closed,
                force_closed: status.result.force_closed,
                complete: status.result.complete,
                code: status.result.code().as_str_name().to_owned(),
                detail: status.result.detail,
            })
        })
    }
}

/// Process-state accessors behind the admin handlers. Every closure reads
/// live state on each request; none retains a payload.
/// The route/metric plane seams the management plane reads from.
struct AdminPlaneSeams {
    backend_metrics: control_topology::BackendMetricsReader,
    route_plane: control_router::RoutePlaneHandle,
}

fn admin_hooks(
    lb_readiness: LbReadiness,
    config: &ConfigModuleHandle,
    serving: &DataplaneServingHandle,
    registry: &Arc<MetricsRegistry>,
    dispatch: dataplane::control_dispatch::ControlDispatchHandle,
    log_file: Option<PathBuf>,
    seams: AdminPlaneSeams,
) -> control_admin::AdminHooks {
    let AdminPlaneSeams {
        backend_metrics,
        route_plane,
    } = seams;
    let health_config = config.clone();
    let health_serving = serving.clone();
    let status_serving = serving.clone();
    let registry = Arc::clone(registry);
    let config_admin: control_admin::SharedConfigAdmin = Arc::new(OwnerConfigAdmin {
        handle: config.clone(),
        applied_config: serving.applied_config_generation(),
    });
    control_admin::AdminHooks {
        health_inputs: Arc::new(move || control_admin::HealthInputs {
            // Go `PreClose` sets closing when the drain begins.
            closing: lb_readiness.closing(),
            namespaces_ready: health_config.is_ready(),
            applied_generation: health_serving.status().applied_generation,
            config_checksum: health_config.source().current().config_checksum(),
        }),
        metrics_text: Arc::new(move || registry.render_prometheus_text()),
        dataplane_status: Arc::new(move || dataplane_status(&status_serving.status())),
        config: config_admin,
        drain: Some(Arc::new(DispatchDrainAdmin { dispatch })),
        log_file,
        backend_metrics: Arc::new(move |cluster| {
            backend_metrics(cluster).map_or_else(Vec::new, |bytes| bytes.to_vec())
        }),
        redirect: Arc::new(move || {
            route_plane
                .redirect_connections()
                .map(|summary| {
                    let line = format!(
                        "{{\"component\":\"control-admin\",\"event\":\"redirect_connections\",\"active\":{},\"offered\":{},\"accepted\":{}}}",
                        summary.active, summary.offered, summary.accepted
                    );
                    process_logging::emit(process_logging::Level::Info, &line);
                })
                .map_err(|error| format!("{error:?}"))
        }),
    }
}

/// Reads the server-HTTP TLS identity retained by the current accepted config
/// generation, the same source the metric owner endpoint uses.
fn admin_tls_source(
    source: control_config::ConfigNamespaceStore,
) -> control_admin::TlsConfigSource {
    Arc::new(move || {
        source
            .current()
            .prepared()
            .downcast_ref::<topology_composition::PreparedProcessSet>()
            .and_then(topology_composition::PreparedProcessSet::server_http_tls)
    })
}

/// Projects the Rust-owned generation counters onto the Go
/// `/api/dataplane/status` shape. There is no transport hop in-process, so
/// desired and sent are both the latest composed generation.
fn dataplane_status(status: &GenerationStatusSnapshot) -> control_admin::DataplaneStatus {
    let composed = status.composition_generation.max(status.applied_generation);
    let last_result_code = if status.rejected_generation > status.applied_generation {
        "ERROR_CODE_INVALID_SNAPSHOT"
    } else if status.applied_generation > 0 {
        "ERROR_CODE_OK"
    } else {
        "ERROR_CODE_UNSPECIFIED"
    };
    control_admin::DataplaneStatus {
        enabled: true,
        desired_generation: composed,
        sent_generation: composed,
        applied_generation: status.applied_generation,
        rejected_generation: status.rejected_generation,
        last_result_code: last_result_code.to_owned(),
        detail: String::new(),
        last_good_age_ms: status
            .last_good_age
            .map_or(0, |age| i64::try_from(age.as_millis()).unwrap_or(i64::MAX)),
    }
}

async fn spawn_metrics_http(
    address: Option<SocketAddr>,
    registry: Arc<MetricsRegistry>,
) -> Result<Option<JoinHandle<()>>, String> {
    let Some(address) = address else {
        return Ok(None);
    };
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| format!("bind metrics endpoint {address}: {error}"))?;
    Ok(Some(tokio::spawn(metrics_http::serve(listener, registry))))
}

#[allow(clippy::too_many_lines)]
fn parse_options(arguments: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut arguments = arguments.into_iter();
    let mut config_file = env::var_os(CONFIG_FILE_ENV).map(PathBuf::from);
    let mut socket = env::var_os(CONTROL_SOCKET_ENV).map(PathBuf::from);
    let mut uid = env::var(CONTROL_UID_ENV)
        .ok()
        .map(|value| parse_uid(&value))
        .transpose()?;
    let mut tls_roots: Vec<PathBuf> = env::var_os(TLS_ROOTS_ENV)
        .map(|value| env::split_paths(&value).collect())
        .unwrap_or_default();
    let mut drain_grace = None;
    let mut health_port: u16 = 0;
    let mut metrics_addr: Option<SocketAddr> = None;
    let mut admin_addr: Option<SocketAddr> = None;
    let mut log_file: Option<PathBuf> = None;
    let mut routing_shadow_socket = None;
    let mut standalone = false;
    let mut socket_argument = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--version" | "-V" => return Ok(Command::Version),
            "--help" | "-h" => return Ok(Command::Help),
            "--integration-capabilities" => return Ok(Command::IntegrationCapabilities),
            "--standalone" => standalone = true,
            "--config" => {
                config_file = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--config requires a path".to_owned())?,
                ));
            }
            "--control-socket" => {
                socket_argument = true;
                socket =
                    Some(PathBuf::from(arguments.next().ok_or_else(|| {
                        "--control-socket requires a path".to_owned()
                    })?));
            }
            "--routing-shadow-socket" => {
                routing_shadow_socket =
                    Some(PathBuf::from(arguments.next().ok_or_else(|| {
                        "--routing-shadow-socket requires a path".to_owned()
                    })?));
            }
            "--control-uid" => {
                uid = Some(parse_uid(
                    &arguments
                        .next()
                        .ok_or_else(|| "--control-uid requires a uid".to_owned())?,
                )?);
            }
            "--tls-root" => tls_roots.push(PathBuf::from(
                arguments
                    .next()
                    .ok_or_else(|| "--tls-root requires a path".to_owned())?,
            )),
            "--health-port" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--health-port requires a port".to_owned())?;
                health_port = value
                    .parse()
                    .map_err(|_| format!("health port must be a u16, got {value:?}"))?;
            }
            "--metrics-addr" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--metrics-addr requires host:port".to_owned())?;
                metrics_addr =
                    Some(value.parse().map_err(|_| {
                        format!("metrics address must be host:port, got {value:?}")
                    })?);
            }
            "--admin-addr" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--admin-addr requires host:port".to_owned())?;
                admin_addr = Some(
                    value
                        .parse()
                        .map_err(|_| format!("admin address must be host:port, got {value:?}"))?,
                );
            }
            "--log-file" => {
                log_file = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| "--log-file requires a path".to_owned())?,
                ));
            }
            "--drain-grace-seconds" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| "--drain-grace-seconds requires a number".to_owned())?;
                let seconds: u64 = value
                    .parse()
                    .map_err(|_| format!("drain grace must be a u64, got {value:?}"))?;
                if seconds > MAX_DRAIN_GRACE_SECONDS {
                    return Err(format!(
                        "--drain-grace-seconds must be at most {MAX_DRAIN_GRACE_SECONDS} \
                         (30 days), got {seconds}"
                    ));
                }
                drain_grace = Some(Duration::from_secs(seconds));
            }
            _ => return Err(format!("unknown argument {argument:?}")),
        }
    }
    if standalone && socket_argument {
        return Err("--standalone must not set a Go control socket".to_owned());
    }
    // A local-mode client object remains for the compatibility-only session
    // adapter. No transport task is started and this path is never opened.
    let control_socket = if standalone {
        PathBuf::from("/dev/null")
    } else {
        socket.ok_or_else(|| format!("--control-socket or {CONTROL_SOCKET_ENV} is required"))?
    };
    if !control_socket.is_absolute() {
        return Err("control socket path must be absolute".to_owned());
    }
    if routing_shadow_socket
        .as_ref()
        .is_some_and(|path| !path.is_absolute() || *path == control_socket)
    {
        return Err("routing shadow socket must be distinct and absolute".to_owned());
    }
    if tls_roots.iter().any(|root| !root.is_absolute()) {
        return Err("TLS allowlist roots must be absolute".to_owned());
    }
    if log_file.as_ref().is_some_and(|path| !path.is_absolute()) {
        return Err("log file path must be absolute".to_owned());
    }
    Ok(Command::Run(Box::new(Options {
        config_file: config_file
            .ok_or_else(|| format!("--config or {CONFIG_FILE_ENV} is required"))?,
        standalone,
        control_socket,
        routing_shadow_socket,
        control_uid: if standalone {
            0
        } else {
            uid.ok_or_else(|| format!("--control-uid or {CONTROL_UID_ENV} is required"))?
        },
        tls_roots,
        drain_grace,
        health_port,
        metrics_addr,
        admin_addr,
        log_file,
    })))
}

fn parse_uid(value: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|_| format!("control uid must be a uint32, got {value:?}"))
}

impl startup::Teardown for legacy_router_shadow::consumer::Task {
    fn teardown(self) -> startup::TeardownFuture {
        Box::pin(async move {
            self.shutdown().await;
        })
    }
}

fn usage() -> &'static str {
    "Usage: tiproxy-rs --config <path> (--standalone | --control-socket <absolute-path> --control-uid <uid>) \
     [--tls-root <absolute-path>]... [--drain-grace-seconds <n>] [--health-port <n>] [--metrics-addr <host:port>] [--admin-addr <host:port>] [--log-file <absolute-path>] [--routing-shadow-socket <absolute-path>]\n\
     Environment: TIPROXY_CONFIG, TIPROXY_CONTROL_SOCKET, TIPROXY_CONTROL_UID, TIPROXY_TLS_ROOTS"
}

fn version_output() -> String {
    format!("tiproxy-rs {VERSION} (commit {COMMIT}, built {BUILD_TIME})")
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;

    use control_config::{ConfigNamespaceSource, ConfigNamespaceStore};

    use super::{
        Command, GenerationStatusSnapshot, INTEGRATION_CAPABILITIES, MAX_DRAIN_GRACE_SECONDS,
        Options, StartupGuard, config_persistence_client, dataplane_status,
        join_modules_with_timeout, parse_options, persistence_options, session_loop_config,
        version_output, wait_module_ready,
    };
    use crate::config_composition::control_config;
    use crate::startup::{Teardown, TeardownFuture};
    use dataplane::control_runtime::SnapshotConsumer;
    use dataplane::{
        DataplaneSnapshotConsumer, MetricsRegistry, SystemMemoryProbe,
        spawn_system_time_monitor_with_clock,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::net::TcpStream;

    use super::{MeteringSampler, RunningProcess, StopJoin};
    use control_plane::{
        ControlConfig, ControlModule, ControlModuleSet, ControlRuntime as InProcessControlRuntime,
        JsonStderrSink, LifecyclePhase, LogLevel, MetricsPolicy, ModuleContext, ModuleFuture,
        OwnershipRegistry,
    };
    use dataplane::metering::MeteringSamplerError;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    /// Reviewer regression (`CodexM5`, `813b3140`): the supervisor observes the
    /// admin server's completion, then hands the same handle to the one
    /// teardown helper; that helper must not poll a completed handle again.
    #[tokio::test]
    async fn review_completed_admin_join_must_not_be_polled_again_during_cleanup() {
        let (shutdown, _stop) = watch::channel(false);
        let mut admin = super::AdminTask {
            task: tokio::spawn(async {}),
            shutdown,
        };
        // This is the same ownership sequence as the admin_exit select branch:
        // observe completion, then pass the retained AdminTask to final cleanup.
        (&mut admin.task)
            .await
            .unwrap_or_else(|error| unreachable!("admin completed normally: {error}"));
        let cleanup = tokio::spawn(admin.teardown()).await;
        assert!(cleanup.is_ok(), "cleanup must not panic: {cleanup:?}");
    }

    #[test]
    fn a_control_failure_is_not_masked_by_the_final_sampler_handoff() {
        assert_eq!(
            super::prefer_control_failure(
                Err("control root cause".to_owned()),
                Err("metering durable dispatch owner unavailable".to_owned()),
            ),
            Err("control root cause".to_owned())
        );
        assert_eq!(
            super::prefer_control_failure(
                Ok(()),
                Err("metering durable dispatch rejected snapshots: Persistence".to_owned()),
            ),
            Err("metering durable dispatch rejected snapshots: Persistence".to_owned())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lb_removal_wait_elapses_but_supervised_fault_preempts_it() {
        let elapsed = super::wait_for_lb_removal_or(
            super::sleep_lb_window(Duration::from_secs(3)),
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(elapsed, None);

        let (fault_tx, fault_rx) = tokio::sync::oneshot::channel();
        let wait = tokio::spawn(super::wait_for_lb_removal_or(
            super::sleep_lb_window(Duration::from_secs(30)),
            async {
                fault_rx
                    .await
                    .unwrap_or_else(|error| unreachable!("fault sender: {error}"))
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!wait.is_finished(), "the listener window is still active");
        fault_tx
            .send("module failed")
            .unwrap_or_else(|_| unreachable!("waiter still exists"));
        assert_eq!(
            wait.await
                .unwrap_or_else(|error| unreachable!("wait task: {error}")),
            Some("module failed")
        );

        let huge =
            super::wait_for_lb_removal_or(super::sleep_lb_window(Duration::MAX), async { "fault" })
                .await;
        assert_eq!(huge, Some("fault"));
    }

    #[test]
    fn lb_readiness_turns_off_while_route_admission_stays_ready() {
        let owner = armed_owner();
        let lb_readiness = super::LbReadiness::new(owner.handle());
        assert!(!lb_readiness.ready());
        owner
            .mark_ready()
            .unwrap_or_else(|error| unreachable!("mark ready: {error}"));
        assert!(lb_readiness.ready());
        lb_readiness.mark_unhealthy();
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Ready);
        assert!(!lb_readiness.ready());
        assert!(lb_readiness.closing());
        owner
            .begin_shutdown(control_plane::ShutdownReason::Signal)
            .unwrap_or_else(|error| unreachable!("begin ordered shutdown: {error}"));
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Quiescing);
        assert!(!lb_readiness.ready());
    }

    async fn bound_serving_with_lb_wait(
        wait_millis: u64,
    ) -> (
        dataplane::DataplaneServingHandle,
        u16,
        tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        use control_proto::snapshot::{SnapshotLineage, SnapshotStore, UnixTime};
        use control_proto::v1::{
            ConfigSnapshot, KeepalivePolicy, Listener, ProxyProtocolMode, StateSnapshot, TlsPolicy,
        };

        let port_socket = std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap_or_else(|error| unreachable!("ephemeral port: {error}"));
        let port = port_socket
            .local_addr()
            .unwrap_or_else(|error| unreachable!("local address: {error}"))
            .port();
        drop(port_socket);
        let keepalive = KeepalivePolicy {
            enabled: true,
            ..KeepalivePolicy::default()
        };
        let raw = StateSnapshot {
            config: Some(ConfigSnapshot {
                max_connections: 10,
                connection_buffer_bytes: 4096,
                frontend_keepalive: Some(keepalive),
                healthy_backend_keepalive: Some(keepalive),
                unhealthy_backend_keepalive: Some(keepalive),
                proxy_protocol: ProxyProtocolMode::Disabled as i32,
                listeners: vec![Listener {
                    address: "127.0.0.1".to_owned(),
                    port: u32::from(port),
                    name: "sql-0".to_owned(),
                }],
                server_version: "TiProxy-test".to_owned(),
                frontend_tls: Some(TlsPolicy::default()),
                backend_tls: Some(TlsPolicy::default()),
                graceful_wait_millis: wait_millis,
                ..ConfigSnapshot::default()
            }),
            ..StateSnapshot::default()
        };
        let store =
            SnapshotStore::new([]).unwrap_or_else(|error| unreachable!("snapshot store: {error}"));
        let snapshot = store
            .apply(
                1,
                raw,
                UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
                SnapshotLineage::for_tests("life001"),
            )
            .unwrap_or_else(|error| unreachable!("validated snapshot: {error}"))
            .snapshot;
        let (seen_tx, seen_rx) = tokio::sync::mpsc::unbounded_channel();
        let handler = Arc::new(move |_: dataplane::AcceptedConnection| {
            let seen_tx = seen_tx.clone();
            async move {
                let _ = seen_tx.send(());
            }
        });
        let (mut consumer, serving) =
            DataplaneSnapshotConsumer::new(Arc::new(SystemMemoryProbe::new()), handler);
        consumer
            .apply(&snapshot, &|| true)
            .await
            .unwrap_or_else(|error| unreachable!("bind serving: {error}"));
        (serving, port, seen_rx)
    }

    #[tokio::test]
    async fn signal_coordinator_keeps_lb_and_session_drain_clocks_separate() {
        let owner = armed_owner();
        owner
            .mark_ready()
            .unwrap_or_else(|error| unreachable!("mark ready: {error}"));
        let readiness = super::LbReadiness::new(owner.handle());
        let (serving, port, mut seen_rx) = bound_serving_with_lb_wait(2_000).await;
        assert!(serving.is_serving().await);
        assert!(readiness.ready());
        assert_eq!(
            serving.graceful_wait_before_shutdown().await,
            Duration::from_secs(2)
        );
        let serving_probe = serving.clone();
        let (drain_tx, mut drain_rx) = watch::channel(None);
        let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
        let (lb_done_tx, lb_done_rx) = tokio::sync::oneshot::channel::<()>();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let coordinator = tokio::spawn({
            let owner = Arc::clone(&owner);
            let readiness = readiness.clone();
            async move {
                super::signal_stop_drain_and_join_sessions(
                    &owner,
                    &serving,
                    &readiness,
                    async {
                        lb_done_rx
                            .await
                            .unwrap_or_else(|error| unreachable!("LB window release: {error}"));
                    },
                    async {
                        ready_tx
                            .send(())
                            .unwrap_or_else(|()| unreachable!("readiness handshake receiver"));
                        std::future::pending::<super::OrderedWaitExit>().await
                    },
                    &drain_tx,
                    &session_shutdown_tx,
                )
                .await
            }
        });
        ready_rx
            .await
            .unwrap_or_else(|error| unreachable!("readiness handshake: {error}"));
        assert!(!readiness.ready());
        assert!(readiness.closing());
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Ready);
        assert_eq!(*drain_rx.borrow(), None);
        assert!(!*session_shutdown_rx.borrow());
        let connection = TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap_or_else(|error| unreachable!("accept during LB wait: {error}"));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), seen_rx.recv())
                .await
                .unwrap_or_else(|error| unreachable!("admission receipt: {error}")),
            Some(())
        );
        drop(connection);

        // Keep real socket I/O on a real clock. The timer itself is tested
        // separately; this production-coordinator test controls the boundary
        // so scheduler load cannot close the listener before the admission.
        lb_done_tx
            .send(())
            .unwrap_or_else(|()| unreachable!("coordinator still waits"));
        tokio::time::timeout(Duration::from_secs(10), async {
            while drain_rx.borrow().is_none() {
                drain_rx
                    .changed()
                    .await
                    .unwrap_or_else(|error| unreachable!("drain signal: {error}"));
            }
        })
        .await
        .unwrap_or_else(|error| unreachable!("drain starts: {error}"));
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Draining);
        assert_eq!(*drain_rx.borrow(), Some(Duration::from_secs(30)));
        assert!(!*session_shutdown_rx.borrow());
        assert!(!serving_probe.is_serving().await);
        let mut refused = false;
        for _ in 0..10 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                refused = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(refused, "SQL listener stops before the session drain grace");

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(!*session_shutdown_rx.borrow());
        tokio::time::advance(Duration::from_secs(30)).await;
        let result = coordinator
            .await
            .unwrap_or_else(|error| unreachable!("coordinator task: {error}"))
            .unwrap_or_else(|error| unreachable!("coordinator: {error}"));
        assert!(result.0.is_ok());
        assert!(result.1.is_none());
        assert!(*session_shutdown_rx.borrow());
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Stopping);
    }

    #[tokio::test(start_paused = true)]
    async fn signal_coordinator_fault_preempts_the_lb_window() {
        let owner = armed_owner();
        owner
            .mark_ready()
            .unwrap_or_else(|error| unreachable!("mark ready: {error}"));
        let readiness = super::LbReadiness::new(owner.handle());
        let (_consumer, serving) = DataplaneSnapshotConsumer::new(
            Arc::new(SystemMemoryProbe::new()),
            Arc::new(|_: dataplane::AcceptedConnection| async {}),
        );
        let (drain_tx, drain_rx) = watch::channel(None);
        let (session_shutdown_tx, _session_shutdown_rx) = watch::channel(false);
        let (fault_tx, fault_rx) = tokio::sync::oneshot::channel();
        let coordinator = tokio::spawn({
            let owner = Arc::clone(&owner);
            let readiness = readiness.clone();
            async move {
                super::signal_stop_drain_and_join_sessions(
                    &owner,
                    &serving,
                    &readiness,
                    super::sleep_lb_window(Duration::from_secs(60)),
                    async {
                        fault_rx
                            .await
                            .unwrap_or_else(|error| unreachable!("fault sender: {error}"))
                    },
                    &drain_tx,
                    &session_shutdown_tx,
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Ready);
        assert_eq!(*drain_rx.borrow(), None);
        fault_tx
            .send(super::OrderedWaitExit::Module("module failed".to_owned()))
            .unwrap_or_else(|_| unreachable!("waiter still exists"));
        tokio::task::yield_now().await;
        assert_eq!(owner.handle().lifecycle().phase, LifecyclePhase::Draining);
        assert_eq!(*drain_rx.borrow(), Some(Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(30)).await;
        let result = coordinator
            .await
            .unwrap_or_else(|error| unreachable!("coordinator task: {error}"))
            .unwrap_or_else(|error| unreachable!("coordinator: {error}"));
        assert!(result.0.is_ok());
        assert!(matches!(result.1, Some(super::OrderedWaitExit::Module(_))));
    }

    type TeardownLog = Arc<Mutex<Vec<&'static str>>>;

    fn record(log: &TeardownLog, label: &'static str) {
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(label);
    }

    #[tokio::test(start_paused = true)]
    async fn module_readiness_is_bounded_and_diagnostic() {
        let timeout = wait_module_ready(
            "route-plane module",
            Duration::from_secs(3),
            std::future::pending::<Result<(), &'static str>>(),
        )
        .await;
        assert_eq!(
            timeout,
            Err("initialize route-plane module: readiness_timeout after 3s".to_owned())
        );

        let closed = wait_module_ready(
            "topology module",
            Duration::from_secs(3),
            std::future::ready(Err::<(), _>("ready channel closed")),
        )
        .await;
        assert_eq!(
            closed,
            Err("initialize topology module: ready channel closed".to_owned())
        );
    }

    /// A fake optional resource that records its slot label when torn down, so a
    /// test can drive the *real* `StartupGuard::rollback` and assert the exact
    /// reverse teardown order and that each resource is torn down once.
    struct FakeResource {
        label: &'static str,
        log: TeardownLog,
    }

    impl Teardown for FakeResource {
        fn teardown(self) -> TeardownFuture {
            Box::pin(async move { record(&self.log, self.label) })
        }
    }

    fn fake(label: &'static str, log: &TeardownLog) -> FakeResource {
        FakeResource {
            label,
            log: Arc::clone(log),
        }
    }

    /// A control module that runs until the runtime reaches `Stopping`, then
    /// records "modules", so a test can prove the guard joined it (rather than
    /// aborting it) after the optional resources and before the owner is
    /// finished.
    struct StoppableModule {
        log: TeardownLog,
    }

    impl ControlModule for StoppableModule {
        fn name(&self) -> &'static str {
            "startup_guard_fake_module"
        }

        fn run(self: Box<Self>, context: ModuleContext) -> ModuleFuture {
            Box::pin(async move {
                let mut lifecycle = context.lifecycle();
                while lifecycle.changed().await.is_ok() {
                    if matches!(
                        lifecycle.borrow().phase,
                        LifecyclePhase::Stopping | LifecyclePhase::Stopped
                    ) {
                        break;
                    }
                }
                record(&self.log, "modules");
                Ok(())
            })
        }
    }

    struct StuckModule;

    impl ControlModule for StuckModule {
        fn name(&self) -> &'static str {
            "stuck_shutdown_test_module"
        }

        fn run(self: Box<Self>, _context: ModuleContext) -> ModuleFuture {
            Box::pin(std::future::pending())
        }
    }

    /// A `Starting` owner (never marked ready — the real state at an early
    /// startup failure).
    fn armed_owner() -> Arc<InProcessControlRuntime> {
        let registry = OwnershipRegistry::new();
        Arc::new(
            InProcessControlRuntime::claim_process(
                &registry,
                "startup-guard-test".to_owned(),
                ControlConfig::new(
                    1,
                    Duration::from_secs(30),
                    0,
                    control_plane::TlsPolicy::default(),
                    LogLevel::Info,
                    MetricsPolicy::default(),
                )
                .unwrap_or_else(|error| unreachable!("control config: {error}")),
                Arc::new(JsonStderrSink),
            )
            .unwrap_or_else(|error| unreachable!("claim process: {error}")),
        )
    }

    fn modules_with_stoppable(
        in_process: &Arc<InProcessControlRuntime>,
        log: &TeardownLog,
    ) -> ControlModuleSet {
        let handle = in_process.handle();
        let mut modules = ControlModuleSet::new(&handle);
        modules
            .spawn(StoppableModule {
                log: Arc::clone(log),
            })
            .unwrap_or_else(|error| unreachable!("spawn config module: {error}"));
        modules
    }

    #[tokio::test]
    async fn module_shutdown_timeout_aborts_and_joins_a_stuck_module() {
        let in_process = armed_owner();
        let mut modules = ControlModuleSet::new(&in_process.handle());
        modules
            .spawn(StuckModule)
            .unwrap_or_else(|error| unreachable!("spawn stuck module: {error}"));

        let timeout = Duration::from_millis(20);
        assert_eq!(
            join_modules_with_timeout(&mut modules, timeout).await,
            Err("control module shutdown exceeded 20ms".to_owned())
        );
        assert!(
            modules.is_empty(),
            "the timeout aborts and joins every module"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routing_observer_is_joined_on_startup_rollback_without_sql_supervision() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        let modules = modules_with_stoppable(&in_process, &log);
        let mut guard = StartupGuard::<FakeResource, FakeResource, FakeResource, FakeResource>::arm(
            Arc::clone(&in_process),
            modules,
        );
        let observer = legacy_router_shadow::consumer::Task::spawn(PathBuf::from(
            "/tmp/absent-routing-shadow.sock",
        ));
        let diagnostics = observer.diagnostics();
        guard.routing_shadow = Some(observer);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            guard.rollback("injected later startup failure".to_owned()),
        )
        .await
        .unwrap_or_else(|error| unreachable!("join observer: {error}"));
        assert_eq!(error, "injected later startup failure");
        assert!(
            diagnostics.snapshot().stopped,
            "startup rollback must cancel and join observer"
        );
    }

    #[test]
    fn routing_observer_cli_is_explicit_and_distinct() {
        let base = [
            "--config",
            "/tmp/tiproxy.toml",
            "--control-socket",
            "/tmp/control.sock",
            "--control-uid",
            "0",
        ];
        let Command::Run(options) = parse_options(base.map(str::to_owned))
            .unwrap_or_else(|error| unreachable!("options: {error}"))
        else {
            unreachable!("run")
        };
        assert!(options.routing_shadow_socket.is_none());
        for path in ["relative.sock", "/tmp/control.sock"] {
            assert!(
                parse_options(
                    base.into_iter()
                        .chain(["--routing-shadow-socket", path])
                        .map(str::to_owned)
                )
                .is_err()
            );
        }
        let Command::Run(options) = parse_options(
            base.into_iter()
                .chain(["--routing-shadow-socket", "/tmp/observer.sock"])
                .map(str::to_owned),
        )
        .unwrap_or_else(|error| unreachable!("observer options: {error}")) else {
            unreachable!("run")
        };
        assert_eq!(
            options.routing_shadow_socket,
            Some(PathBuf::from("/tmp/observer.sock"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cp_cfg_ready_failure_joins_the_module_then_finishes_the_owner() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        let modules = modules_with_stoppable(&in_process, &log);
        // No optional resource is set (the failure is before any was acquired):
        // give the four generic slots a concrete type.
        let guard = StartupGuard::<FakeResource, FakeResource, FakeResource, FakeResource>::arm(
            Arc::clone(&in_process),
            modules,
        );

        let error = guard
            .rollback("initialize config owner: injected".to_owned())
            .await;
        assert_eq!(error, "initialize config owner: injected");
        assert_eq!(
            *log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec!["modules"],
            "only the module set is torn down; it is joined, not abandoned"
        );
        assert!(
            in_process.finish().is_err(),
            "the owner was already finished by rollback"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_health_bind_failure_rolls_back_optionals_without_health_then_module_and_owner() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        // The health task was never spawned, so annotate the unset fourth slot.
        let mut guard = StartupGuard::<FakeResource, FakeResource, FakeResource, FakeResource>::arm(
            Arc::clone(&in_process),
            modules_with_stoppable(&in_process, &log),
        );
        // A health-bind failure: the runtime, exporter, and sampler are live, but
        // the health task was never spawned.
        guard.set_runtime(fake("legacy_runtime", &log));
        guard.set_metrics_exporter(fake("metrics_exporter", &log));
        guard.set_metering_sampler(fake("metering_sampler", &log));

        let error = guard
            .rollback("bind health endpoint: injected".to_owned())
            .await;
        assert_eq!(error, "bind health endpoint: injected");
        assert_eq!(
            *log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                "metering_sampler",
                "metrics_exporter",
                "legacy_runtime",
                "modules"
            ],
            "optionals retire latest-first (no health), then the module, then the owner"
        );
        assert!(in_process.finish().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mark_ready_failure_rolls_back_every_resource_in_reverse_order() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        let mut guard = StartupGuard::arm(
            Arc::clone(&in_process),
            modules_with_stoppable(&in_process, &log),
        );
        // A mark-ready failure: every optional resource is live.
        guard.set_runtime(fake("legacy_runtime", &log));
        guard.set_metrics_exporter(fake("metrics_exporter", &log));
        guard.set_metering_sampler(fake("metering_sampler", &log));
        guard.set_health_task(fake("health_task", &log));

        let error = guard.rollback("mark ready: injected".to_owned()).await;
        assert_eq!(error, "mark ready: injected");
        assert_eq!(
            *log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                "health_task",
                "metering_sampler",
                "metrics_exporter",
                "legacy_runtime",
                "modules",
            ],
            "all optionals retire latest-first, then the module, then the owner"
        );
        assert!(in_process.finish().is_err());
    }

    /// The monitor is a background producer, so a failed startup has to stop
    /// and join it rather than leave it ticking against a dead process. A unit
    /// test of the loop and a fixed golden cannot show this: only the guard's
    /// own rollback path can.
    #[tokio::test(start_paused = true)]
    async fn a_failed_startup_stops_the_system_time_monitor() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        let mut guard = StartupGuard::arm(
            Arc::clone(&in_process),
            modules_with_stoppable(&in_process, &log),
        );
        guard.set_runtime(fake("legacy_runtime", &log));
        guard.set_metrics_exporter(fake("metrics_exporter", &log));
        guard.set_metering_sampler(fake("metering_sampler", &log));
        guard.set_health_task(fake("health_task", &log));

        // The clock closure counts every sample, so the task's liveness is
        // observable from outside without reaching into its internals.
        let samples = Arc::new(AtomicU64::new(0));
        let counted = {
            let samples = Arc::clone(&samples);
            move || {
                samples.fetch_add(1, Ordering::Relaxed);
                0_i128
            }
        };
        guard.set_system_time_monitor(spawn_system_time_monitor_with_clock(
            Arc::new(MetricsRegistry::new()),
            counted,
        ));
        tokio::task::yield_now().await;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            samples.load(Ordering::Relaxed) > 0,
            "the monitor must be sampling before the rollback"
        );

        let error = guard.rollback("mark ready: injected".to_owned()).await;
        assert_eq!(error, "mark ready: injected");
        let after_rollback = samples.load(Ordering::Relaxed);
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            samples.load(Ordering::Relaxed),
            after_rollback,
            "rollback must stop and join the monitor, not leave it ticking"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_normal_commit_transfers_every_resource_and_disarms() {
        let log: TeardownLog = Arc::new(Mutex::new(Vec::new()));
        let in_process = armed_owner();
        let modules = ControlModuleSet::new(&in_process.handle());
        let mut guard = StartupGuard::arm(Arc::clone(&in_process), modules);
        guard.set_runtime(fake("legacy_runtime", &log));
        guard.set_metrics_exporter(fake("metrics_exporter", &log));
        guard.set_metering_sampler(fake("metering_sampler", &log));
        guard.set_health_task(fake("health_task", &log));

        let observer = legacy_router_shadow::consumer::Task::spawn(PathBuf::from(
            "/tmp/absent-routing-shadow.sock",
        ));
        let diagnostics = observer.diagnostics();
        guard.routing_shadow = Some(observer);

        // Commit hands every resource out (and disarms cleanly — no drop bomb).
        let RunningProcess {
            modules: _modules,
            runtime,
            metrics_exporter,
            metering_sampler,
            health_task,
            metrics_http_task: _,
            system_time_monitor: _,
            log_reload_task: _,
            admin_task: _,
            routing_shadow,
        } = guard.commit();
        // Tearing the transferred handles down proves they were moved out of the
        // guard rather than dropped by commit.
        routing_shadow
            .unwrap_or_else(|| unreachable!("observer transferred"))
            .shutdown()
            .await;
        assert!(
            diagnostics.snapshot().stopped,
            "normal commit transfers the owned observer for joined shutdown"
        );
        runtime.teardown().await;
        metrics_exporter.teardown().await;
        metering_sampler.teardown().await;
        health_task
            .unwrap_or_else(|| unreachable!("health task transferred"))
            .teardown()
            .await;
        assert_eq!(
            *log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                "legacy_runtime",
                "metrics_exporter",
                "metering_sampler",
                "health_task",
            ],
            "every resource was transferred out of the guard by commit"
        );
    }

    #[tokio::test]
    async fn the_teardown_sequence_signals_stop_then_awaits_join() {
        // The shared two-phase adapter every production resource uses: dropping
        // either the stop or the awaited join is caught here.
        struct Probe {
            stopped: Arc<AtomicBool>,
            joined: Arc<AtomicBool>,
        }
        impl StopJoin for Probe {
            fn stop(&self) {
                self.stopped.store(true, Ordering::SeqCst);
            }
            fn join(self) -> TeardownFuture {
                Box::pin(async move { self.joined.store(true, Ordering::SeqCst) })
            }
        }

        let stopped = Arc::new(AtomicBool::new(false));
        let joined = Arc::new(AtomicBool::new(false));
        Probe {
            stopped: Arc::clone(&stopped),
            joined: Arc::clone(&joined),
        }
        .teardown()
        .await;
        assert!(stopped.load(Ordering::SeqCst), "stop was signalled");
        assert!(
            joined.load(Ordering::SeqCst),
            "join was awaited (its body only runs when awaited)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_metering_sampler_teardown_signals_then_joins() {
        // The task only finishes after the stop signal, and takes a bounded
        // moment to do so, so a dropped stop hangs (caught by the timeout) and a
        // dropped join returns before the task finishes (caught by the elapsed
        // floor and completion flag).
        let (shutdown, mut receiver) = watch::channel(false);
        let joined = Arc::new(AtomicBool::new(false));
        let joined_in_task = Arc::clone(&joined);
        let task: JoinHandle<Result<(), MeteringSamplerError>> = tokio::spawn(async move {
            while !*receiver.borrow_and_update() {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            joined_in_task.store(true, Ordering::SeqCst);
            Ok(())
        });
        let sampler = MeteringSampler { task, shutdown };

        let start = Instant::now();
        let Ok(()) = tokio::time::timeout(Duration::from_secs(5), sampler.teardown()).await else {
            unreachable!("teardown must not hang: the stop signal was delivered");
        };
        assert!(
            joined.load(Ordering::SeqCst),
            "the sampler task ran to completion (join awaited it)"
        );
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "teardown waited for the task rather than detaching it"
        );
    }

    #[tokio::test]
    async fn the_health_task_teardown_aborts_then_joins() {
        // A never-completing task with a drop flag: abort + await cancels it and
        // joins it, so its guard drops. A dropped abort hangs (timeout); a
        // dropped join returns before the cancellation completes, so on the
        // single-threaded runtime the guard has not dropped yet.
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = Arc::clone(&dropped);
        let (started, wait_started) = tokio::sync::oneshot::channel();
        let task: JoinHandle<()> = tokio::spawn(async move {
            let _guard = DropFlag(dropped_in_task);
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        // Ensure the task has run past constructing its guard before it is
        // aborted, so the abort actually cancels a live guard.
        let Ok(()) = wait_started.await else {
            unreachable!("the health task started");
        };

        let Ok(()) = tokio::time::timeout(Duration::from_secs(5), task.teardown()).await else {
            unreachable!("teardown must not hang: the task was aborted");
        };
        assert!(
            dropped.load(Ordering::SeqCst),
            "the health task was aborted and joined (its guard dropped)"
        );
    }

    #[test]
    fn version_output_labels_all_build_metadata() {
        let output = version_output();
        assert!(output.starts_with("tiproxy-rs "));
        assert!(output.contains(" (commit "));
        assert!(output.contains(", built "));
    }

    #[test]
    fn every_post_spawn_startup_failure_routes_through_the_one_rollback_seam() {
        // A source contract for the composition root: between arming the startup
        // guard and committing it, every failure must exit through
        // `guard.rollback` and there must be no bare `?`/`.map_err(..)?` that
        // bypasses it, so a new fallible acquisition cannot silently leak an
        // already-started resource.
        let source = include_str!("main.rs");
        let armed = source
            .find("// STARTUP-GUARD:ARMED")
            .unwrap_or_else(|| unreachable!("armed marker present"));
        let commit = source
            .find("// STARTUP-GUARD:COMMIT")
            .unwrap_or_else(|| unreachable!("commit marker present"));
        assert!(commit > armed, "commit marker follows the armed marker");
        let region = &source[armed..commit];

        // Every failure exit in the guarded region goes through the single seam.
        // (`.rollback(` is matched contiguously; `guard` may sit on the prior
        // wrapped line.)
        let returns = region.matches("return Err(").count();
        let rollbacks = region.matches(".rollback(").count();
        assert!(returns > 0, "the guarded region has failure exits");
        assert_eq!(
            returns, rollbacks,
            "every guarded failure exit must call guard.rollback exactly once"
        );

        // No `?` operator may bypass the guard inside the region.
        assert!(
            !region.contains(")?"),
            "no `?` operator may bypass the guard in the startup region"
        );
        assert!(
            !region.contains("?;"),
            "no `?` operator may bypass the guard in the startup region"
        );
        assert_eq!(
            region.matches("wait_module_ready(").count(),
            4,
            "config, topology, route-plane, and native meter peer readiness are deadline-bounded"
        );
        for module in [
            "config owner",
            "topology module",
            "route-plane module",
            "native meter peer",
        ] {
            assert!(
                region.contains(&format!("\"{module}\"")),
                "the readiness diagnostic names {module}"
            );
        }
        assert!(
            region.contains("bind metric owner endpoint"),
            "an occupied fixed owner port reaches startup rollback with a stable diagnostic"
        );
    }

    #[test]
    fn parses_operational_cli() {
        let command = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--tls-root".to_owned(),
            "/etc/tiproxy/tls".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(
            *options,
            Options {
                routing_shadow_socket: None,
                config_file: PathBuf::from("/etc/tiproxy/tiproxy.toml"),
                standalone: false,
                control_socket: PathBuf::from("/tmp/control.sock"),
                control_uid: 42,
                tls_roots: vec![PathBuf::from("/etc/tiproxy/tls")],
                drain_grace: None,
                health_port: 0,
                metrics_addr: None,
                admin_addr: None,
                log_file: None,
            }
        );
    }

    #[test]
    fn standalone_cli_needs_no_go_socket_or_uid() {
        let command = parse_options([
            "--standalone".to_owned(),
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("standalone process only needs its own config file")
        };
        assert!(options.standalone);
        assert_eq!(options.control_socket, PathBuf::from("/dev/null"));
        assert_eq!(options.control_uid, 0);
    }

    #[test]
    fn drain_grace_is_the_session_drain_deadline() {
        let command = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--drain-grace-seconds".to_owned(),
            "45".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(options.drain_grace, Some(Duration::from_secs(45)));
        assert_eq!(
            session_loop_config(options.drain_grace.unwrap_or_default()).drain_deadline,
            Duration::from_secs(45),
            "one lineage: the CLI grace is the per-session FSM deadline"
        );
    }

    #[test]
    fn integration_capabilities_are_the_honest_contract() {
        let Ok(Command::IntegrationCapabilities) =
            parse_options(["--integration-capabilities".to_owned()])
        else {
            unreachable!("the capability probe needs no other arguments")
        };
        assert_eq!(
            INTEGRATION_CAPABILITIES,
            "in-process-control-runtime,control-bridge-v1,rust-route-owner,rust-meter-owner,rust-api-owner,mysql-listener,health-endpoint,graceful-shutdown,tls,proxy-v2,zlib,zstd",
            "only what the binary truthfully provides: Rust route ownership plus the wired plain slice, TLS, PROXY v2, and compression"
        );
        for wired in [
            "rust-route-owner",
            "rust-meter-owner",
            "rust-api-owner",
            "tls",
            "proxy-v2",
            "zlib",
            "zstd",
        ] {
            assert!(
                INTEGRATION_CAPABILITIES.contains(wired),
                "{wired:?} is wired (WIRE-activation A1/B/C), so it must be advertised"
            );
        }
    }

    #[test]
    fn operational_cli_projects_into_rust_control_domain() {
        let options = Options {
            routing_shadow_socket: None,
            config_file: PathBuf::from("/etc/tiproxy/tiproxy.toml"),
            standalone: false,
            control_socket: PathBuf::from("/tmp/control.sock"),
            control_uid: 42,
            tls_roots: vec![PathBuf::from("/etc/tiproxy/tls")],
            drain_grace: Some(Duration::from_secs(45)),
            health_port: 8081,
            metrics_addr: None,
            admin_addr: None,
            log_file: None,
        };
        let source = ConfigNamespaceStore::from_toml(
            b"enable-traffic-replay = false\n",
            None,
            std::path::Path::new("/tmp"),
        )
        .unwrap_or_else(|error| unreachable!("valid source: {error}"));
        let config = control_config(
            source.current().as_ref(),
            options.health_port,
            &options.tls_roots,
            options.drain_grace,
        )
        .unwrap_or_else(|error| unreachable!("valid control config: {error}"));
        assert_eq!(config.generation(), 1);
        assert_eq!(
            config.drain_grace(),
            options.drain_grace.unwrap_or_default()
        );
        assert_eq!(config.health_port(), options.health_port);
        assert_eq!(config.tls().roots(), options.tls_roots);
    }

    #[test]
    fn persistence_uses_restart_pinned_legacy_pd_addrs() {
        let source = ConfigNamespaceStore::from_toml(
            br#"
[proxy]
pd-addrs = "owner-pd:2379"

[[proxy.backend-clusters]]
name = "a-first"
pd-addrs = "routing-pd:2379"
"#,
            None,
            std::path::Path::new("/tmp"),
        )
        .unwrap_or_else(|error| unreachable!("valid source: {error}"));
        let (client, election) = persistence_options(
            source.current().as_ref(),
            "process-a",
            &crate::tls_material::open_tls_roots(&[]),
        )
        .unwrap_or_else(|error| unreachable!("valid persistence options: {error}"));
        let client = client.unwrap_or_else(|| unreachable!("persistence is configured"));
        assert_eq!(client.endpoints(), ["http://owner-pd:2379"]);
        assert!(election.is_some());
    }

    #[test]
    fn config_persistence_rejects_a_same_root_symlink_ca() {
        // The persistence production path must use the safe read: a symlink CA is
        // rejected. A bare read would follow the link and build a client.
        let dir = std::env::temp_dir();
        let real = dir.join(format!("cptopo-persist-real-{}.pem", std::process::id()));
        std::fs::write(&real, b"ca").unwrap_or_else(|error| unreachable!("write: {error}"));
        let link = dir.join(format!("cptopo-persist-link-{}.pem", std::process::id()));
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&real, &link)
            .unwrap_or_else(|error| unreachable!("symlink: {error}"));
        let toml = format!(
            "[proxy]\npd-addrs = \"owner-pd:2379\"\n[security.cluster-tls]\nca = \"{}\"\n",
            link.display()
        );
        let source =
            ConfigNamespaceStore::from_toml(toml.as_bytes(), None, std::path::Path::new("/tmp"))
                .unwrap_or_else(|error| unreachable!("valid source: {error}"));
        let roots = crate::tls_material::open_tls_roots(&[dir]);
        assert!(
            config_persistence_client(source.current().effective(), &roots).is_err(),
            "a symlink CA must be rejected by the persistence reader"
        );
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&real);
    }

    #[test]
    fn config_persistence_rejects_an_oversize_ca() {
        let dir = std::env::temp_dir();
        let ca = dir.join(format!("cptopo-persist-big-{}.pem", std::process::id()));
        let file =
            std::fs::File::create(&ca).unwrap_or_else(|error| unreachable!("create: {error}"));
        // A sparse file over the 16 MiB bound.
        file.set_len(17 * 1024 * 1024)
            .unwrap_or_else(|error| unreachable!("set_len: {error}"));
        let toml = format!(
            "[proxy]\npd-addrs = \"owner-pd:2379\"\n[security.cluster-tls]\nca = \"{}\"\n",
            ca.display()
        );
        let source =
            ConfigNamespaceStore::from_toml(toml.as_bytes(), None, std::path::Path::new("/tmp"))
                .unwrap_or_else(|error| unreachable!("valid source: {error}"));
        let roots = crate::tls_material::open_tls_roots(&[dir]);
        assert!(
            config_persistence_client(source.current().effective(), &roots).is_err(),
            "an oversize CA must be rejected by the persistence reader"
        );
        let _ = std::fs::remove_file(&ca);
    }

    #[test]
    fn persistence_rejects_skip_ca_instead_of_silently_downgrading_to_plaintext() {
        let source = ConfigNamespaceStore::from_toml(
            b"[security.cluster-tls]\nskip-ca = true\n",
            None,
            std::path::Path::new("/tmp"),
        )
        .unwrap_or_else(|error| unreachable!("valid source model: {error}"));
        assert!(
            config_persistence_client(
                source.current().effective(),
                &crate::tls_material::open_tls_roots(&[])
            )
            .is_err(),
            "skip-ca without a CA must not silently construct a plaintext etcd client"
        );
    }

    #[test]
    fn parses_health_port() {
        let command = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--health-port".to_owned(),
            "8081".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(options.health_port, 8081);
        assert_eq!(options.metrics_addr, None);
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "/tmp/control.sock".to_owned(),
                "--health-port".to_owned(),
                "not-a-port".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_admin_addr() {
        let command = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--admin-addr".to_owned(),
            "127.0.0.1:3082".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(
            options.admin_addr,
            "127.0.0.1:3082".parse::<SocketAddr>().ok(),
            "the admin address is a plain socket address"
        );
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "/tmp/control.sock".to_owned(),
                "--admin-addr".to_owned(),
                "3082".to_owned(),
            ])
            .is_err(),
            "a bare port is not an address"
        );
    }

    #[test]
    fn dataplane_status_projection_follows_the_go_shape() {
        let mut status = GenerationStatusSnapshot {
            applied_generation: 3,
            rejected_generation: 0,
            composition_generation: 4,
            last_good_age: Some(Duration::from_millis(1500)),
            ..Default::default()
        };
        let projected = dataplane_status(&status);
        assert_eq!(
            projected.to_json(),
            "{\"applied_generation\":3,\"desired_generation\":4,\"detail\":\"\",\"enabled\":true,\"last_good_age_ms\":1500,\"last_result_code\":\"ERROR_CODE_OK\",\"rejected_generation\":0,\"sent_generation\":4}"
        );
        status.rejected_generation = 5;
        assert_eq!(
            dataplane_status(&status).last_result_code,
            "ERROR_CODE_INVALID_SNAPSHOT"
        );
        assert_eq!(
            dataplane_status(&GenerationStatusSnapshot::default()).last_result_code,
            "ERROR_CODE_UNSPECIFIED"
        );
    }

    #[test]
    fn parses_metrics_addr() {
        let command = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--metrics-addr".to_owned(),
            "0.0.0.0:3081".to_owned(),
        ]);
        let Ok(Command::Run(options)) = command else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(
            options.metrics_addr,
            "0.0.0.0:3081".parse::<SocketAddr>().ok(),
            "the metrics address is a plain socket address"
        );
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "/tmp/control.sock".to_owned(),
                "--control-uid".to_owned(),
                "42".to_owned(),
                "--metrics-addr".to_owned(),
                "3081".to_owned(),
            ])
            .is_err(),
            "a bare port is not a socket address"
        );
    }

    #[test]
    fn parses_log_file_and_requires_an_absolute_path() {
        let Ok(Command::Run(options)) = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--log-file".to_owned(),
            "/var/log/tiproxy-rs.log".to_owned(),
        ]) else {
            unreachable!("valid operational arguments")
        };
        assert_eq!(
            options.log_file,
            Some(PathBuf::from("/var/log/tiproxy-rs.log"))
        );
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "/tmp/control.sock".to_owned(),
                "--control-uid".to_owned(),
                "42".to_owned(),
                "--log-file".to_owned(),
                "tiproxy-rs.log".to_owned(),
            ])
            .is_err(),
            "a relative log file would depend on the working directory"
        );
    }
    #[test]
    fn rejects_over_bound_drain_grace() {
        let Err(error) = parse_options([
            "--config".to_owned(),
            "/etc/tiproxy/tiproxy.toml".to_owned(),
            "--control-socket".to_owned(),
            "/tmp/control.sock".to_owned(),
            "--control-uid".to_owned(),
            "42".to_owned(),
            "--drain-grace-seconds".to_owned(),
            (MAX_DRAIN_GRACE_SECONDS + 1).to_string(),
        ]) else {
            unreachable!("an over-bound grace must be rejected")
        };
        assert!(error.contains("at most"), "the bound is named: {error}");
    }

    #[test]
    fn rejects_relative_or_incomplete_cli() {
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "control.sock".to_owned(),
                "--control-uid".to_owned(),
                "42".to_owned(),
            ])
            .is_err()
        );
        assert!(
            parse_options([
                "--config".to_owned(),
                "/etc/tiproxy/tiproxy.toml".to_owned(),
                "--control-socket".to_owned(),
                "/tmp/control.sock".to_owned(),
            ])
            .is_err()
        );
    }
}
