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

//! Production file/etcd owner module for configuration and namespaces.

use std::collections::BTreeMap;
use std::future::{Future, pending};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use control_etcd::{
    ElectionConfig, ElectionError, ElectionSession, ElectionState, PersistentPutMode,
    RecoveryOutcome,
};
use control_external::{EtcdClientConfig, EtcdConnector, EtcdOperationError};
use control_plane::{
    ControlModule, LifecyclePhase, ModuleContext, ModuleError, ModuleFuture, OwnerToken,
};
use etcd_client::{EventType, GetOptions, WatchOptions};
use serde_json::json;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::model::{
    ConfigError, EffectiveConfig, LogOnlineConfig, NamespaceConfig, ProxyOnlineConfig,
};
use crate::source::{
    CONFIG_PREFIX, CandidateValidator, ConfigNamespaceSource, ConfigNamespaceStore, LOG_CONFIG_KEY,
    NAMESPACE_CONFIG_PREFIX, PROXY_CONFIG_KEY, PreparedArtifact, StoreError,
    decode_persistent_entries,
};

const FILE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const ETCD_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const RAW_CANDIDATE_CAPACITY: usize = 32;
const MUTATION_CAPACITY: usize = 32;

/// Construction policy for the Rust configuration/namespace owner.
#[derive(Clone)]
pub struct ConfigModuleOptions {
    /// Optional `TiProxy` TOML file. Absence uses Go-compatible defaults.
    pub config_file: Option<PathBuf>,
    /// Command-line advertise-address override.
    pub advertise_addr: Option<String>,
    /// Working directory used to resolve the default workdir value.
    pub current_dir: PathBuf,
    /// Optional persistent /config dependency. Absence disables persistence.
    pub etcd: Option<EtcdClientConfig>,
    /// Election policy required when persistence is enabled.
    pub election: Option<ElectionConfig>,
    /// Optional factory that rebuilds the PD/etcd client after accepted
    /// `security.cluster-tls` configuration or material changes.
    pub persistence_factory: Option<Arc<dyn PersistenceClientFactory>>,
}

/// Rebuilds the config owner's PD/etcd transport from an accepted effective
/// configuration. The binary owns TLS file loading; this domain module owns
/// replacement of its reader and election session.
pub trait PersistenceClientFactory: Send + Sync {
    /// Builds the complete current client, or `None` when persistence is
    /// disabled.
    ///
    /// # Errors
    ///
    /// Returns a redacted construction failure.
    fn build(&self, effective: &EffectiveConfig) -> Result<Option<EtcdClientConfig>, String>;
}

impl<F> PersistenceClientFactory for F
where
    F: Fn(&EffectiveConfig) -> Result<Option<EtcdClientConfig>, String> + Send + Sync,
{
    fn build(&self, effective: &EffectiveConfig) -> Result<Option<EtcdClientConfig>, String> {
        self(effective)
    }
}

struct ModuleCandidateValidator {
    candidate: Option<Arc<dyn CandidateValidator>>,
    persistence: Option<Arc<dyn PersistenceClientFactory>>,
}

impl CandidateValidator for ModuleCandidateValidator {
    fn validate(
        &self,
        effective: &EffectiveConfig,
        namespaces: &[NamespaceConfig],
    ) -> Result<PreparedArtifact, &'static str> {
        // The inner candidate owns the prepared artifact; the persistence probe
        // is a side validation that prepares nothing.
        let prepared = if let Some(candidate) = &self.candidate {
            candidate.validate(effective, namespaces)?
        } else {
            PreparedArtifact::empty()
        };
        if let Some(persistence) = &self.persistence {
            persistence
                .build(effective)
                .map_err(|_| "persistence_transport_rejected")?;
        }
        Ok(prepared)
    }
}

/// Bounded caller-visible mutation failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ConfigMutationError {
    /// The process was started without persistent /config ownership.
    #[error("persistent configuration is disabled")]
    PersistenceDisabled,
    /// This process is not the confirmed current config election owner.
    #[error("configuration mutation requires current leadership")]
    NotLeader,
    /// The mutation key/value was invalid or could not be encoded.
    #[error("invalid configuration mutation")]
    Invalid,
    /// The etcd dependency did not commit the mutation.
    #[error("configuration persistence is unavailable")]
    Unavailable,
    /// The module has stopped accepting mutations.
    #[error("configuration module is stopped")]
    Stopped,
    /// The document changes a restart-required field; nothing was applied.
    #[error("configuration mutation requires a restart")]
    RestartRequired,
}

enum Mutation {
    Proxy(ProxyOnlineConfig),
    Log(LogOnlineConfig),
    Namespace(NamespaceConfig),
    DeleteNamespace(String),
    /// A partial TOML document applied to this process's file base.
    LocalToml(Vec<u8>),
}

struct MutationRequest {
    mutation: Mutation,
    result: oneshot::Sender<Result<(), ConfigMutationError>>,
}

#[derive(PartialEq, Eq)]
struct ExternalMaterialFingerprint(Vec<(PathBuf, ExternalMaterialState)>);

#[derive(PartialEq, Eq)]
enum ExternalMaterialState {
    Readable(Vec<u8>),
    Unavailable,
}

/// Cloneable source and owner-fenced mutation surface for CP-ADMIN/CP-ROUTE.
#[derive(Clone)]
pub struct ConfigModuleHandle {
    source: ConfigNamespaceStore,
    mutations: mpsc::Sender<MutationRequest>,
    persistence_enabled: bool,
    ready: tokio::sync::watch::Receiver<bool>,
}

impl ConfigModuleHandle {
    /// Returns the one process-local immutable config/namespace source.
    #[must_use]
    pub const fn source(&self) -> &ConfigNamespaceStore {
        &self.source
    }

    /// Waits until the initial persistent `/config` range read has been
    /// decoded and incorporated. A file-only owner is ready immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigMutationError::Stopped`] if the owner exits before
    /// publishing a complete initial view.
    pub async fn wait_ready(&self) -> Result<(), ConfigMutationError> {
        let mut ready = self.ready.clone();
        while !*ready.borrow_and_update() {
            ready
                .changed()
                .await
                .map_err(|_| ConfigMutationError::Stopped)?;
        }
        Ok(())
    }

    /// Whether the initial persistent `/config` view has been incorporated
    /// (always true for a file-only owner). Non-blocking counterpart of
    /// [`Self::wait_ready`] for readiness probes.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        *self.ready.borrow()
    }

    /// Applies a partial TOML document to this process's file base exactly as
    /// Go's `ConfigManager.SetTOMLConfig` does for `PUT /api/admin/config/`:
    /// omitted fields keep their values, explicitly supplied zero values
    /// overwrite, the complete candidate is validated, and a change to a
    /// restart-required field rejects the whole document. The persistent
    /// etcd overlay still applies on top. This is instance-scoped like the Go
    /// API and works without persistence.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigMutationError::RestartRequired`], `Invalid`, or
    /// `Stopped`.
    pub async fn apply_local_toml(&self, data: Vec<u8>) -> Result<(), ConfigMutationError> {
        let (result, response) = oneshot::channel();
        self.mutations
            .send(MutationRequest {
                mutation: Mutation::LocalToml(data),
                result,
            })
            .await
            .map_err(|_| ConfigMutationError::Stopped)?;
        response.await.unwrap_or(Err(ConfigMutationError::Stopped))
    }

    /// Persists the dynamic proxy subset under /config/proxy.
    ///
    /// # Errors
    ///
    /// Returns a bounded ownership, validation, dependency, or lifecycle error.
    pub async fn set_proxy(&self, value: ProxyOnlineConfig) -> Result<(), ConfigMutationError> {
        self.send(Mutation::Proxy(value)).await
    }

    /// Persists the dynamic log subset under /config/log.
    ///
    /// # Errors
    ///
    /// Returns a bounded ownership, validation, dependency, or lifecycle error.
    pub async fn set_log(&self, value: LogOnlineConfig) -> Result<(), ConfigMutationError> {
        self.send(Mutation::Log(value)).await
    }

    /// Persists one namespace under its exact historical key.
    ///
    /// # Errors
    ///
    /// Returns a bounded ownership, validation, dependency, or lifecycle error.
    pub async fn set_namespace(&self, value: NamespaceConfig) -> Result<(), ConfigMutationError> {
        self.send(Mutation::Namespace(value)).await
    }

    /// Deletes one namespace from its exact historical key.
    ///
    /// # Errors
    ///
    /// Returns a bounded ownership, validation, dependency, or lifecycle error.
    pub async fn delete_namespace(&self, name: String) -> Result<(), ConfigMutationError> {
        self.send(Mutation::DeleteNamespace(name)).await
    }

    async fn send(&self, mutation: Mutation) -> Result<(), ConfigMutationError> {
        if !self.persistence_enabled {
            return Err(ConfigMutationError::PersistenceDisabled);
        }
        let (result, response) = oneshot::channel();
        self.mutations
            .send(MutationRequest { mutation, result })
            .await
            .map_err(|_| ConfigMutationError::Stopped)?;
        response.await.unwrap_or(Err(ConfigMutationError::Stopped))
    }
}

/// File/etcd control module that owns the only published config generation.
pub struct ConfigModule {
    options: ConfigModuleOptions,
    source: ConfigNamespaceStore,
    file_content: Vec<u8>,
    file_revision: u64,
    external_material: ExternalMaterialFingerprint,
    mutations: mpsc::Receiver<MutationRequest>,
    ready: tokio::sync::watch::Sender<bool>,
}

impl ConfigModule {
    /// Parses and installs real generation one before returning the module.
    ///
    /// # Errors
    ///
    /// Returns file-read, option-shape, parse, or validation failures. When
    /// persistence is enabled an election policy is mandatory.
    pub fn load(options: ConfigModuleOptions) -> Result<(Self, ConfigModuleHandle), StoreError> {
        Self::load_inner(options, None)
    }

    /// Parses generation one with an additional process-composition validator.
    ///
    /// # Errors
    ///
    /// Returns the same bounded option, file, parse, or validation failures as
    /// [`Self::load`].
    pub fn load_with_validator(
        options: ConfigModuleOptions,
        validator: Arc<dyn CandidateValidator>,
    ) -> Result<(Self, ConfigModuleHandle), StoreError> {
        Self::load_inner(options, Some(validator))
    }

    fn load_inner(
        options: ConfigModuleOptions,
        validator: Option<Arc<dyn CandidateValidator>>,
    ) -> Result<(Self, ConfigModuleHandle), StoreError> {
        if options.etcd.is_some() != options.election.is_some() {
            return Err(StoreError::PersistentEntry {
                class: "etcd_election_option_mismatch",
            });
        }
        let file_content = match &options.config_file {
            Some(path) => std::fs::read(path).map_err(|_| StoreError::PersistentEntry {
                class: "config_file_unavailable",
            })?,
            None => Vec::new(),
        };
        // Transport construction is part of candidate validation. Otherwise a
        // reload could publish a new cluster-TLS generation and only then
        // discover that the config owner's replacement etcd client cannot be
        // built, violating the atomic last-good contract.
        let validator = Arc::new(ModuleCandidateValidator {
            candidate: validator,
            persistence: options.persistence_factory.clone(),
        });
        let source = ConfigNamespaceStore::from_toml_with_validator(
            &file_content,
            options.advertise_addr.as_deref(),
            &options.current_dir,
            validator,
        )?;
        if let Some(factory) = &options.persistence_factory {
            let rebuilt = factory.build(source.current().effective()).map_err(|_| {
                StoreError::PersistentEntry {
                    class: "persistence_transport_rejected",
                }
            })?;
            if rebuilt != options.etcd {
                return Err(StoreError::PersistentEntry {
                    class: "persistence_transport_mismatch",
                });
            }
        }
        let external_material = external_material_fingerprint(&source);
        let (mutation_tx, mutations) = mpsc::channel(MUTATION_CAPACITY);
        let (ready, ready_rx) = tokio::sync::watch::channel(options.etcd.is_none());
        let handle = ConfigModuleHandle {
            source: source.clone(),
            mutations: mutation_tx,
            persistence_enabled: options.etcd.is_some(),
            ready: ready_rx,
        };
        Ok((
            Self {
                options,
                source,
                file_content,
                file_revision: 1,
                external_material,
                mutations,
                ready,
            },
            handle,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(mut self, context: ModuleContext) -> Result<(), ModuleError> {
        let mut lifecycle = context.lifecycle();
        let (mut file_tick, mut election_tick) = module_ticks(&self.options);

        let (candidate_tx, mut candidates) = mpsc::channel(RAW_CANDIDATE_CAPACITY);
        let mut reader = spawn_etcd_reader(
            self.options.etcd.as_ref(),
            context.owner(),
            &lifecycle,
            candidate_tx.clone(),
        );
        let mut election: Option<ElectionSession> = None;
        let mut campaign = self.new_campaign(context.owner().clone(), Duration::ZERO);
        let mut bootstrap_missing: Option<(bool, bool)> = None;
        let mut bootstrap_seen = false;

        loop {
            tokio::select! {
                changed = lifecycle.changed() => {
                    if changed.is_err() || shutdown_started(lifecycle.borrow().phase) {
                        break;
                    }
                }
                _ = file_tick.tick() => {
                    let source_changed = self.reload_file() | self.reload_external_material();
                    if source_changed {
                        self.reconfigure_persistence(
                            &mut reader,
                            &mut election,
                            &mut campaign,
                            context.owner(),
                            &lifecycle,
                            candidate_tx.clone(),
                        ).await?;
                    }
                }
                candidate = candidates.recv(), if reader.is_some() => {
                    let Some(candidate) = candidate else {
                        if shutdown_started(lifecycle.borrow().phase) {
                            break;
                        }
                        return Err(module_error("etcd_reader_stopped"));
                    };
                    let bootstrap = !bootstrap_seen;
                    if bootstrap {
                        bootstrap_seen = true;
                        bootstrap_missing = Some(missing_persistent_defaults(&candidate));
                    }
                    if let Err(error) = self.apply_raw_candidate(candidate) {
                        if bootstrap {
                            return Err(error);
                        }
                    } else if bootstrap {
                        self.ready.send_replace(true);
                    }
                    if let (Some(session), Some(missing)) =
                        (election.as_mut(), bootstrap_missing.take())
                    {
                        initialize_missing(session, &self.source, missing).await?;
                    }
                }
                joined = join_reader(&mut reader), if reader.is_some() => {
                    match joined {
                        Some(Ok(Ok(()))) if shutdown_started(lifecycle.borrow().phase) => break,
                        Some(Ok(Ok(()))) => return Err(module_error("etcd_reader_stopped")),
                        Some(Ok(Err(class))) => return Err(module_error(class)),
                        Some(Err(_)) => return Err(module_error("etcd_reader_panicked")),
                        None => return Err(module_error("etcd_reader_missing")),
                    }
                }
                result = poll_campaign(&mut campaign), if campaign.is_some() => {
                    campaign = None;
                    match result {
                        Some(Ok(session)) => {
                            election = Some(session);
                            if let (Some(session), Some(missing)) =
                                (election.as_mut(), bootstrap_missing.take())
                            {
                                initialize_missing(session, &self.source, missing).await?;
                            }
                        }
                        Some(Err(ElectionError::StaleOwner)) => {
                            return Err(module_error("stale_owner"));
                        }
                        Some(Err(_)) => {
                            campaign = self.new_campaign(
                                context.owner().clone(),
                                ETCD_RETRY_INTERVAL,
                            );
                        }
                        None => {}
                    }
                }
                _ = election_tick.tick(), if election.is_some() => {
                    let retained = maintain_election(election.as_mut()).await?;
                    if !retained {
                        election = None;
                        campaign = self.new_campaign(context.owner().clone(), Duration::ZERO);
                    }
                }
                request = self.mutations.recv() => {
                    let Some(request) = request else {
                        continue;
                    };
                    if let Mutation::LocalToml(data) = &request.mutation {
                        let result = self.apply_local_toml(data);
                        let _ = request.result.send(result);
                        continue;
                    }
                    let result = match election.as_mut() {
                        Some(session) => persist_mutation(
                            session,
                            &self.source,
                            &self.options.current_dir,
                            request.mutation,
                        ).await,
                        None => Err(ConfigMutationError::NotLeader),
                    };
                    if matches!(
                        election.as_ref().map(ElectionSession::snapshot),
                        Some(snapshot) if matches!(snapshot.state, ElectionState::Retired)
                    ) {
                        election = None;
                        campaign = self.new_campaign(context.owner().clone(), Duration::ZERO);
                    }
                    let _ = request.result.send(result);
                }
            }
        }

        Box::pin(self.finish(reader.take(), election)).await
    }

    fn new_campaign(&self, owner: OwnerToken, delay: Duration) -> Option<CampaignFuture> {
        let client_config = self.options.etcd.clone()?;
        let election_config = self.options.election.clone()?;
        Some(Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            ElectionSession::campaign(owner, client_config, election_config).await
        }))
    }

    fn reload_file(&mut self) -> bool {
        let Some(path) = self.options.config_file.as_ref() else {
            return false;
        };
        let Ok(content) = std::fs::read(path) else {
            return false;
        };
        if content == self.file_content {
            return false;
        }
        let file_revision = self.file_revision.saturating_add(1);
        let Ok(published) = self.source.apply_toml(
            &content,
            self.options.advertise_addr.as_deref(),
            file_revision,
            &self.options.current_dir,
        ) else {
            return false;
        };
        self.file_content = content;
        self.file_revision = file_revision;
        self.external_material = external_material_fingerprint(&self.source);
        published.is_some()
    }

    /// `SetTOMLConfig` through the owner: the same merge, validation and
    /// fail-closed restart boundary as a local file reload, with a new file
    /// revision so a later file change re-applies on top of the accepted
    /// document.
    fn apply_local_toml(&mut self, data: &[u8]) -> Result<(), ConfigMutationError> {
        let file_revision = self.file_revision.saturating_add(1);
        match self.source.apply_toml(
            data,
            self.options.advertise_addr.as_deref(),
            file_revision,
            &self.options.current_dir,
        ) {
            Ok(_) => {
                self.file_revision = file_revision;
                self.external_material = external_material_fingerprint(&self.source);
                Ok(())
            }
            Err(StoreError::Config(ConfigError::RestartRequired { .. })) => {
                Err(ConfigMutationError::RestartRequired)
            }
            Err(_) => Err(ConfigMutationError::Invalid),
        }
    }

    fn reload_external_material(&mut self) -> bool {
        let candidate = external_material_fingerprint(&self.source);
        if candidate == self.external_material {
            return false;
        }
        if self.source.refresh_external_material().is_ok() {
            self.external_material = candidate;
            return true;
        }
        false
    }

    async fn reconfigure_persistence(
        &mut self,
        reader: &mut Option<JoinHandle<Result<(), &'static str>>>,
        election: &mut Option<ElectionSession>,
        campaign: &mut Option<CampaignFuture>,
        owner: &OwnerToken,
        lifecycle: &tokio::sync::watch::Receiver<control_plane::LifecycleSnapshot>,
        candidates: mpsc::Sender<RawCandidate>,
    ) -> Result<(), ModuleError> {
        let Some(factory) = &self.options.persistence_factory else {
            return Ok(());
        };
        let rebuilt = factory
            .build(self.source.current().effective())
            .map_err(|_| module_error("persistence_transport_rejected"))?;
        if rebuilt == self.options.etcd {
            return Ok(());
        }

        // Stop both old users before installing the new transport. A failed
        // lease revoke is safe: the next campaign cannot become leader until
        // the old lease expires, and owner-fenced writes remain impossible in
        // the gap.
        *campaign = None;
        if let Some(session) = election.take() {
            let _ = session.shutdown().await;
        }
        if let Some(task) = reader.take() {
            task.abort();
            let _ = task.await;
        }

        self.options.etcd = rebuilt;
        *reader = spawn_etcd_reader(self.options.etcd.as_ref(), owner, lifecycle, candidates);
        *campaign = self.new_campaign(owner.clone(), Duration::ZERO);
        Ok(())
    }

    fn apply_raw_candidate(&mut self, candidate: RawCandidate) -> Result<(), ModuleError> {
        let RawCandidate {
            revision,
            mut entries,
            namespace_update,
        } = candidate;
        self.source.observe_etcd_revision(revision);
        if namespace_update == NamespaceUpdate::Preserve {
            // A watch batch carries the reader's complete accumulated prefix,
            // including a namespace value that may have been rejected by an
            // earlier namespace event.  A later proxy/log-only event must not
            // retry, decode or accidentally adopt that unrelated material.
            entries.retain(|key, _| !is_namespace_key(key));
        }
        let decoded = decode_persistent_entries(entries).map_err(|error| {
            if let Some(log) = persistent_candidate_rejection_log(revision, &error) {
                control_plane::logging::emit(control_plane::logging::Level::Warn, &log);
            }
            module_error("persistent_candidate_decode_rejected")
        })?;
        let applied = match namespace_update {
            NamespaceUpdate::Preserve => self.source.apply_persistent_preserving_namespaces(
                decoded,
                revision,
                &self.options.current_dir,
            ),
            NamespaceUpdate::Replace => {
                self.source
                    .apply_persistent(decoded, revision, &self.options.current_dir)
            }
        };
        applied.map_err(|_| module_error("persistent_candidate_apply_rejected"))?;
        self.external_material = external_material_fingerprint(&self.source);
        Ok(())
    }

    async fn finish(
        &mut self,
        reader: Option<JoinHandle<Result<(), &'static str>>>,
        election: Option<ElectionSession>,
    ) -> Result<(), ModuleError> {
        if let Some(reader) = reader {
            let _ = reader.await;
        }
        if let Some(session) = election {
            let _ = session.shutdown().await;
        }
        while let Ok(request) = self.mutations.try_recv() {
            let _ = request.result.send(Err(ConfigMutationError::Stopped));
        }
        Ok(())
    }
}

fn persistent_candidate_rejection_log(revision: i64, error: &StoreError) -> Option<String> {
    let StoreError::PersistentNamespace { key, class } = error else {
        return None;
    };
    Some(
        json!({
            "component": "control-config",
            "event": "persistent_candidate_rejected",
            "revision": revision,
            "key": key,
            "error_class": class,
        })
        .to_string(),
    )
}

fn external_material_fingerprint(source: &ConfigNamespaceStore) -> ExternalMaterialFingerprint {
    let snapshot = source.current();
    let mut paths = snapshot
        .effective()
        .tls_material_paths()
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    for namespace in snapshot.namespaces() {
        let mut namespace_paths = Vec::new();
        namespace.append_tls_material_paths(&mut namespace_paths);
        paths.extend(namespace_paths.into_iter().map(PathBuf::from));
    }
    paths.sort();
    paths.dedup();
    ExternalMaterialFingerprint(
        paths
            .into_iter()
            .map(|path| {
                let state = std::fs::read(&path)
                    .map(ExternalMaterialState::Readable)
                    .unwrap_or(ExternalMaterialState::Unavailable);
                (path, state)
            })
            .collect(),
    )
}

impl ControlModule for ConfigModule {
    fn name(&self) -> &'static str {
        "control_config"
    }

    fn run(self: Box<Self>, context: ModuleContext) -> ModuleFuture {
        Box::pin(self.run_inner(context))
    }
}

type CampaignFuture =
    Pin<Box<dyn Future<Output = Result<ElectionSession, ElectionError>> + Send + 'static>>;

async fn poll_campaign(
    campaign: &mut Option<CampaignFuture>,
) -> Option<Result<ElectionSession, ElectionError>> {
    match campaign {
        Some(campaign) => Some(campaign.await),
        None => pending().await,
    }
}

async fn join_reader(
    reader: &mut Option<JoinHandle<Result<(), &'static str>>>,
) -> Option<Result<Result<(), &'static str>, tokio::task::JoinError>> {
    let result = match reader {
        Some(reader) => reader.await,
        None => pending().await,
    };
    // A cancelled pending join must leave the reader owned by the module, but
    // finish must not poll a handle whose output was already consumed here.
    *reader = None;
    Some(result)
}

#[derive(Clone, Debug)]
struct RawCandidate {
    revision: i64,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
    namespace_update: NamespaceUpdate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamespaceUpdate {
    Preserve,
    Replace,
}

async fn run_etcd_reader(
    owner: OwnerToken,
    client_config: EtcdClientConfig,
    mut lifecycle: tokio::sync::watch::Receiver<control_plane::LifecycleSnapshot>,
    candidates: mpsc::Sender<RawCandidate>,
) -> Result<(), &'static str> {
    let connector = EtcdConnector::new(owner, client_config);
    loop {
        if shutdown_started(lifecycle.borrow().phase) {
            return Ok(());
        }
        let result = read_until_disconnect(&connector, &mut lifecycle, &candidates).await;
        match result {
            Ok(()) => return Ok(()),
            Err(ReaderError::StaleOwner) => return Err("stale_owner"),
            Err(ReaderError::Dependency) => {
                tokio::select! {
                    changed = lifecycle.changed() => {
                        if changed.is_err() || shutdown_started(lifecycle.borrow().phase) {
                            return Ok(());
                        }
                    }
                    () = tokio::time::sleep(ETCD_RETRY_INTERVAL) => {}
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ReaderError {
    StaleOwner,
    Dependency,
}

async fn read_until_disconnect(
    connector: &EtcdConnector,
    lifecycle: &mut tokio::sync::watch::Receiver<control_plane::LifecycleSnapshot>,
    candidates: &mpsc::Sender<RawCandidate>,
) -> Result<(), ReaderError> {
    let mut connection = connector.connect().await.map_err(|error| match error {
        control_external::EtcdConnectError::StaleOwner => ReaderError::StaleOwner,
        control_external::EtcdConnectError::Dependency(_) => {
            eprintln!("tiproxy-rs etcd connect failed: {error:?}");
            ReaderError::Dependency
        }
    })?;
    let response = connection
        .execute(|client| {
            Box::pin(client.get(CONFIG_PREFIX, Some(GetOptions::new().with_prefix())))
        })
        .await
        .map_err(|error| {
            eprintln!("tiproxy-rs etcd get /config failed: {error:?}");
            map_reader_operation(&error)
        })?;
    let revision = response
        .header()
        .map_or(0, etcd_client::ResponseHeader::revision);
    let mut entries = response
        .kvs()
        .iter()
        .map(|value| (value.key().to_vec(), value.value().to_vec()))
        .collect::<BTreeMap<_, _>>();
    let namespace_update = if entries.keys().any(|key| is_namespace_key(key)) {
        NamespaceUpdate::Replace
    } else {
        NamespaceUpdate::Preserve
    };
    send_candidate(lifecycle, candidates, revision, &entries, namespace_update).await?;

    let start_revision = revision.saturating_add(1).max(1);
    let mut stream = connection
        .execute(move |client| {
            Box::pin(
                client.watch(
                    CONFIG_PREFIX,
                    Some(
                        WatchOptions::new()
                            .with_prefix()
                            .with_start_revision(start_revision)
                            .with_progress_notify(),
                    ),
                ),
            )
        })
        .await
        .map_err(|error| map_reader_operation(&error))?;
    loop {
        let response = tokio::select! {
            changed = lifecycle.changed() => {
                if changed.is_err() || shutdown_started(lifecycle.borrow().phase) {
                    return Ok(());
                }
                continue;
            }
            response = stream.message() => response,
        }
        .map_err(|_| ReaderError::Dependency)?
        .ok_or(ReaderError::Dependency)?;
        if response.canceled() {
            return Err(ReaderError::Dependency);
        }
        let revision = response
            .header()
            .map_or(0, etcd_client::ResponseHeader::revision);
        let mut changed = false;
        let mut namespace_update = NamespaceUpdate::Preserve;
        for event in response.events() {
            let Some(value) = event.kv() else {
                continue;
            };
            if is_namespace_key(value.key()) {
                namespace_update = NamespaceUpdate::Replace;
            }
            match event.event_type() {
                EventType::Put => {
                    entries.insert(value.key().to_vec(), value.value().to_vec());
                }
                EventType::Delete => {
                    entries.remove(value.key());
                }
            }
            changed = true;
        }
        if changed {
            send_candidate(lifecycle, candidates, revision, &entries, namespace_update).await?;
        }
    }
}

fn is_namespace_key(key: &[u8]) -> bool {
    key.starts_with(NAMESPACE_CONFIG_PREFIX.as_bytes())
}

async fn send_candidate(
    lifecycle: &mut tokio::sync::watch::Receiver<control_plane::LifecycleSnapshot>,
    candidates: &mpsc::Sender<RawCandidate>,
    revision: i64,
    entries: &BTreeMap<Vec<u8>, Vec<u8>>,
    namespace_update: NamespaceUpdate,
) -> Result<(), ReaderError> {
    tokio::select! {
        changed = lifecycle.changed() => {
            if changed.is_err() || shutdown_started(lifecycle.borrow().phase) {
                Ok(())
            } else {
                Err(ReaderError::Dependency)
            }
        }
        result = candidates.send(RawCandidate {
            revision,
            entries: entries.clone(),
            namespace_update,
        }) => result.map_err(|_| ReaderError::Dependency),
    }
}

const fn map_reader_operation(error: &EtcdOperationError) -> ReaderError {
    match error {
        EtcdOperationError::StaleOwner => ReaderError::StaleOwner,
        EtcdOperationError::Dependency(_) => ReaderError::Dependency,
    }
}

async fn maintain_election(session: Option<&mut ElectionSession>) -> Result<bool, ModuleError> {
    let Some(session) = session else {
        return Ok(false);
    };
    let result = if matches!(session.snapshot().state, ElectionState::Uncertain) {
        session.recover().await
    } else {
        session.keep_alive().await
    };
    match result {
        Ok(RecoveryOutcome::Retired(_)) => Ok(false),
        Err(ElectionError::StaleOwner) => Err(module_error("stale_owner")),
        Ok(RecoveryOutcome::Restored) | Err(_) => Ok(true),
    }
}

async fn initialize_missing(
    session: &mut ElectionSession,
    source: &ConfigNamespaceStore,
    (proxy_missing, log_missing): (bool, bool),
) -> Result<(), ModuleError> {
    let (proxy, log) = source.persistent_defaults();
    if proxy_missing {
        let value = serde_json::to_vec(&proxy).map_err(|_| module_error("json_encode_failed"))?;
        match session
            .fenced_persistent_put(PROXY_CONFIG_KEY, value, PersistentPutMode::Create)
            .await
        {
            Ok(_) => {}
            Err(ElectionError::StaleOwner) => return Err(module_error("stale_owner")),
            Err(ElectionError::NotLeader) => return Ok(()),
            Err(_) => return Err(module_error("persistent_init_failed")),
        }
    }
    if log_missing {
        let value = serde_json::to_vec(&log).map_err(|_| module_error("json_encode_failed"))?;
        match session
            .fenced_persistent_put(LOG_CONFIG_KEY, value, PersistentPutMode::Create)
            .await
        {
            Ok(_) => {}
            Err(ElectionError::StaleOwner) => return Err(module_error("stale_owner")),
            Err(ElectionError::NotLeader) => return Ok(()),
            Err(_) => return Err(module_error("persistent_init_failed")),
        }
    }
    Ok(())
}

async fn persist_mutation(
    session: &mut ElectionSession,
    source: &ConfigNamespaceStore,
    current_dir: &std::path::Path,
    mutation: Mutation,
) -> Result<(), ConfigMutationError> {
    match mutation {
        Mutation::Proxy(value) => {
            source
                .validate_proxy_mutation(value.clone(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)?;
            let encoded = serde_json::to_vec(&value).map_err(|_| ConfigMutationError::Invalid)?;
            let result = persist_put(session, PROXY_CONFIG_KEY.to_owned(), encoded).await?;
            source
                .apply_committed_proxy_mutation(value, result.revision(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)
        }
        Mutation::Log(value) => {
            source
                .validate_log_mutation(value.clone(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)?;
            let encoded = serde_json::to_vec(&value).map_err(|_| ConfigMutationError::Invalid)?;
            let result = persist_put(session, LOG_CONFIG_KEY.to_owned(), encoded).await?;
            source
                .apply_committed_log_mutation(value, result.revision(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)
        }
        Mutation::Namespace(value) => {
            if value.namespace.is_empty() || value.namespace.as_bytes().contains(&0) {
                return Err(ConfigMutationError::Invalid);
            }
            source
                .validate_namespace_mutation(value.clone(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)?;
            let key = format!("{NAMESPACE_CONFIG_PREFIX}{}", value.namespace);
            let encoded = serde_json::to_vec(&value).map_err(|_| ConfigMutationError::Invalid)?;
            let result = persist_put(session, key, encoded).await?;
            source
                .apply_committed_namespace_mutation(value, result.revision(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)
        }
        // Handled by the owner loop before any persistence; never reaches here.
        Mutation::LocalToml(_) => Err(ConfigMutationError::Invalid),
        Mutation::DeleteNamespace(name) => {
            if name.is_empty() || name.as_bytes().contains(&0) {
                return Err(ConfigMutationError::Invalid);
            }
            source
                .validate_namespace_deletion(&name, current_dir)
                .map_err(|_| ConfigMutationError::Invalid)?;
            let result = session
                .fenced_persistent_delete(format!("{NAMESPACE_CONFIG_PREFIX}{name}"))
                .await
                .map_err(|error| map_mutation_error(&error))?;
            source
                .apply_committed_namespace_deletion(&name, result.revision(), current_dir)
                .map_err(|_| ConfigMutationError::Invalid)
        }
    }
}

async fn persist_put(
    session: &mut ElectionSession,
    key: String,
    value: Vec<u8>,
) -> Result<control_etcd::PersistentPutResult, ConfigMutationError> {
    session
        .fenced_persistent_put(key, value, PersistentPutMode::Upsert)
        .await
        .map_err(|error| map_mutation_error(&error))
}

const fn map_mutation_error(error: &ElectionError) -> ConfigMutationError {
    match error {
        ElectionError::StaleOwner | ElectionError::StaleScope | ElectionError::NotLeader => {
            ConfigMutationError::NotLeader
        }
        ElectionError::InvalidTransactionInput | ElectionError::InvalidResponse { .. } => {
            ConfigMutationError::Invalid
        }
        ElectionError::Connect(_)
        | ElectionError::Operation { .. }
        | ElectionError::Stream { .. }
        | ElectionError::Timeout { .. }
        | ElectionError::WatchCanceled => ConfigMutationError::Unavailable,
    }
}

fn election_maintenance_interval(config: Option<&ElectionConfig>) -> Duration {
    config.map_or(Duration::from_secs(10), |config| {
        let ttl_millis = u64::try_from(config.session_ttl_seconds())
            .unwrap_or(1)
            .saturating_mul(1_000);
        Duration::from_millis((ttl_millis / 3).max(100))
    })
}

fn module_ticks(options: &ConfigModuleOptions) -> (tokio::time::Interval, tokio::time::Interval) {
    let mut file = tokio::time::interval(FILE_POLL_INTERVAL);
    file.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut election =
        tokio::time::interval(election_maintenance_interval(options.election.as_ref()));
    election.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    (file, election)
}

fn spawn_etcd_reader(
    client_config: Option<&EtcdClientConfig>,
    owner: &OwnerToken,
    lifecycle: &tokio::sync::watch::Receiver<control_plane::LifecycleSnapshot>,
    candidates: mpsc::Sender<RawCandidate>,
) -> Option<JoinHandle<Result<(), &'static str>>> {
    client_config.cloned().map(|client_config| {
        tokio::spawn(run_etcd_reader(
            owner.clone(),
            client_config,
            lifecycle.clone(),
            candidates,
        ))
    })
}

fn missing_persistent_defaults(candidate: &RawCandidate) -> (bool, bool) {
    (
        !candidate.entries.contains_key(PROXY_CONFIG_KEY.as_bytes()),
        !candidate.entries.contains_key(LOG_CONFIG_KEY.as_bytes()),
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
        module: "control_config",
        error_class,
    }
}

#[cfg(test)]
#[path = "module_tests.rs"]
mod tests;
