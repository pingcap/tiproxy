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

//! Atomic validation and last-good storage for complete dataplane snapshots.

use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::net::IpAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex as StdMutex, RwLock};

use rustls::client::danger::HandshakeSignatureValid;
pub use rustls::pki_types::UnixTime;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    DigitallySignedStruct, DistinguishedName, Error as TlsError, RootCertStore, ServerConfig,
    SignatureScheme,
};
use x509_parser::prelude::{ASN1Time, parse_x509_certificate};

use crate::v1::{
    BackendSnapshot, ConfigSnapshot, ErrorCode, KeepalivePolicy, NamespaceSnapshot,
    ProxyProtocolMode, SnapshotResult, StateSnapshot, TlsPolicy,
};

const MIN_CONNECTION_BUFFER_BYTES: u32 = 1024;
const MAX_CONNECTION_BUFFER_BYTES: u32 = 16 * 1024 * 1024;
const MAX_TLS_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LISTENERS: usize = 4096;
const MAX_BACKENDS: usize = 65_536;
const MAX_NAMESPACES: usize = 4096;
const MAX_CERTIFICATE_CHAIN_LENGTH: usize = 64;
const MAX_CA_CERTIFICATES: usize = 256;

/// Stable category for a rejected snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotErrorKind {
    /// A field or referenced file is malformed.
    Invalid,
    /// A valid setting cannot run in Rust dataplane mode.
    Unsupported,
    /// The generation is older than the committed state.
    Stale,
    /// Snapshot store synchronization failed.
    Internal,
}

/// A redacted snapshot rejection suitable for `SnapshotResult.detail`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotError {
    kind: SnapshotErrorKind,
    detail: String,
}

impl SnapshotError {
    /// An invalid-input rejection (public for composition owners that
    /// must answer malformed snapshot traffic).
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self {
            kind: SnapshotErrorKind::Invalid,
            detail: detail.into(),
        }
    }

    /// A valid setting that the Rust dataplane cannot apply.
    pub fn unsupported(detail: impl Into<String>) -> Self {
        Self {
            kind: SnapshotErrorKind::Unsupported,
            detail: detail.into(),
        }
    }

    fn stale(detail: impl Into<String>) -> Self {
        Self {
            kind: SnapshotErrorKind::Stale,
            detail: detail.into(),
        }
    }

    fn internal(detail: impl Into<String>) -> Self {
        Self {
            kind: SnapshotErrorKind::Internal,
            detail: detail.into(),
        }
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn kind(&self) -> SnapshotErrorKind {
        self.kind
    }

    /// Returns a redacted diagnostic that names fields, never secret contents.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Converts this rejection into the v1 response body.
    #[must_use]
    pub fn to_result(&self, applied_generation: u64) -> SnapshotResult {
        let code = match self.kind {
            SnapshotErrorKind::Invalid => ErrorCode::InvalidSnapshot,
            SnapshotErrorKind::Unsupported => ErrorCode::UnsupportedConfiguration,
            SnapshotErrorKind::Stale => ErrorCode::StaleGeneration,
            SnapshotErrorKind::Internal => ErrorCode::Internal,
        };
        SnapshotResult {
            applied_generation,
            code: code.into(),
            detail: self.detail.clone(),
        }
    }
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for SnapshotError {}

/// Parsed TLS material owned by one immutable snapshot generation.
#[derive(Debug)]
pub struct ValidatedTlsPolicy {
    /// Canonical certificate chain loaded for this generation.
    pub certificate_chain: Vec<CertificateDer<'static>>,
    private_key: Option<Arc<PrivateKeyDer<'static>>>,
    /// Canonical CA roots loaded for this generation.
    pub roots: RootCertStore,
    /// Minimum TLS version from the snapshot.
    pub minimum_version: String,
    /// Common-name allowlist from the snapshot.
    pub allowed_common_names: Vec<String>,
    /// Whether backend peer CA verification may be skipped.
    pub skip_ca_verification: bool,
}

impl ValidatedTlsPolicy {
    /// Returns an owned copy of the private key for a new TLS connection config.
    #[must_use]
    pub fn private_key(&self) -> Option<PrivateKeyDer<'static>> {
        self.private_key.as_deref().map(PrivateKeyDer::clone_key)
    }
}

#[derive(Debug)]
struct CommonNameClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    allowed_common_names: BTreeSet<String>,
}

impl ClientCertVerifier for CommonNameClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let (_, certificate) = parse_x509_certificate(end_entity.as_ref())
            .map_err(|_| TlsError::General("client certificate DER is invalid".to_owned()))?;
        let common_name_allowed = certificate.subject().iter_common_name().any(|name| {
            name.as_str()
                .ok()
                .is_some_and(|value| self.allowed_common_names.contains(value))
        });
        if !common_name_allowed {
            return Err(TlsError::General(
                "client certificate common name is not allowed".to_owned(),
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Independent process-local generations attached to a composed serving
/// view. Both are zero on the legacy one-source path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompositionGenerations {
    /// Composer counter that produced the view.
    pub composition: u64,
    /// Rust-owned config source generation the composition was composed
    /// from. It travels with the view so only the serving apply that
    /// installs the view can publish it.
    pub config: u64,
}

/// One complete, immutable generation ready for new sessions.
#[derive(Debug)]
pub struct ValidatedSnapshot {
    generation: u64,
    generations: CompositionGenerations,
    source_raw: StateSnapshot,
    raw: StateSnapshot,
    /// Server TLS config captured by new frontend sessions.
    pub frontend_server_config: Option<Arc<ServerConfig>>,
    /// Parsed frontend TLS policy and material.
    pub frontend_tls: ValidatedTlsPolicy,
    /// Parsed backend TLS policy and material.
    pub backend_tls: ValidatedTlsPolicy,
}

impl ValidatedSnapshot {
    /// Returns the committed nonzero generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the independent process-local generation used to compose this
    /// serving view. Zero identifies the legacy one-source path.
    #[must_use]
    pub const fn composition_generation(&self) -> u64 {
        self.generations.composition
    }

    /// Returns the Rust-owned config source generation this serving view was
    /// composed from. Zero identifies the legacy one-source path. Carried
    /// with the validated view so a successful serving apply can publish the
    /// config lineage it actually installed.
    #[must_use]
    pub const fn config_generation(&self) -> u64 {
        self.generations.config
    }

    /// Returns the complete validated protobuf snapshot.
    #[must_use]
    pub const fn raw(&self) -> &StateSnapshot {
        &self.raw
    }

    /// Returns the bridge payload used for idempotence and generation
    /// lineage. During field-level control-plane handoff this may differ from
    /// [`Self::raw`], which is the composed Rust-owned serving view.
    #[must_use]
    pub const fn source_raw(&self) -> &StateSnapshot {
        &self.source_raw
    }
}

/// The Go process lineage a snapshot belongs to. Generation
/// monotonicity is only defined WITHIN one lineage (the control
/// protocol requires monotonic generations per Go process, not
/// globally): a restarted Go starts a fresh sequence, and the store
/// must recognize the rollover instead of rejecting the new lineage's
/// generations as stale — while an INVALID candidate from a new
/// lineage must never switch the store's lineage early, or a same-Go
/// reconnect would afterwards be misjudged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotLineage {
    /// The Go peer's process id from its Hello.
    pub peer_process_id: Arc<str>,
    /// The Go peer's start time from its Hello.
    pub peer_started_unix_millis: u64,
}

impl SnapshotLineage {
    /// A fixture lineage for tests and single-lineage compositions.
    #[must_use]
    pub fn for_tests(process_id: &str) -> Self {
        Self {
            peer_process_id: Arc::from(process_id),
            peer_started_unix_millis: 1_700_000_000_000,
        }
    }
}

/// A validated-but-uncommitted snapshot between the two phases of
/// [`SnapshotStore::stage`] / [`SnapshotStore::commit`]. The token
/// holds the store's writer reservation for its whole lifetime:
/// dropping it without committing releases the reservation with the
/// committed state untouched, and no concurrent writer can advance the
/// store while it is held — so a downstream success between the phases
/// can never be invalidated by a racing commit.
#[derive(Debug)]
pub struct Staged {
    writer: WriterGuard,
    state: StagedState,
    /// The lineage this snapshot was produced by — committed
    /// ATOMICALLY with the snapshot, never before validation and the
    /// downstream apply succeed.
    lineage: SnapshotLineage,
}

#[derive(Debug)]
enum StagedState {
    /// The exact generation+content is already committed: the earlier
    /// two-phase apply (downstream included) succeeded — answer
    /// success without re-running anything.
    Unchanged(Arc<ValidatedSnapshot>),
    /// Validated against the committed state; not yet committed.
    Validated(Arc<ValidatedSnapshot>),
}

impl Staged {
    /// The staged snapshot's validated view.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<ValidatedSnapshot> {
        match &self.state {
            StagedState::Unchanged(snapshot) | StagedState::Validated(snapshot) => snapshot,
        }
    }

    /// Whether commit would change the committed state.
    #[must_use]
    pub const fn is_changed(&self) -> bool {
        matches!(self.state, StagedState::Validated(_))
    }
}

/// An owned reservation used instead of `std::sync::MutexGuard`: the
/// staged token crosses the serving consumer's async boundary, while a
/// standard mutex guard is not `Send`.
#[derive(Debug, Default)]
struct WriterReservation {
    held: StdMutex<bool>,
    available: Condvar,
}

impl WriterReservation {
    fn acquire(self: &Arc<Self>) -> Result<WriterGuard, SnapshotError> {
        let mut held = self
            .held
            .lock()
            .map_err(|_| SnapshotError::internal("snapshot writer reservation poisoned"))?;
        while *held {
            held = self
                .available
                .wait(held)
                .map_err(|_| SnapshotError::internal("snapshot writer reservation poisoned"))?;
        }
        *held = true;
        Ok(WriterGuard {
            reservation: Arc::clone(self),
        })
    }
}

#[derive(Debug)]
struct WriterGuard {
    reservation: Arc<WriterReservation>,
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        let mut held = self
            .reservation
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *held = false;
        self.reservation.available.notify_one();
    }
}

/// Result of an atomic apply attempt.
#[derive(Debug)]
pub struct ApplyOutcome {
    /// The current generation, retained by `Arc` for session capture.
    pub snapshot: Arc<ValidatedSnapshot>,
    /// False for an identical same-generation replay.
    pub changed: bool,
}

impl ApplyOutcome {
    /// Converts this successful apply or idempotent replay into the v1 response body.
    #[must_use]
    pub fn to_result(&self) -> SnapshotResult {
        SnapshotResult {
            applied_generation: self.snapshot.generation,
            code: ErrorCode::Ok.into(),
            detail: if self.changed {
                "snapshot applied".to_owned()
            } else {
                "snapshot already applied".to_owned()
            },
        }
    }
}

/// Validates candidates in isolation and atomically swaps only complete state.
#[derive(Clone)]
pub struct SnapshotStore {
    allowed_tls_roots: Vec<PathBuf>,
    current: Arc<RwLock<Option<Arc<ValidatedSnapshot>>>>,
    /// The committed snapshot's lineage. Written only under the writer
    /// reservation (commit), read only under it (stage), so the pair
    /// (current, lineage) is writer-consistent.
    lineage: Arc<RwLock<Option<SnapshotLineage>>>,
    /// Serializes the whole two-phase apply: a [`Staged`] token holds
    /// this guard, so no other writer — `apply` included — can advance
    /// the committed state between `stage` and `commit`. A downstream
    /// consumer success can therefore never be followed by a
    /// stale/conflict commit failure.
    writer: Arc<WriterReservation>,
}

impl SnapshotStore {
    /// Creates a store with canonical directories that TLS files must remain beneath.
    ///
    /// # Errors
    ///
    /// Returns an error when a root is relative, absent, or not a directory.
    pub fn new(
        allowed_tls_roots: impl IntoIterator<Item = PathBuf>,
    ) -> Result<Self, SnapshotError> {
        let mut roots = Vec::new();
        for root in allowed_tls_roots {
            if !root.is_absolute() {
                return Err(SnapshotError::invalid(
                    "TLS allowlist root must be absolute",
                ));
            }
            let canonical = fs::canonicalize(&root)
                .map_err(|_| SnapshotError::invalid("TLS allowlist root is unavailable"))?;
            let metadata = fs::metadata(&canonical)
                .map_err(|_| SnapshotError::invalid("TLS allowlist root is unavailable"))?;
            if !metadata.is_dir() {
                return Err(SnapshotError::invalid(
                    "TLS allowlist root is not a directory",
                ));
            }
            roots.push(canonical);
        }
        roots.sort();
        roots.dedup();
        Ok(Self {
            allowed_tls_roots: roots,
            current: Arc::new(RwLock::new(None)),
            lineage: Arc::new(RwLock::new(None)),
            writer: Arc::new(WriterReservation::default()),
        })
    }

    /// Returns the last-good immutable state for capture by a new session.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the store lock is poisoned.
    pub fn current(&self) -> Result<Option<Arc<ValidatedSnapshot>>, SnapshotError> {
        self.current
            .read()
            .map(|guard| guard.clone())
            .map_err(|_| SnapshotError::internal("snapshot store lock poisoned"))
    }

    /// Validates a complete candidate and atomically commits a newer generation.
    ///
    /// The supplied time makes certificate validity checks deterministic in tests.
    /// Equal, byte-equivalent generations are idempotent. Every rejection retains
    /// the last-good snapshot.
    ///
    /// # Errors
    ///
    /// Returns a categorized, redacted reason for invalid, unsupported, stale,
    /// conflicting, or internally unavailable state.
    pub fn apply(
        &self,
        generation: u64,
        snapshot: StateSnapshot,
        now: UnixTime,
        lineage: SnapshotLineage,
    ) -> Result<ApplyOutcome, SnapshotError> {
        let staged = self.stage(generation, snapshot, now, lineage)?;
        self.commit(staged)
    }

    /// Phase one of a two-phase apply: validates the snapshot against
    /// the committed state **without advancing it**. The caller runs
    /// its downstream application (serving-side consumer) between
    /// `stage` and [`SnapshotStore::commit`]; a downstream rejection
    /// leaves the store untouched, so a replay of the same generation
    /// re-validates and re-runs the downstream instead of answering a
    /// false success off an already-advanced store.
    ///
    /// # Errors
    ///
    /// Returns stale/invalid/internal errors exactly like
    /// [`SnapshotStore::apply`].
    pub fn stage(
        &self,
        generation: u64,
        snapshot: StateSnapshot,
        now: UnixTime,
        lineage: SnapshotLineage,
    ) -> Result<Staged, SnapshotError> {
        self.stage_with_source(generation, snapshot.clone(), snapshot, now, lineage)
    }

    /// Stages a bridge source payload and a separately composed effective
    /// serving payload under one protocol generation. Equal-generation
    /// idempotence compares the source payload, while validation and consumers
    /// observe only the effective payload. This is the field-level migration
    /// seam used while topology remains on the legacy bridge.
    ///
    /// # Errors
    ///
    /// Returns the same stale, conflicting, validation, or internal errors as
    /// [`Self::stage`].
    pub fn stage_with_source(
        &self,
        generation: u64,
        source_snapshot: StateSnapshot,
        effective_snapshot: StateSnapshot,
        now: UnixTime,
        lineage: SnapshotLineage,
    ) -> Result<Staged, SnapshotError> {
        self.stage_composed(
            generation,
            source_snapshot,
            effective_snapshot,
            CompositionGenerations::default(),
            now,
            lineage,
        )
    }

    /// Stages one source/effective pair and records its independent
    /// process-local composition generation together with the Rust-owned
    /// config source generation the composition used.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::stage_with_source`].
    pub fn stage_composed(
        &self,
        generation: u64,
        source_snapshot: StateSnapshot,
        effective_snapshot: StateSnapshot,
        generations: CompositionGenerations,
        now: UnixTime,
        lineage: SnapshotLineage,
    ) -> Result<Staged, SnapshotError> {
        let writer = self.writer.acquire()?;
        if generation == 0 {
            return Err(SnapshotError::invalid(
                "snapshot generation must be nonzero",
            ));
        }
        // Generation monotonicity is a WITHIN-lineage rule: a snapshot
        // from a different Go lineage starts a fresh sequence and skips
        // the stale/duplicate comparisons entirely — its candidate must
        // still fully validate, and the store's lineage only advances
        // at COMMIT, so an invalid new-lineage candidate leaves the old
        // last-good state and lineage untouched.
        let same_lineage = {
            let committed = self
                .lineage
                .read()
                .map_err(|_| SnapshotError::internal("snapshot store lock poisoned"))?;
            committed.as_ref().is_none_or(|current| *current == lineage)
        };
        if same_lineage {
            let guard = self
                .current
                .read()
                .map_err(|_| SnapshotError::internal("snapshot store lock poisoned"))?;
            if let Some(current) = guard.as_ref() {
                if generation < current.generation {
                    return Err(SnapshotError::stale(format!(
                        "snapshot generation {generation} is older than {}",
                        current.generation
                    )));
                }
                if generation == current.generation {
                    if source_snapshot == current.source_raw {
                        // Committed already — which implies the whole
                        // two-phase apply (downstream included)
                        // succeeded when it was committed.
                        return Ok(Staged {
                            writer,
                            state: StagedState::Unchanged(Arc::clone(current)),
                            lineage,
                        });
                    }
                    return Err(SnapshotError::invalid(
                        "same snapshot generation has different contents",
                    ));
                }
            }
        }
        let candidate = Arc::new(self.validate(
            generation,
            generations,
            source_snapshot,
            effective_snapshot,
            now,
        )?);
        Ok(Staged {
            writer,
            state: StagedState::Validated(candidate),
            lineage,
        })
    }

    /// Validates a serving-only recomposition without changing bridge lineage
    /// or the committed protocol generation.
    ///
    /// # Errors
    ///
    /// Returns the same complete snapshot validation errors as [`Self::stage`].
    pub fn validate_detached(
        &self,
        generation: u64,
        snapshot: StateSnapshot,
        now: UnixTime,
    ) -> Result<Arc<ValidatedSnapshot>, SnapshotError> {
        self.validate_composed(generation, CompositionGenerations::default(), snapshot, now)
    }

    /// Validates a serving-only recomposition with its independent
    /// process-local generation and the Rust-owned config source generation
    /// it was composed from, without changing store state.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as [`Self::validate_detached`].
    pub fn validate_composed(
        &self,
        generation: u64,
        generations: CompositionGenerations,
        snapshot: StateSnapshot,
        now: UnixTime,
    ) -> Result<Arc<ValidatedSnapshot>, SnapshotError> {
        if generation == 0 {
            return Err(SnapshotError::invalid(
                "snapshot generation must be nonzero",
            ));
        }
        Ok(Arc::new(self.validate(
            generation,
            generations,
            snapshot.clone(),
            snapshot,
            now,
        )?))
    }

    /// Validates one detached TLS material set without publishing snapshot state.
    ///
    /// This is used by process-local control modules whose TLS policy is not
    /// represented in the temporary dataplane bridge snapshot (HTTP, topology,
    /// and namespace policies). The same allowlist, file bounds, PEM parsing,
    /// certificate-time checks, and client/server policy rules apply.
    ///
    /// # Errors
    ///
    /// Returns a bounded snapshot error when policy or material is invalid.
    pub fn validate_tls_material(
        &self,
        field: &str,
        policy: &TlsPolicy,
        now: UnixTime,
        server_side: bool,
    ) -> Result<(), SnapshotError> {
        self.validate_tls(field, Some(policy), now, server_side)
            .map(|_| ())
    }

    /// Validates and prepares one detached server TLS identity without
    /// publishing snapshot state.
    ///
    /// This is the retained counterpart of [`Self::validate_tls_material`] for
    /// process-local HTTP services. The returned `ServerConfig` owns the exact
    /// certificate, key, client-CA, common-name policy, and protocol-version
    /// material read during candidate validation, so the eventual listener
    /// never re-reads paths after the generation is published. `None` means the
    /// accepted policy is plaintext.
    ///
    /// # Errors
    ///
    /// Returns the same bounded validation errors as
    /// [`Self::validate_tls_material`].
    pub fn prepare_server_tls(
        &self,
        field: &str,
        policy: &TlsPolicy,
        now: UnixTime,
    ) -> Result<Option<Arc<ServerConfig>>, SnapshotError> {
        self.validate_tls(field, Some(policy), now, true)
            .map(|(_, server)| server)
    }

    /// Phase two: publishes a staged snapshot. The staged token still
    /// holds the store's writer reservation, so **no concurrent writer
    /// can have advanced the store between the phases** — the commit
    /// is a plain publication, not a re-negotiation.
    ///
    /// # Errors
    ///
    /// Returns an internal error when the store lock is poisoned.
    pub fn commit(&self, staged: Staged) -> Result<ApplyOutcome, SnapshotError> {
        if !Arc::ptr_eq(&self.writer, &staged.writer.reservation) {
            return Err(SnapshotError::internal(
                "staged snapshot belongs to another store",
            ));
        }
        let Staged {
            writer: _writer,
            state,
            lineage,
        } = staged;
        let candidate = match state {
            StagedState::Unchanged(current) => {
                return Ok(ApplyOutcome {
                    snapshot: current,
                    changed: false,
                });
            }
            StagedState::Validated(candidate) => candidate,
        };
        // The writer reservation is still held (the token carries it),
        // so the committed state cannot have moved since `stage`: this
        // write is a plain publication, not a re-negotiation.
        // Lineage and snapshot publish together under the held writer
        // reservation: readers of `current` never pair a snapshot with
        // another lineage's rules.
        {
            let mut committed = self
                .lineage
                .write()
                .map_err(|_| SnapshotError::internal("snapshot store lock poisoned"))?;
            *committed = Some(lineage);
        }
        let mut guard = self
            .current
            .write()
            .map_err(|_| SnapshotError::internal("snapshot store lock poisoned"))?;
        *guard = Some(Arc::clone(&candidate));
        Ok(ApplyOutcome {
            snapshot: candidate,
            changed: true,
        })
    }

    fn validate(
        &self,
        generation: u64,
        generations: CompositionGenerations,
        source_raw: StateSnapshot,
        snapshot: StateSnapshot,
        now: UnixTime,
    ) -> Result<ValidatedSnapshot, SnapshotError> {
        let config = snapshot
            .config
            .as_ref()
            .ok_or_else(|| SnapshotError::invalid("config snapshot is required"))?;
        validate_config(config)?;
        validate_backends(&snapshot.backends)?;
        validate_namespaces(&snapshot.namespaces)?;
        let (frontend_tls, frontend_server_config) =
            self.validate_tls("frontend_tls", config.frontend_tls.as_ref(), now, true)?;
        let (backend_tls, _) =
            self.validate_tls("backend_tls", config.backend_tls.as_ref(), now, false)?;
        if config.require_backend_tls
            && backend_tls.roots.is_empty()
            && !backend_tls.skip_ca_verification
        {
            return Err(SnapshotError::invalid(
                "require_backend_tls needs backend CA or skip_ca_verification",
            ));
        }
        Ok(ValidatedSnapshot {
            generation,
            generations,
            source_raw,
            raw: snapshot,
            frontend_server_config,
            frontend_tls,
            backend_tls,
        })
    }

    fn validate_tls(
        &self,
        field: &str,
        policy: Option<&TlsPolicy>,
        now: UnixTime,
        frontend: bool,
    ) -> Result<(ValidatedTlsPolicy, Option<Arc<ServerConfig>>), SnapshotError> {
        let policy = policy.cloned().unwrap_or_default();
        validate_tls_policy(field, &policy, frontend)?;
        let certificate_chain = self.load_certificate_chain(field, &policy, now)?;
        let private_key = self.load_private_key(field, &policy)?;
        let roots = self.load_roots(field, &policy, now)?;
        let server_config = Self::build_server_identity(
            field,
            &policy,
            &certificate_chain,
            private_key.as_deref(),
            &roots,
            frontend,
        )?;
        Ok((
            ValidatedTlsPolicy {
                certificate_chain,
                private_key,
                roots,
                minimum_version: policy.minimum_version,
                allowed_common_names: policy.allowed_common_names,
                skip_ca_verification: policy.skip_ca_verification,
            },
            server_config,
        ))
    }

    fn load_certificate_chain(
        &self,
        field: &str,
        policy: &TlsPolicy,
        now: UnixTime,
    ) -> Result<Vec<CertificateDer<'static>>, SnapshotError> {
        if policy.certificate_path.is_empty() {
            return Ok(Vec::new());
        }
        let bytes = self.read_tls_file(&policy.certificate_path, field)?;
        let certificates = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SnapshotError::invalid(format!("{field} certificate PEM is invalid")))?;
        if certificates.is_empty() {
            return Err(SnapshotError::invalid(format!(
                "{field} certificate chain is empty"
            )));
        }
        if certificates.len() > MAX_CERTIFICATE_CHAIN_LENGTH {
            return Err(SnapshotError::invalid(format!(
                "{field} certificate chain exceeds 64 entries"
            )));
        }
        for certificate in &certificates {
            validate_certificate_time(certificate, now, field)?;
        }
        Ok(certificates)
    }

    fn load_roots(
        &self,
        field: &str,
        policy: &TlsPolicy,
        now: UnixTime,
    ) -> Result<RootCertStore, SnapshotError> {
        let mut roots = RootCertStore::empty();
        if policy.ca_path.is_empty() {
            return Ok(roots);
        }
        let bytes = self.read_tls_file(&policy.ca_path, field)?;
        let certificates = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SnapshotError::invalid(format!("{field} CA PEM is invalid")))?;
        if certificates.is_empty() {
            return Err(SnapshotError::invalid(format!(
                "{field} CA bundle is empty"
            )));
        }
        if certificates.len() > MAX_CA_CERTIFICATES {
            return Err(SnapshotError::invalid(format!(
                "{field} CA bundle exceeds 256 entries"
            )));
        }
        for certificate in certificates {
            validate_certificate_time(&certificate, now, field)?;
            roots.add(certificate).map_err(|_| {
                SnapshotError::invalid(format!("{field} CA certificate is invalid"))
            })?;
        }
        Ok(roots)
    }

    fn load_private_key(
        &self,
        field: &str,
        policy: &TlsPolicy,
    ) -> Result<Option<Arc<PrivateKeyDer<'static>>>, SnapshotError> {
        if policy.private_key_path.is_empty() {
            return Ok(None);
        }
        let key_bytes = self.read_tls_file(&policy.private_key_path, field)?;
        let mut private_keys = PrivateKeyDer::pem_slice_iter(&key_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| SnapshotError::invalid(format!("{field} private key PEM is invalid")))?;
        if private_keys.len() != 1 {
            return Err(SnapshotError::invalid(format!(
                "{field} must contain exactly one private key"
            )));
        }
        let private_key = private_keys
            .pop()
            .ok_or_else(|| SnapshotError::invalid(format!("{field} private key PEM is invalid")))?;
        Ok(Some(Arc::new(private_key)))
    }

    fn build_server_identity(
        field: &str,
        policy: &TlsPolicy,
        certificate_chain: &[CertificateDer<'static>],
        private_key: Option<&PrivateKeyDer<'static>>,
        roots: &RootCertStore,
        frontend: bool,
    ) -> Result<Option<Arc<ServerConfig>>, SnapshotError> {
        if certificate_chain.is_empty() {
            return Ok(None);
        }
        let private_key = private_key
            .ok_or_else(|| SnapshotError::invalid(format!("{field} private key is required")))?;
        // Go `GetMinTLSVer` selects 1.3 only for a `1.3` suffix. 1.0 and 1.1
        // are accepted; rustls cannot offer them, so the handshake floor is 1.2.
        let builder = if go_min_tls_version(&policy.minimum_version) == "1.3" {
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        } else {
            ServerConfig::builder()
        };
        let client_verifier = if frontend {
            build_client_verifier(field, policy, roots)?
        } else {
            WebPkiClientVerifier::no_client_auth()
        };
        let config = builder
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certificate_chain.to_vec(), private_key.clone_key())
            .map_err(|_| {
                SnapshotError::invalid(format!("{field} certificate and private key do not match"))
            })?;
        Ok(Some(Arc::new(config)))
    }

    fn read_tls_file(&self, path: &str, field: &str) -> Result<Vec<u8>, SnapshotError> {
        let path = Path::new(path);
        // Go opens certificate paths with `os.ReadFile`, which resolves a
        // relative path against the process working directory.
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()
                .map_err(|_| SnapshotError::invalid(format!("{field} file is unavailable")))?
                .join(path)
        };
        let canonical = fs::canonicalize(&absolute)
            .map_err(|_| SnapshotError::invalid(format!("{field} file is unavailable")))?;
        // An empty allowlist matches Go: there is no extra root jail. A
        // configured `--tls-root` or `tls-allowed-roots` still confines reads.
        if !self.allowed_tls_roots.is_empty()
            && !self
                .allowed_tls_roots
                .iter()
                .any(|root| canonical.starts_with(root))
        {
            return Err(SnapshotError::invalid(format!(
                "{field} file is outside configured TLS roots"
            )));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&canonical)
            .map_err(|_| SnapshotError::invalid(format!("{field} file is unavailable")))?;
        let metadata = file
            .metadata()
            .map_err(|_| SnapshotError::invalid(format!("{field} file is unavailable")))?;
        if !metadata.is_file() || metadata.len() > MAX_TLS_FILE_BYTES {
            return Err(SnapshotError::invalid(format!(
                "{field} file must be a regular file no larger than 16 MiB"
            )));
        }
        let capacity = usize::try_from(metadata.len())
            .map_err(|_| SnapshotError::invalid(format!("{field} file is too large")))?;
        let mut contents = Vec::with_capacity(capacity);
        file.read_to_end(&mut contents)
            .map_err(|_| SnapshotError::invalid(format!("{field} file cannot be read")))?;
        if contents.len() as u64 > MAX_TLS_FILE_BYTES {
            return Err(SnapshotError::invalid(format!("{field} file is too large")));
        }
        Ok(contents)
    }
}

fn build_client_verifier(
    field: &str,
    policy: &TlsPolicy,
    roots: &RootCertStore,
) -> Result<Arc<dyn ClientCertVerifier>, SnapshotError> {
    if roots.is_empty() {
        return Ok(WebPkiClientVerifier::no_client_auth());
    }
    let builder = WebPkiClientVerifier::builder(Arc::new(roots.clone()));
    let verifier = if policy.skip_ca_verification && policy.allowed_common_names.is_empty() {
        builder.allow_unauthenticated().build()
    } else {
        builder.build()
    }
    .map_err(|_| SnapshotError::invalid(format!("{field} client CA is invalid")))?;
    if policy.allowed_common_names.is_empty() {
        return Ok(verifier);
    }
    Ok(Arc::new(CommonNameClientVerifier {
        inner: verifier,
        allowed_common_names: policy.allowed_common_names.iter().cloned().collect(),
    }))
}

/// Go `GetMinTLSVer` suffix match. Empty and unrecognized values are TLS 1.2.
fn go_min_tls_version(value: &str) -> &'static str {
    if value.ends_with("1.0") {
        "1.0"
    } else if value.ends_with("1.1") {
        "1.1"
    } else if value.ends_with("1.2") {
        "1.2"
    } else if value.ends_with("1.3") {
        "1.3"
    } else {
        "1.2"
    }
}

fn validate_tls_policy(
    field: &str,
    policy: &TlsPolicy,
    frontend: bool,
) -> Result<(), SnapshotError> {
    if policy.certificate_path.is_empty() != policy.private_key_path.is_empty() {
        return Err(SnapshotError::invalid(format!(
            "{field} certificate and private key must be configured together"
        )));
    }
    if frontend
        && policy.certificate_path.is_empty()
        && (!policy.ca_path.is_empty()
            || !policy.allowed_common_names.is_empty()
            || policy.skip_ca_verification)
    {
        return Err(SnapshotError::invalid(
            "frontend_tls client-auth policy requires a server certificate",
        ));
    }
    if !policy.allowed_common_names.is_empty() && policy.ca_path.is_empty() {
        return Err(SnapshotError::invalid(format!(
            "{field}.allowed_common_names requires a CA"
        )));
    }
    if policy.allowed_common_names.len() > 1024
        || policy
            .allowed_common_names
            .iter()
            .any(|name| name.trim().is_empty() || name.len() > 255)
    {
        return Err(SnapshotError::invalid(format!(
            "{field}.allowed_common_names is invalid"
        )));
    }
    Ok(())
}

fn validate_config(config: &ConfigSnapshot) -> Result<(), SnapshotError> {
    // `traffic_replay_enabled` stays in the snapshot so the checksum matches
    // Go. The native process does not capture or replay; that is reported by
    // the traffic API, and it does not reject the serving generation.
    if !config.high_memory_reject_threshold.is_finite()
        || config.high_memory_reject_threshold < 0.0
        || config.high_memory_reject_threshold > 1.0
        || (config.high_memory_reject_threshold > 0.0 && config.high_memory_reject_threshold < 0.5)
    {
        return Err(SnapshotError::invalid(
            "high_memory_reject_threshold must be zero or between 0.5 and 1.0",
        ));
    }
    if !(MIN_CONNECTION_BUFFER_BYTES..=MAX_CONNECTION_BUFFER_BYTES)
        .contains(&config.connection_buffer_bytes)
    {
        return Err(SnapshotError::invalid(
            "connection_buffer_bytes must be between 1 KiB and 16 MiB",
        ));
    }
    validate_keepalive("frontend_keepalive", config.frontend_keepalive.as_ref())?;
    validate_keepalive(
        "healthy_backend_keepalive",
        config.healthy_backend_keepalive.as_ref(),
    )?;
    validate_keepalive(
        "unhealthy_backend_keepalive",
        config.unhealthy_backend_keepalive.as_ref(),
    )?;
    match ProxyProtocolMode::try_from(config.proxy_protocol) {
        Ok(ProxyProtocolMode::Disabled | ProxyProtocolMode::V2) => {}
        _ => {
            return Err(SnapshotError::invalid(
                "proxy_protocol must be disabled or v2",
            ));
        }
    }
    if config.listeners.is_empty() || config.listeners.len() > MAX_LISTENERS {
        return Err(SnapshotError::invalid(
            "listeners must contain between 1 and 4096 entries",
        ));
    }
    let mut listener_names = BTreeSet::new();
    let mut listener_addresses = BTreeSet::new();
    for listener in &config.listeners {
        if listener.name.is_empty() || listener.name.len() > 128 {
            return Err(SnapshotError::invalid("listener name is invalid"));
        }
        // Port 0 is Go's empty `proxy.addr`: `net.Listen("tcp", "")` binds an
        // ephemeral port instead of failing configuration validation.
        if listener.port > u32::from(u16::MAX) {
            return Err(SnapshotError::invalid("listener port is invalid"));
        }
        validate_host(&listener.address, "listener address")?;
        if !listener_names.insert(listener.name.as_str())
            || !listener_addresses.insert((listener.address.as_str(), listener.port))
        {
            return Err(SnapshotError::invalid("listeners contain a duplicate"));
        }
    }
    for cidr in &config.public_cidrs {
        validate_cidr(cidr, "public CIDR")?;
    }
    if config.server_version.is_empty() || config.server_version.len() > 255 {
        return Err(SnapshotError::invalid("server_version is invalid"));
    }
    Ok(())
}

fn validate_keepalive(
    field: &str,
    keepalive: Option<&KeepalivePolicy>,
) -> Result<(), SnapshotError> {
    if keepalive.is_none() {
        return Err(SnapshotError::invalid(format!("{field} is required")));
    }
    Ok(())
}

fn validate_backends(backends: &[BackendSnapshot]) -> Result<(), SnapshotError> {
    if backends.len() > MAX_BACKENDS {
        return Err(SnapshotError::invalid("too many backends"));
    }
    let mut identifiers = BTreeSet::new();
    for backend in backends {
        if backend.backend_id.is_empty() || backend.backend_id.len() > 255 {
            return Err(SnapshotError::invalid("backend_id is invalid"));
        }
        if !identifiers.insert(backend.backend_id.as_str()) {
            return Err(SnapshotError::invalid("backend_id is duplicated"));
        }
        validate_host_port(&backend.address, "backend address")?;
        // A clusterless backend is the honest legacy projection: static
        // `backend.instances` carry no cluster name at all (DPL-07).
        // Empty is legal; a present name is length-bounded.
        if backend.cluster_name.len() > 255 {
            return Err(SnapshotError::invalid("backend cluster_name is invalid"));
        }
        for cidr in &backend.cidrs {
            validate_cidr(cidr, "backend CIDR")?;
        }
        if backend.labels.len() > 1024
            || backend
                .labels
                .iter()
                .any(|(key, value)| key.len() > 1024 || value.len() > 4096)
        {
            return Err(SnapshotError::invalid("backend labels exceed bounds"));
        }
    }
    Ok(())
}

fn validate_namespaces(namespaces: &[NamespaceSnapshot]) -> Result<(), SnapshotError> {
    if namespaces.len() > MAX_NAMESPACES {
        return Err(SnapshotError::invalid("too many namespaces"));
    }
    let mut names = BTreeSet::new();
    for namespace in namespaces {
        if namespace.name.is_empty() || namespace.name.len() > 255 {
            return Err(SnapshotError::invalid("namespace name is invalid"));
        }
        if !names.insert(namespace.name.as_str()) {
            return Err(SnapshotError::invalid("namespace name is duplicated"));
        }
        // An empty backend_cluster is a legal, honest projection: Go
        // namespaces carry no cluster binding of their own, so the
        // cluster is reported only when every backend of the namespace
        // agrees (DPL-07). A namespace with no backends yet (the boot
        // default) or a mixed-cluster namespace is unscoped, not
        // invalid.
        if namespace.backend_cluster.len() > 255 {
            return Err(SnapshotError::invalid(
                "namespace backend_cluster is invalid",
            ));
        }
        if namespace.users.len() > 65_536
            || namespace
                .users
                .iter()
                .any(|user| user.is_empty() || user.len() > 255)
        {
            return Err(SnapshotError::invalid("namespace users are invalid"));
        }
    }
    Ok(())
}

fn validate_host(host: &str, field: &str) -> Result<(), SnapshotError> {
    if host.len() > 255
        || host.chars().any(char::is_whitespace)
        || host.contains(['/', '[', ']'])
        || (!host.is_empty()
            && IpAddr::from_str(host).is_err()
            && !host.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
            }))
    {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn validate_host_port(value: &str, field: &str) -> Result<(), SnapshotError> {
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    };
    if host.contains(':') && !(value.starts_with('[') && host.ends_with(']')) {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    }
    let host = host.trim_matches(['[', ']']);
    validate_host(host, field)?;
    if host.is_empty() || port.parse::<u16>().map_or(true, |parsed| parsed == 0) {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn validate_cidr(value: &str, field: &str) -> Result<(), SnapshotError> {
    let Some((address, prefix)) = value.split_once('/') else {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    };
    let address = IpAddr::from_str(address)
        .map_err(|_| SnapshotError::invalid(format!("{field} is invalid")))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|_| SnapshotError::invalid(format!("{field} is invalid")))?;
    let maximum = if address.is_ipv4() { 32 } else { 128 };
    if prefix > maximum {
        return Err(SnapshotError::invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn validate_certificate_time(
    certificate: &CertificateDer<'_>,
    now: UnixTime,
    field: &str,
) -> Result<(), SnapshotError> {
    let (_, certificate) = parse_x509_certificate(certificate.as_ref())
        .map_err(|_| SnapshotError::invalid(format!("{field} certificate DER is invalid")))?;
    let timestamp = i64::try_from(now.as_secs())
        .map_err(|_| SnapshotError::invalid("snapshot validation time is invalid"))?;
    let validation_time = ASN1Time::from_timestamp(timestamp)
        .map_err(|_| SnapshotError::invalid("snapshot validation time is invalid"))?;
    if !certificate.validity().is_valid_at(validation_time) {
        return Err(SnapshotError::invalid(format!(
            "{field} certificate is expired or not yet valid"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rcgen::generate_simple_self_signed;

    use super::*;

    #[test]
    fn frontend_client_common_name_is_enforced_after_ca_validation()
    -> Result<(), Box<dyn std::error::Error>> {
        let generated = generate_simple_self_signed(["client.test".to_owned()])?;
        let certificate = CertificateDer::from(generated.cert.der().to_vec());
        let mut roots = RootCertStore::empty();
        roots.add(certificate.clone())?;
        let now = UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000));

        let allowed = TlsPolicy {
            allowed_common_names: vec!["rcgen self signed cert".to_owned()],
            skip_ca_verification: true,
            ..Default::default()
        };
        let verifier = build_client_verifier("frontend_tls", &allowed, &roots)?;
        assert!(verifier.client_auth_mandatory());
        verifier.verify_client_cert(&certificate, &[], now)?;

        let denied = TlsPolicy {
            allowed_common_names: vec!["different-client".to_owned()],
            ..Default::default()
        };
        let verifier = build_client_verifier("frontend_tls", &denied, &roots)?;
        let error = verifier
            .verify_client_cert(&certificate, &[], now)
            .err()
            .ok_or("unconfigured common name unexpectedly allowed")?;
        assert!(error.to_string().contains("common name is not allowed"));
        Ok(())
    }

    #[test]
    fn unscoped_namespaces_are_valid() {
        // DPL-07 contract: a namespace with no unambiguous cluster
        // (no backends yet, or backends across clusters) projects an
        // empty backend_cluster and MUST be accepted — the boot-time
        // default namespace always starts this way.
        let namespaces = vec![
            NamespaceSnapshot {
                name: "default".to_owned(),
                users: Vec::new(),
                backend_cluster: String::new(),
            },
            NamespaceSnapshot {
                name: "ns-alpha".to_owned(),
                users: vec!["alice".to_owned()],
                backend_cluster: "alpha".to_owned(),
            },
        ];
        assert!(validate_namespaces(&namespaces).is_ok());

        let overlong = vec![NamespaceSnapshot {
            name: "ns".to_owned(),
            users: Vec::new(),
            backend_cluster: "c".repeat(256),
        }];
        assert!(validate_namespaces(&overlong).is_err());
    }
}
