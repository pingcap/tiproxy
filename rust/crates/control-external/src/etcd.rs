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

//! Fenced `etcd-client` construction for PD's etcd v3 API.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use control_plane::OwnerToken;
use control_plane::permit::CommitPermit;
use etcd_client::Client;
use http::Uri;
use thiserror::Error;

use crate::explicit_dns::ResolveError;

/// Maximum configured PD endpoints for one client.
pub const MAX_ETCD_ENDPOINTS: usize = 32;
/// Maximum byte length of one configured endpoint.
pub const MAX_ETCD_ENDPOINT_BYTES: usize = 2_048;
/// Maximum explicit nameservers threaded into one cluster client.
pub const MAX_NS_SERVERS: usize = 32;
const MAX_TIMEOUT: Duration = Duration::from_secs(300);

/// The minimum accepted TLS protocol version for an etcd connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EtcdTlsVersion {
    /// TLS 1.2.
    V1_2,
    /// TLS 1.3.
    V1_3,
}

impl EtcdTlsVersion {
    /// Parses a minimum version with Go `GetMinTLSVer` suffix rules.
    ///
    /// `""` keeps the default floor. A `1.3` suffix selects TLS 1.3 only.
    /// `1.0` and `1.1` are accepted and floored at TLS 1.2, because rustls
    /// cannot offer the older protocols. Any other non-empty value falls
    /// back to TLS 1.2, matching Go's warning-and-default path.
    ///
    /// # Errors
    ///
    /// This function does not reject a version string. The `Result` is kept
    /// so existing callers can still propagate a configuration error.
    #[allow(clippy::unnecessary_wraps)]
    pub fn parse(value: &str) -> Result<Option<Self>, EtcdConfigError> {
        if value.is_empty() {
            return Ok(None);
        }
        // Same suffix order as Go `GetMinTLSVer`. 1.0 and 1.1 cannot be
        // offered by rustls, so they use the 1.2 floor.
        if value.ends_with("1.0") || value.ends_with("1.1") || value.ends_with("1.2") {
            return Ok(Some(Self::V1_2));
        }
        if value.ends_with("1.3") {
            return Ok(Some(Self::V1_3));
        }
        Ok(Some(Self::V1_2))
    }
}

/// Advanced TLS verification policy for an etcd connection, mirroring the frozen
/// backend `cluster-tls` fields. The default verifies the server against the
/// configured CA with no version floor and no name pinning — the behavior the
/// config-persistence and election owners rely on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EtcdTlsPolicy {
    /// Minimum accepted TLS protocol version, or `None` for the default floor.
    pub minimum_version: Option<EtcdTlsVersion>,
    /// Exact-match allowlist of accepted server-certificate common names. Empty
    /// disables common-name pinning.
    pub allowed_common_names: Vec<String>,
    /// Skip CA-chain and hostname trust while still performing the TLS handshake
    /// signature check (mirrors Go `skip-ca`). A CA is not required when set;
    /// any configured `allowed_common_names` is still enforced.
    pub skip_ca_verification: bool,
}

/// Maximum accepted common-name pins, matching the serving TLS contract.
const MAX_TLS_COMMON_NAMES: usize = 1024;
/// Maximum accepted length of one common-name pin.
const MAX_TLS_COMMON_NAME_LEN: usize = 255;

/// Trims, bounds, sorts, and de-duplicates the common-name allowlist.
///
/// # Errors
///
/// Rejects an over-count list or an entry that is empty or over-length after
/// trimming.
fn normalized_common_names(names: Vec<String>) -> Result<Vec<String>, EtcdConfigError> {
    if names.len() > MAX_TLS_COMMON_NAMES {
        return Err(EtcdConfigError::TooManyCommonNames);
    }
    let mut normalized = Vec::with_capacity(names.len());
    for name in names {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(EtcdConfigError::EmptyCommonName);
        }
        if trimmed.len() > MAX_TLS_COMMON_NAME_LEN {
            return Err(EtcdConfigError::CommonNameTooLong);
        }
        normalized.push(trimmed.to_owned());
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

/// Splits a normalized `host:port` (or bracketed `[<ip>]:port`) entry into its
/// host and port, enforcing exactly the shape produced by the config layer's
/// `normalize_ns_server`/`split_host_port`: a non-empty host, an explicit numeric
/// port that is not zero, and a bracketed host that is a valid IP literal (never
/// a bracketed name). Returns the inner host (brackets stripped).
///
/// This is the single spec shared by the two direct-API validators — this
/// config's [`EtcdClientConfig::with_ns_servers`] and the resolver's
/// `NsEntry::parse` — so both reject the same malformed inputs as the config
/// projection.
pub(crate) fn split_normalized_host_port(value: &str) -> Option<(&str, u16)> {
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        // Bracketed form must wrap a valid IP literal, mirroring the config.
        let (host, port) = rest.split_once("]:")?;
        if host.parse::<std::net::IpAddr>().is_err() {
            return None;
        }
        (host, port)
    } else {
        let (host, port) = value.rsplit_once(':')?;
        if host.contains(':') {
            // Unbracketed multi-colon host is not a normalized form.
            return None;
        }
        (host, port)
    };
    if host.is_empty() {
        return None;
    }
    // An explicit numeric port that is not zero, matching `split_host_port`.
    let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;
    Some((host, port))
}

/// Validated mTLS material and verification policy for an etcd connection.
#[derive(Clone, PartialEq, Eq)]
pub struct EtcdTlsConfig {
    ca_certificate_pem: Option<Vec<u8>>,
    identity: Option<(Vec<u8>, Vec<u8>)>,
    domain_name: Option<String>,
    policy: EtcdTlsPolicy,
}

impl fmt::Debug for EtcdTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdTlsConfig")
            .field(
                "ca_certificate_configured",
                &self.ca_certificate_pem.is_some(),
            )
            .field("client_identity_configured", &self.identity.is_some())
            .field("domain_name", &self.domain_name)
            .field("minimum_version", &self.policy.minimum_version)
            .field("common_name_pins", &self.policy.allowed_common_names.len())
            .field("skip_ca_verification", &self.policy.skip_ca_verification)
            .finish()
    }
}

impl EtcdTlsConfig {
    /// Creates TLS material and policy from an optional CA bundle, an optional
    /// client identity, an optional explicit server name, and a verification
    /// policy.
    ///
    /// A CA bundle is required unless [`EtcdTlsPolicy::skip_ca_verification`] is
    /// set; when it is set, the connection still performs the TLS handshake and
    /// enforces any `allowed_common_names`.
    ///
    /// # Errors
    ///
    /// Rejects a missing CA when CA verification is required, incomplete client
    /// identities, empty explicit domain names, and empty common-name entries.
    pub fn new(
        ca_certificate_pem: Option<Vec<u8>>,
        client_certificate_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        domain_name: Option<String>,
        policy: EtcdTlsPolicy,
    ) -> Result<Self, EtcdConfigError> {
        let EtcdTlsPolicy {
            minimum_version,
            allowed_common_names,
            skip_ca_verification,
        } = policy;
        let ca_certificate_pem = ca_certificate_pem.filter(|ca| !ca.is_empty());
        if !skip_ca_verification && ca_certificate_pem.is_none() {
            return Err(EtcdConfigError::EmptyCaCertificate);
        }
        let identity = match (client_certificate_pem, client_key_pem) {
            (Some(certificate), Some(key)) if !certificate.is_empty() && !key.is_empty() => {
                Some((certificate, key))
            }
            (None, None) => None,
            _ => return Err(EtcdConfigError::IncompleteClientIdentity),
        };
        if domain_name.as_deref().is_some_and(str::is_empty) {
            return Err(EtcdConfigError::EmptyDomainName);
        }
        let allowed_common_names = normalized_common_names(allowed_common_names)?;
        Ok(Self {
            ca_certificate_pem,
            identity,
            domain_name,
            policy: EtcdTlsPolicy {
                minimum_version,
                allowed_common_names,
                skip_ca_verification,
            },
        })
    }

    /// Builds the client TLS configuration for the custom etcd transport from
    /// the generation-bound material and verification policy (the advanced
    /// `minimum_version` / `allowed_common_names` / `skip_ca_verification` that
    /// the tonic `TlsOptions` path cannot express).
    ///
    /// # Errors
    ///
    /// Returns [`EtcdConfigError::TlsSetup`] when the material cannot form a
    /// client configuration.
    pub fn client_config(&self) -> Result<Arc<rustls::ClientConfig>, EtcdConfigError> {
        crate::tls::build_client_config(
            self.ca_certificate_pem.as_deref(),
            self.identity
                .as_ref()
                .map(|(certificate, key)| (certificate.as_slice(), key.as_slice())),
            &self.policy,
        )
    }

    /// Returns the explicit TLS server-name override used for SNI, if set.
    pub(crate) fn domain_name(&self) -> Option<&str> {
        self.domain_name.as_deref()
    }
}

/// Validated production connection policy shared by later control modules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdClientConfig {
    endpoints: Vec<String>,
    connect_timeout: Duration,
    request_timeout: Duration,
    keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
    tcp_keep_alive: Duration,
    tls: Option<EtcdTlsConfig>,
    ns_servers: Arc<[Arc<str>]>,
}

impl EtcdClientConfig {
    /// Creates the Go-parity connection policy for a bounded endpoint set.
    ///
    /// Bare `host:port` entries are supported because `TiProxy`'s existing
    /// `proxy.pd-addrs` uses that form. A TLS configuration upgrades bare
    /// entries to HTTPS; explicit HTTP/HTTPS schemes must agree with it.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, malformed, path-bearing, or policy-mismatched
    /// endpoints.
    pub fn new(
        endpoints: impl IntoIterator<Item = String>,
        tls: Option<EtcdTlsConfig>,
    ) -> Result<Self, EtcdConfigError> {
        let endpoints = normalize_endpoints(endpoints, tls.is_some())?;
        Ok(Self {
            endpoints,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(3),
            tcp_keep_alive: Duration::from_secs(30),
            tls,
            ns_servers: Arc::from([]),
        })
    }

    /// Threads the cluster's explicit, already-normalized nameservers onto this
    /// config, preserving Go's stable lexicographic order and its duplicates.
    ///
    /// The list is a shared, immutable snapshot; the empty list means no explicit
    /// nameservers. Each entry must be a normalized `host:port` (or bracketed
    /// `[v6]:port`) with an explicit numeric port, matching the config layer's
    /// `normalize_ns_server`. When non-empty, the connector resolves the target
    /// host through the shared owner-fenced explicit-nameserver resolver over
    /// these servers — never the system resolver; only a nameserver *hostname*
    /// entry is bootstrapped through the system resolver. An empty list keeps the
    /// system-resolver path for the target host.
    ///
    /// # Errors
    ///
    /// Returns [`EtcdConfigError::TooManyNsServers`] when the list exceeds
    /// [`MAX_NS_SERVERS`], or [`EtcdConfigError::InvalidNsServer`] when an entry
    /// is not a normalized `host:port`.
    pub fn with_ns_servers(mut self, ns_servers: Arc<[Arc<str>]>) -> Result<Self, EtcdConfigError> {
        if ns_servers.len() > MAX_NS_SERVERS {
            return Err(EtcdConfigError::TooManyNsServers {
                count: ns_servers.len(),
                maximum: MAX_NS_SERVERS,
            });
        }
        for (index, server) in ns_servers.iter().enumerate() {
            if split_normalized_host_port(server).is_none() {
                return Err(EtcdConfigError::InvalidNsServer { index });
            }
        }
        self.ns_servers = ns_servers;
        Ok(self)
    }

    /// Replaces all bounded timeout and keepalive values.
    ///
    /// # Errors
    ///
    /// Rejects zero or over-five-minute values and keepalive timeouts greater
    /// than their interval.
    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        request_timeout: Duration,
        keep_alive_interval: Duration,
        keep_alive_timeout: Duration,
        tcp_keep_alive: Duration,
    ) -> Result<Self, EtcdConfigError> {
        for (name, value) in [
            ("connect_timeout", connect_timeout),
            ("request_timeout", request_timeout),
            ("keep_alive_interval", keep_alive_interval),
            ("keep_alive_timeout", keep_alive_timeout),
            ("tcp_keep_alive", tcp_keep_alive),
        ] {
            if value.is_zero() || value > MAX_TIMEOUT {
                return Err(EtcdConfigError::InvalidDuration { name, value });
            }
        }
        if keep_alive_timeout > keep_alive_interval {
            return Err(EtcdConfigError::KeepAliveTimeoutExceedsInterval);
        }
        self.connect_timeout = connect_timeout;
        self.request_timeout = request_timeout;
        self.keep_alive_interval = keep_alive_interval;
        self.keep_alive_timeout = keep_alive_timeout;
        self.tcp_keep_alive = tcp_keep_alive;
        Ok(self)
    }

    /// Returns normalized endpoints in deterministic order.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Returns the validated explicit nameservers in their frozen, duplicate-
    /// preserving order (empty when none are configured).
    #[must_use]
    pub fn ns_servers(&self) -> &[Arc<str>] {
        &self.ns_servers
    }

    /// Returns the per-operation request deadline.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the whole-connection establishment budget for the custom
    /// transport (single DNS + TCP + TLS deadline per endpoint).
    pub(crate) const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the HTTP/2 keepalive ping interval for the custom transport.
    pub(crate) const fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval
    }

    /// Returns the HTTP/2 keepalive ping timeout for the custom transport.
    pub(crate) const fn keep_alive_timeout(&self) -> Duration {
        self.keep_alive_timeout
    }

    /// Returns the TCP keepalive idle time applied to each dialed socket.
    pub(crate) const fn tcp_keep_alive(&self) -> Duration {
        self.tcp_keep_alive
    }

    /// Returns the validated TLS material and policy, if TLS is configured.
    pub(crate) fn tls(&self) -> Option<&EtcdTlsConfig> {
        self.tls.as_ref()
    }

    /// Returns the advanced TLS verification policy, if TLS is configured.
    ///
    /// Exposed so the composition root can assert the topology→transport policy
    /// mapping (skip-CA, common-name pins, minimum version) that
    /// [`EtcdConnector::connect`] threads into the custom transport.
    #[must_use]
    pub fn tls_policy(&self) -> Option<&EtcdTlsPolicy> {
        self.tls.as_ref().map(|tls| &tls.policy)
    }
}

/// Configuration rejection at the external boundary.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EtcdConfigError {
    /// At least one endpoint is required.
    #[error("at least one etcd endpoint is required")]
    EmptyEndpoints,
    /// The configured endpoint count is bounded.
    #[error("too many etcd endpoints: {count} exceeds {maximum}")]
    TooManyEndpoints {
        /// Observed endpoint count.
        count: usize,
        /// Maximum endpoint count.
        maximum: usize,
    },
    /// One endpoint is empty, malformed, or over its byte bound.
    #[error("invalid etcd endpoint at index {index}: {reason}")]
    InvalidEndpoint {
        /// Zero-based endpoint index.
        index: usize,
        /// Payload-free rejection reason.
        reason: &'static str,
    },
    /// Duplicate normalized endpoint.
    #[error("duplicate etcd endpoint at index {index}")]
    DuplicateEndpoint {
        /// Zero-based endpoint index.
        index: usize,
    },
    /// Explicit endpoint scheme disagrees with the TLS policy.
    #[error("etcd endpoint scheme and TLS policy disagree at index {index}")]
    TlsSchemeMismatch {
        /// Zero-based endpoint index.
        index: usize,
    },
    /// TLS requires at least one trust anchor unless CA verification is skipped.
    #[error("etcd TLS CA certificate is empty")]
    EmptyCaCertificate,
    /// Client certificate and key must be configured together.
    #[error("etcd TLS client certificate and key must both be configured")]
    IncompleteClientIdentity,
    /// An explicit TLS server name cannot be empty.
    #[error("etcd TLS domain name is empty")]
    EmptyDomainName,
    /// The minimum TLS version must be unset, `1.2`, or `1.3`.
    #[error("unsupported etcd TLS minimum version")]
    UnsupportedTlsVersion,
    /// An allowed common name entry cannot be empty.
    #[error("etcd TLS allowed common name is empty")]
    EmptyCommonName,
    /// An allowed common name entry exceeds its length bound.
    #[error("etcd TLS allowed common name is too long")]
    CommonNameTooLong,
    /// The allowed common name list exceeds its count bound.
    #[error("too many etcd TLS allowed common names")]
    TooManyCommonNames,
    /// The configured nameserver count is bounded.
    #[error("too many nameservers: {count} exceeds {maximum}")]
    TooManyNsServers {
        /// Observed nameserver count.
        count: usize,
        /// Maximum nameserver count.
        maximum: usize,
    },
    /// One nameserver is not a normalized `host:port`.
    #[error("invalid nameserver at index {index}")]
    InvalidNsServer {
        /// Zero-based nameserver index.
        index: usize,
    },
    /// The explicit-nameserver resolver could not be constructed for a cluster
    /// that configures `ns_servers`. The payload-free [`ResolveError`] source
    /// carries the class without leaking any host or address.
    #[error("explicit nameserver resolver could not be built")]
    ExplicitResolver(#[source] ResolveError),
    /// The TLS material and policy could not form a client configuration.
    #[error("etcd TLS client configuration could not be built")]
    TlsSetup,
    /// Timeout and keepalive values are bounded.
    #[error("invalid {name} duration {value:?}")]
    InvalidDuration {
        /// Duration role.
        name: &'static str,
        /// Rejected duration.
        value: Duration,
    },
    /// HTTP/2 keepalive timeout cannot exceed its interval.
    #[error("etcd keepalive timeout exceeds its interval")]
    KeepAliveTimeoutExceedsInterval,
}

/// The dependency-layer cause behind an [`EtcdConnectError::Dependency`].
///
/// The custom owner-fenced transport is assembled from the validated config
/// before the client is created, so a connection failure can originate either in
/// that assembly (a configuration fault) or in the external client itself.
#[derive(Debug, Error)]
pub enum EtcdConnectSource {
    /// The external etcd client rejected or could not reach the endpoint set.
    #[error("etcd client connection failed")]
    Client(#[source] etcd_client::Error),
    /// The owner-fenced custom transport could not be built from the config.
    #[error("etcd transport configuration is invalid")]
    Configuration(#[source] EtcdConfigError),
}

/// Runtime connection failure with stable public classes.
#[derive(Debug, Error)]
pub enum EtcdConnectError {
    /// The originating control owner is no longer current.
    #[error("stale control owner")]
    StaleOwner,
    /// The external client rejected or could not reach the endpoint set, or the
    /// custom transport could not be built from the validated configuration.
    #[error("etcd dependency connection failed")]
    Dependency(#[source] EtcdConnectSource),
}

/// A fenced semantic etcd operation failure.
#[derive(Debug, Error)]
pub enum EtcdOperationError {
    /// The originating control owner is no longer current.
    #[error("stale control owner")]
    StaleOwner,
    /// The etcd operation failed while the owner remained current.
    #[error("etcd dependency operation failed")]
    Dependency(#[source] etcd_client::Error),
}

/// Factory that can create clients only for its exact owner generation.
#[derive(Clone)]
pub struct EtcdConnector {
    owner: OwnerToken,
    config: EtcdClientConfig,
}

impl EtcdConnector {
    /// Binds a validated config to the unique process-local owner token.
    #[must_use]
    pub const fn new(owner: OwnerToken, config: EtcdClientConfig) -> Self {
        Self { owner, config }
    }

    /// Binds the owner-fenced custom transport to a raw etcd client for the
    /// exact owner generation, rechecking the owner across each step.
    ///
    /// This does **not** establish the network. The custom endpoints built by
    /// [`crate::transport::build_custom_channel`] are lazy, and
    /// [`Client::from_channel`] with no credentials sends no RPC, so DNS, TCP,
    /// and TLS only happen on the first etcd operation — where they are covered
    /// by the owner-fenced dialer and the outer [`EtcdConnection::execute`]
    /// fence. Ownership is rechecked before interpreting each fallible step so a
    /// retired generation always wins over a build or client error: the fence
    /// sits *before* the `?` on both the transport build and the client
    /// creation.
    ///
    /// # Errors
    ///
    /// Returns [`EtcdConnectError::StaleOwner`] whenever the exact generation
    /// was released; otherwise returns [`EtcdConnectError::Dependency`] carrying
    /// the transport-configuration or client cause without retrying forever.
    pub async fn connect(&self) -> Result<EtcdConnection, EtcdConnectError> {
        if !self.owner.is_current() {
            return Err(EtcdConnectError::StaleOwner);
        }
        let built = crate::transport::build_custom_channel(&self.config, &self.owner);
        if !self.owner.is_current() {
            return Err(EtcdConnectError::StaleOwner);
        }
        let channel = built.map_err(|error| {
            EtcdConnectError::Dependency(EtcdConnectSource::Configuration(error))
        })?;
        let result = Client::from_channel(channel, None).await;
        if !self.owner.is_current() {
            return Err(EtcdConnectError::StaleOwner);
        }
        let client = result
            .map_err(|error| EtcdConnectError::Dependency(EtcdConnectSource::Client(error)))?;
        Ok(EtcdConnection {
            owner: self.owner.clone(),
            client,
            gate: None,
            additional_fence: None,
        })
    }
}

/// A revocable generation gate an [`EtcdConnection`] can carry in addition to its
/// process [`OwnerToken`].
///
/// The `OwnerToken` isolates process owners but not successive configuration
/// generations inside one process. A caller that reuses one long-lived channel
/// across configuration generations installs this gate on the connections it
/// hands out and revokes it when that generation is retired, so an in-flight
/// operation on a retired generation is fenced even while the process owner is
/// still current.
///
/// `Default` is derived and gives a **live** gate, because it delegates to
/// [`CommitPermit::default`], which is itself written to equal
/// `CommitPermit::new`. That delegation is the contract: a permit whose
/// `Default` ever became "already revoked" would silently make every
/// derived gate reject all I/O, so the initial value belongs with the
/// permit and not with a flag here.
#[derive(Clone, Debug, Default)]
pub struct GenerationGate {
    /// The shared authority. Built on `control-plane`'s primitive so that
    /// ownership, configuration and topology revoke through the same
    /// mechanism without depending on this crate -- `control-plane` is the
    /// root of the internal graph, and this one sits above it.
    permit: CommitPermit,
}

impl GenerationGate {
    /// Creates a live gate.
    #[must_use]
    pub fn new() -> Self {
        Self {
            permit: CommitPermit::new(),
        }
    }

    /// Revokes the gate. Every connection carrying it is retired at once, and any
    /// in-flight operation is fenced at its next ownership check.
    ///
    /// Takes the commit lock before revoking, so a [`Self::try_commit`]
    /// already running finishes first and a later one is refused.
    pub fn revoke(&self) {
        self.permit.revoke();
    }

    /// Runs `commit` if and only if the gate is still live, with revocation
    /// excluded for its duration. Returns whether it ran.
    ///
    /// See [`CommitPermit::commit`] for the contract the closure must meet:
    /// short, synchronous, and reaching for no lock a revoker holds first.
    pub fn try_commit<T>(&self, commit: impl FnOnce() -> T) -> Option<T> {
        self.permit.commit(commit)
    }

    /// The underlying authority, for a caller that must commit under this
    /// gate together with other permits.
    #[must_use]
    pub const fn permit(&self) -> &CommitPermit {
        &self.permit
    }

    /// Whether the gate is still live.
    ///
    /// A lock-free read for ordinary I/O fencing. It answers about the past
    /// the instant it returns; anything that must not outlive the
    /// generation belongs in [`Self::try_commit`].
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.permit.is_valid()
    }
}

/// An etcd client that cannot be borrowed after its owner — or its optional
/// generation gate — becomes stale.
pub struct EtcdConnection {
    owner: OwnerToken,
    client: Client,
    gate: Option<GenerationGate>,
    additional_fence: Option<Arc<dyn crate::IoFence>>,
}

impl EtcdConnection {
    /// Executes one semantic etcd operation for the current generation.
    ///
    /// The raw client never escapes this closure. Ownership is checked both
    /// before creating the operation future and after it resolves, so a result
    /// completed by a retired generation is discarded rather than committed by
    /// its caller.
    ///
    /// # Errors
    ///
    /// Returns a stale-owner failure before or after the await point, or the
    /// typed dependency failure from the operation itself.
    pub async fn execute<T, F>(&mut self, operation: F) -> Result<T, EtcdOperationError>
    where
        F: for<'client> FnOnce(
            &'client mut Client,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, etcd_client::Error>> + Send + 'client>,
        >,
    {
        if !self.is_current() {
            return Err(EtcdOperationError::StaleOwner);
        }
        let result = operation(&mut self.client).await;
        if !self.is_current() {
            return Err(EtcdOperationError::StaleOwner);
        }
        result.map_err(EtcdOperationError::Dependency)
    }

    /// Whether this connection is still authorized to issue I/O: its process
    /// owner is current AND its optional generation gate (if any) is still live.
    ///
    /// Callers that impose their own deadline around an operation (which drops
    /// the [`Self::execute`] future before its post-await fence can run) use this
    /// to re-check authorization in their timeout branch, so a retired owner or a
    /// revoked generation is not mistaken for a plain timeout.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.owner.is_current()
            && self.gate.as_ref().is_none_or(GenerationGate::is_live)
            && self
                .additional_fence
                .as_ref()
                .is_none_or(|fence| fence.is_live())
    }

    /// Clones this connection, sharing the same long-lived channel (so DNS
    /// cache, load balancing, and backoff state are preserved) and the same
    /// owner and generation gate.
    #[must_use]
    pub fn fork(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            client: self.client.clone(),
            gate: self.gate.clone(),
            additional_fence: self.additional_fence.clone(),
        }
    }

    /// Clones this connection under a generation gate: the fork shares the same
    /// long-lived channel and owner but is additionally retired the moment
    /// `gate` is revoked, even while the process owner remains current.
    #[must_use]
    pub fn fork_with_gate(&self, gate: GenerationGate) -> Self {
        Self {
            owner: self.owner.clone(),
            client: self.client.clone(),
            gate: Some(gate),
            additional_fence: self.additional_fence.clone(),
        }
    }

    /// Forks the same channel under an additional retained capability. Existing
    /// process, generation and prior extra fences are preserved conjunctively;
    /// each `execute` checks all of them before and after its operation.
    #[must_use]
    pub fn fork_with_fence(&self, fence: Arc<dyn crate::IoFence>) -> Self {
        let additional_fence = match &self.additional_fence {
            Some(previous) => Arc::new(crate::io_fence::OwnedCombinedFence(
                Arc::clone(previous),
                fence,
            )) as Arc<dyn crate::IoFence>,
            None => fence,
        };
        Self {
            owner: self.owner.clone(),
            client: self.client.clone(),
            gate: self.gate.clone(),
            additional_fence: Some(additional_fence),
        }
    }
}

fn normalize_endpoints(
    endpoints: impl IntoIterator<Item = String>,
    tls: bool,
) -> Result<Vec<String>, EtcdConfigError> {
    let collected: Vec<String> = endpoints.into_iter().collect();
    if collected.is_empty() {
        return Err(EtcdConfigError::EmptyEndpoints);
    }
    if collected.len() > MAX_ETCD_ENDPOINTS {
        return Err(EtcdConfigError::TooManyEndpoints {
            count: collected.len(),
            maximum: MAX_ETCD_ENDPOINTS,
        });
    }
    let mut seen = HashSet::with_capacity(collected.len());
    let mut normalized = Vec::with_capacity(collected.len());
    for (index, endpoint) in collected.into_iter().enumerate() {
        let endpoint = endpoint.trim();
        if endpoint.is_empty() || endpoint.len() > MAX_ETCD_ENDPOINT_BYTES {
            return Err(EtcdConfigError::InvalidEndpoint {
                index,
                reason: "empty or over byte bound",
            });
        }
        let explicit_scheme = endpoint.contains("://");
        let candidate = if explicit_scheme {
            endpoint.to_owned()
        } else {
            format!("{}://{endpoint}", if tls { "https" } else { "http" })
        };
        let uri: Uri = candidate
            .parse()
            .map_err(|_| EtcdConfigError::InvalidEndpoint {
                index,
                reason: "malformed URI",
            })?;
        let Some(scheme) = uri.scheme_str() else {
            return Err(EtcdConfigError::InvalidEndpoint {
                index,
                reason: "missing scheme",
            });
        };
        if !matches!(scheme, "http" | "https") || uri.authority().is_none() {
            return Err(EtcdConfigError::InvalidEndpoint {
                index,
                reason: "unsupported scheme or missing authority",
            });
        }
        if uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        {
            return Err(EtcdConfigError::InvalidEndpoint {
                index,
                reason: "credentials are not allowed",
            });
        }
        if uri
            .path_and_query()
            .is_some_and(|path| path.as_str() != "/")
        {
            return Err(EtcdConfigError::InvalidEndpoint {
                index,
                reason: "paths and queries are not allowed",
            });
        }
        if (scheme == "https") != tls {
            return Err(EtcdConfigError::TlsSchemeMismatch { index });
        }
        let normalized_endpoint = uri.to_string().trim_end_matches('/').to_owned();
        if !seen.insert(normalized_endpoint.clone()) {
            return Err(EtcdConfigError::DuplicateEndpoint { index });
        }
        normalized.push(normalized_endpoint);
    }
    normalized.sort_unstable();
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use control_plane::{OwnerScope, OwnershipRegistry};

    use super::{
        EtcdClientConfig, EtcdConfigError, EtcdTlsConfig, EtcdTlsPolicy, EtcdTlsVersion,
        GenerationGate,
    };

    #[tokio::test]
    async fn metric_etcd_forks_preserve_all_additional_fences()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = OwnershipRegistry::new();
        let owner = registry.claim(OwnerScope::Process, "metric-fork-fences")?;
        let connection = super::EtcdConnector::new(
            owner.token(),
            EtcdClientConfig::new(["127.0.0.1:1".to_owned()], None)?,
        )
        .connect()
        .await?;
        for revoke in 0..3 {
            let gates = [
                GenerationGate::new(),
                GenerationGate::new(),
                GenerationGate::new(),
            ];
            let mut fork = connection
                .fork_with_fence(std::sync::Arc::new(gates[0].clone()))
                .fork_with_gate(gates[1].clone())
                .fork_with_fence(std::sync::Arc::new(gates[2].clone()))
                .fork();
            assert!(fork.is_current());
            gates[revoke].revoke();
            assert!(!fork.is_current(), "METRIC_ETCD_FORK_CONJUNCTION");
            assert!(
                connection.is_current(),
                "original connection kept independently live"
            );
            let mut invoked = false;
            let result = fork
                .execute(|_| {
                    invoked = true;
                    Box::pin(async { Ok(()) })
                })
                .await;
            assert!(matches!(result, Err(super::EtcdOperationError::StaleOwner)));
            assert!(!invoked, "METRIC_ETCD_FORK_ZERO_IO");
        }
        Ok(())
    }

    #[test]
    fn a_new_generation_gate_is_live_and_revoke_propagates_to_clones() {
        let gate = GenerationGate::new();
        assert!(gate.is_live(), "a fresh gate is live");
        assert!(
            GenerationGate::default().is_live(),
            "the hand-written Default is a live gate, not a born-revoked one"
        );
        let clone = gate.clone();
        assert!(clone.is_live());
        // Revoking either handle retires all clones (shared flag).
        gate.revoke();
        assert!(!gate.is_live());
        assert!(
            !clone.is_live(),
            "revoke propagates to a clone sharing the gate"
        );
    }

    #[test]
    fn endpoints_are_bounded_normalized_and_tls_consistent() {
        let plain = EtcdClientConfig::new(
            [
                "127.0.0.1:2379".to_owned(),
                "http://localhost:2380".to_owned(),
            ],
            None,
        )
        .unwrap_or_else(|error| unreachable!("valid endpoints: {error}"));
        assert_eq!(
            plain.endpoints(),
            ["http://127.0.0.1:2379", "http://localhost:2380"]
        );
        assert_eq!(
            EtcdClientConfig::new(["https://localhost:2379".to_owned()], None),
            Err(EtcdConfigError::TlsSchemeMismatch { index: 0 })
        );
        assert_eq!(
            EtcdClientConfig::new(
                [
                    "localhost:2379".to_owned(),
                    "http://localhost:2379".to_owned()
                ],
                None,
            ),
            Err(EtcdConfigError::DuplicateEndpoint { index: 1 })
        );
    }

    #[test]
    fn tls_and_timeout_material_is_complete() {
        assert_eq!(
            EtcdTlsConfig::new(None, None, None, None, EtcdTlsPolicy::default()),
            Err(EtcdConfigError::EmptyCaCertificate)
        );
        assert_eq!(
            EtcdTlsConfig::new(
                Some(vec![1]),
                Some(vec![2]),
                None,
                None,
                EtcdTlsPolicy::default()
            ),
            Err(EtcdConfigError::IncompleteClientIdentity)
        );
        let config =
            EtcdClientConfig::new(["localhost:2379".to_owned()], None).and_then(|config| {
                config.with_timeouts(
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                    Duration::from_secs(1),
                )
            });
        assert_eq!(
            config,
            Err(EtcdConfigError::KeepAliveTimeoutExceedsInterval)
        );
    }

    #[test]
    fn minimum_tls_version_follows_go_suffix_rules() {
        assert_eq!(EtcdTlsVersion::parse(""), Ok(None));
        assert_eq!(EtcdTlsVersion::parse("1.2"), Ok(Some(EtcdTlsVersion::V1_2)));
        assert_eq!(EtcdTlsVersion::parse("1.3"), Ok(Some(EtcdTlsVersion::V1_3)));
        assert_eq!(
            EtcdTlsVersion::parse("1.0"),
            Ok(Some(EtcdTlsVersion::V1_2)),
            "TLS 1.0 is accepted and floored at 1.2"
        );
        assert_eq!(
            EtcdTlsVersion::parse("1.1"),
            Ok(Some(EtcdTlsVersion::V1_2)),
            "TLS 1.1 is accepted and floored at 1.2"
        );
        assert_eq!(
            EtcdTlsVersion::parse("tls1.3"),
            Ok(Some(EtcdTlsVersion::V1_3)),
            "Go matches the 1.3 suffix"
        );
        assert_eq!(
            EtcdTlsVersion::parse("nope"),
            Ok(Some(EtcdTlsVersion::V1_2)),
            "an unrecognized value falls back to TLS 1.2"
        );
    }

    #[test]
    fn skip_ca_verification_allows_a_tls_config_without_a_ca() {
        // skip-CA still enables TLS but does not require a trust anchor.
        let policy = EtcdTlsPolicy {
            skip_ca_verification: true,
            ..EtcdTlsPolicy::default()
        };
        assert!(EtcdTlsConfig::new(None, None, None, None, policy.clone()).is_ok());
        // An empty CA is treated as absent, which is fine when skipping.
        assert!(EtcdTlsConfig::new(Some(Vec::new()), None, None, None, policy).is_ok());
    }

    #[test]
    fn a_ca_is_required_when_not_skipping_verification() {
        // Default policy verifies the CA, so a missing or empty CA is rejected.
        assert_eq!(
            EtcdTlsConfig::new(None, None, None, None, EtcdTlsPolicy::default()),
            Err(EtcdConfigError::EmptyCaCertificate)
        );
        assert_eq!(
            EtcdTlsConfig::new(Some(Vec::new()), None, None, None, EtcdTlsPolicy::default()),
            Err(EtcdConfigError::EmptyCaCertificate)
        );
    }

    #[test]
    fn an_empty_allowed_common_name_is_rejected() {
        let policy = EtcdTlsPolicy {
            allowed_common_names: vec!["good".to_owned(), "  ".to_owned()],
            ..EtcdTlsPolicy::default()
        };
        assert_eq!(
            EtcdTlsConfig::new(Some(vec![1]), None, None, None, policy),
            Err(EtcdConfigError::EmptyCommonName)
        );
    }

    #[test]
    fn allowed_common_names_are_bounded() {
        let over_count = EtcdTlsPolicy {
            allowed_common_names: vec!["cn".to_owned(); 1025],
            ..EtcdTlsPolicy::default()
        };
        assert_eq!(
            EtcdTlsConfig::new(Some(vec![1]), None, None, None, over_count),
            Err(EtcdConfigError::TooManyCommonNames)
        );
        let over_length = EtcdTlsPolicy {
            allowed_common_names: vec!["a".repeat(256)],
            ..EtcdTlsPolicy::default()
        };
        assert_eq!(
            EtcdTlsConfig::new(Some(vec![1]), None, None, None, over_length),
            Err(EtcdConfigError::CommonNameTooLong)
        );
    }

    #[test]
    fn allowed_common_names_are_trimmed_sorted_and_deduped() {
        assert_eq!(
            super::normalized_common_names(vec![
                " beta ".to_owned(),
                "alpha".to_owned(),
                "alpha".to_owned(),
            ]),
            Ok(vec!["alpha".to_owned(), "beta".to_owned()])
        );
    }

    #[test]
    fn a_full_policy_with_ca_and_pins_is_accepted() {
        let policy = EtcdTlsPolicy {
            minimum_version: Some(EtcdTlsVersion::V1_3),
            allowed_common_names: vec!["etcd-server".to_owned()],
            skip_ca_verification: false,
        };
        assert!(EtcdTlsConfig::new(Some(vec![1, 2, 3]), None, None, None, policy).is_ok());
    }

    #[test]
    fn tls_debug_redacts_certificate_and_private_key_material() {
        let tls = EtcdTlsConfig::new(
            Some(b"secret-ca-certificate".to_vec()),
            Some(b"secret-client-certificate".to_vec()),
            Some(b"secret-private-key".to_vec()),
            Some("etcd.internal".to_owned()),
            EtcdTlsPolicy::default(),
        )
        .unwrap_or_else(|error| unreachable!("TLS policy: {error}"));
        let rendered = format!("{tls:?}");

        assert!(rendered.contains("client_identity_configured: true"));
        assert!(rendered.contains("etcd.internal"));
        for secret in [
            "secret-ca-certificate",
            "secret-client-certificate",
            "secret-private-key",
        ] {
            assert!(!rendered.contains(secret), "Debug leaked {secret}");
        }
    }

    #[test]
    fn endpoint_credentials_are_rejected() {
        assert!(matches!(
            EtcdClientConfig::new(["http://user:password@localhost:2379".to_owned()], None),
            Err(EtcdConfigError::InvalidEndpoint {
                reason: "credentials are not allowed",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn stale_owner_cannot_start_a_connection() {
        let registry = OwnershipRegistry::new();
        let lease = registry
            .claim(OwnerScope::Process, "owner-A")
            .unwrap_or_else(|error| unreachable!("owner claim: {error}"));
        let connector = super::EtcdConnector::new(
            lease.token(),
            EtcdClientConfig::new(["127.0.0.1:1".to_owned()], None)
                .unwrap_or_else(|error| unreachable!("config: {error}")),
        );
        drop(lease);
        assert!(matches!(
            connector.connect().await,
            Err(super::EtcdConnectError::StaleOwner)
        ));
    }

    #[test]
    fn ns_servers_are_bounded_validated_and_order_preserving() {
        use std::sync::Arc;

        let base = EtcdClientConfig::new(["127.0.0.1:2379".to_owned()], None)
            .unwrap_or_else(|error| unreachable!("config: {error}"));
        assert!(base.ns_servers().is_empty());

        // Frozen lexicographic order with duplicates preserved (not de-duped).
        let servers: Arc<[Arc<str>]> = Arc::from([
            Arc::from("10.0.0.1:53"),
            Arc::from("10.0.0.1:53"),
            Arc::from("[2001:db8::1]:53"),
            Arc::from("dns.internal:5353"),
        ]);
        let threaded = base
            .clone()
            .with_ns_servers(Arc::clone(&servers))
            .unwrap_or_else(|error| unreachable!("ns: {error}"));
        assert_eq!(threaded.ns_servers(), &*servers);

        // Over-bound list is rejected.
        let too_many: Arc<[Arc<str>]> = (0..=super::MAX_NS_SERVERS)
            .map(|index| Arc::from(format!("10.0.0.{}:53", index % 250)))
            .collect();
        assert!(matches!(
            base.clone().with_ns_servers(too_many),
            Err(EtcdConfigError::TooManyNsServers { .. })
        ));

        // Non-normalized entries are rejected, matching the config layer's
        // `normalize_ns_server`: a missing port, a zero port, and a bracketed
        // host that is not an IP literal.
        for bad in ["no-port-host", "dns.example:0", "[dns.example]:53"] {
            let entry: Arc<[Arc<str>]> = Arc::from([Arc::from(bad)]);
            assert!(
                matches!(
                    base.clone().with_ns_servers(entry),
                    Err(EtcdConfigError::InvalidNsServer { index: 0 })
                ),
                "must reject {bad}"
            );
        }
    }
}

#[cfg(test)]
mod gate_commit_tests {
    use super::GenerationGate;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    /// Revocation first: the commit is refused, and nothing it would have
    /// done happens.
    #[test]
    fn a_revocation_ordered_first_refuses_the_commit() {
        let gate = GenerationGate::new();
        gate.revoke();
        let ran = gate.try_commit(|| 1);
        assert_eq!(ran, None, "a revoked gate commits nothing");
    }

    /// Commit first: the revocation waits for it to finish, and the effect
    /// stands. This is the half that must *not* be undone -- the write was
    /// made under a live generation.
    ///
    /// The barrier makes the order real rather than hoped for: the revoking
    /// thread is released only once the commit is known to be inside the
    /// lock, so a gate without one would let the flip land mid-commit.
    #[test]
    fn a_commit_ordered_first_completes_before_the_revocation() {
        let gate = GenerationGate::new();
        let inside = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = mpsc::channel();

        let revoker = {
            let gate = gate.clone();
            let finished = Arc::clone(&finished);
            std::thread::spawn(move || {
                // Only start revoking once the commit is demonstrably inside.
                entered_rx
                    .recv()
                    .unwrap_or_else(|error| unreachable!("commit never entered: {error}"));
                gate.revoke();
                // The commit must have completed before the flip landed.
                assert!(
                    finished.load(Ordering::SeqCst),
                    "revocation overtook a commit that had already begun"
                );
            })
        };

        let committed = gate.try_commit(|| {
            inside.store(true, Ordering::SeqCst);
            let _ = entered_tx.send(());
            // Give the revoker every chance to interleave.
            std::thread::sleep(std::time::Duration::from_millis(50));
            finished.store(true, Ordering::SeqCst);
            7
        });

        revoker
            .join()
            .unwrap_or_else(|_| unreachable!("revoking thread panicked"));
        assert_eq!(committed, Some(7), "a live gate runs its commit");
        assert!(inside.load(Ordering::SeqCst));
        assert!(!gate.is_live(), "the revocation still lands, just after");
    }

    /// Ordinary liveness reads are unchanged; the lock is only on the
    /// commit path.
    #[test]
    fn is_live_still_answers_without_the_commit_lock() {
        let gate = GenerationGate::new();
        assert!(gate.is_live());
        let seen = gate.try_commit(|| gate.is_live());
        assert_eq!(seen, Some(true), "is_live is readable from inside a commit");
        gate.revoke();
        assert!(!gate.is_live());
    }
}
