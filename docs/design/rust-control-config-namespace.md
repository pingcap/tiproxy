# Rust control configuration and namespace ownership

Status: design freeze for CP-CFG/NS ([#146](https://github.com/bb7133/tiproxy/issues/146)).

This document defines the Rust-native configuration and namespace boundary used
by later control-plane modules. The target is one Rust TiProxy process. The
legacy Go/Rust protocol is a migration seam, not an internal Rust domain model.

## Ownership boundary

`control-config` owns:

- parsing, defaults, validation, canonical encoding, and checksums for the full
  TiProxy configuration;
- classification of dynamic and restart-required fields;
- the local-file reload loop and its last-good state;
- persistent dynamic configuration and namespace keys below `/config`;
- the only immutable config/namespace snapshot published in the Rust process;
- certificate-material validation and atomic TLS generation changes.

`control-topology` owns:

- TiProxy self-registration below `/topology/tiproxy`;
- TiDB discovery below `/topology/tidb` and `/keyspaces/tidb`;
- Prometheus discovery and backend health;
- immutable topology snapshots, including the keyspace derived from topology
  paths.

`control-topology` consumes the backend-cluster projection from
`control-config`. It must not read, watch, or write `/config`. `control-config`
must not read, watch, or write topology paths. A namespace does not contain a
keyspace field in the current Go model; CP-CFG must not invent one. Keyspace is
discovered by CP-TOPO and bound immutably to a routed session by CP-ROUTE.

## Process-local consumer API

The domain types and source trait live in the `control-config` crate. This
keeps `control-plane` independent of feature modules and avoids a dependency
cycle. The binary composition root passes a cloned source handle directly to
CP-TOPO and later CP-ROUTE constructors.

```rust
pub trait ConfigNamespaceSource: Send + Sync {
    fn current(&self) -> Arc<ConfigNamespaceSnapshot>;
    fn subscribe(&self) -> watch::Receiver<Arc<ConfigNamespaceSnapshot>>;
}

pub struct ConfigNamespaceSnapshot {
    pub generation: u64,
    pub source_revision: SourceRevision,
    pub config_checksum: u32,
    pub namespace_checksum: u32,
    pub effective: Arc<EffectiveConfig>,
    pub namespaces: Arc<[NamespaceConfig]>,
}

pub struct SourceRevision {
    pub file_revision: u64,
    pub etcd_revision: i64,
}

pub struct BackendClusterConfig {
    pub name: Arc<str>,
    pub pd_addrs: Arc<[Arc<str>]>,
    pub ns_servers: Arc<[Arc<str>]>,
}

pub struct TopologyConfig {
    pub advertise_host: Arc<str>,
    pub sql_port: u16,
    pub status_port: u16,
    pub backend_clusters: Arc<[BackendClusterConfig]>,
    pub cluster_tls: ClientTlsConfig,
    pub health: HealthCheckConfig,
}

pub struct TopologyRuntimeIdentity {
    pub version: Arc<str>,
    pub git_hash: Arc<str>,
    pub deploy_path: PathBuf,
    pub start_timestamp: i64,
}
```

The actual types may use private fields plus accessors, but the information and
semantics above are stable:

- `generation` starts at one and is the sole consumer ordering key. It advances
  by exactly one only when a fully validated effective snapshot changes.
- Snapshot `file_revision` and `etcd_revision` identify only the sources
  incorporated into that accepted immutable view. The store separately tracks
  the highest observed file/etcd cursor, including rejected and no-op
  candidates, for recovery diagnostics. Neither is a consumer ordering key.
  Etcd revisions may skip and therefore must never be fed to CP-001's
  immediate-successor `ConfigStore`.
- A rejected file or etcd candidate records a bounded rejection event but does
  not publish, mutate the current view, or advance `generation`.
- `current` plus `tokio::sync::watch` gives consumers a race-free pull/watch
  pattern. `Arc` makes an accepted snapshot immutable for existing sessions.
- `namespaces` and backend clusters are sorted by name. Address and common-name
  lists are normalized, sorted where order is not semantic, and de-duplicated.
- `pd_addrs` is the trimmed, non-empty expansion of Go's comma-separated
  `PDAddrs`. `ns_servers` is host:port-normalized, using port 53 when omitted.
- When `proxy.backend-clusters` is empty, non-empty legacy `proxy.pd-addrs`
  yields one cluster named `default`, matching `Config.GetBackendClusters`.

CP-TOPO consumes `TopologyConfig`, which is a stable projection of the
effective configuration. `cluster_tls` contains the complete CA and optional
client certificate/private-key paths needed to construct an mTLS etcd client,
not only minimum-version or common-name policy. `health` initially carries the
current Go defaults (enable, interval, retry count/interval, dial timeout, and
metrics interval/timeout); those values become ordinary validated fields if
Go exposes them as user configuration later. `advertise_host`, `sql_port`, and
`status_port` are the resolved equivalent of Go `Config.GetIPPort`.

CP-CFG's own PD/etcd transport is intentionally separate and restart-pinned:
its endpoint set uses legacy `proxy.pd-addrs`, matching Go's `InitEtcdClient`.
An empty `proxy.pd-addrs` disables persistent config. The dynamic
`proxy.backend-clusters` projection may change for CP-TOPO/CP-ROUTE, but it
never silently redirects the running config election/watch session. Accepted
`security.cluster-tls` path or content changes rebuild the PD client, stop the
old reader/election, and campaign with the new transport; owner-fenced writes
remain unavailable throughout the handoff. Transport construction participates
in prospective candidate validation, so an unsupported replacement retains the
last-good source generation instead of publishing and then failing. In
particular, `cluster-tls.skip-ca = true` still opens a TLS connection and
does not require a CA. It does not downgrade the owner connection to
plaintext.

Binary/build/process facts are not configuration: the composition root passes
`TopologyRuntimeIdentity` directly to the CP-TOPO constructor. CP-TOPO combines
it with `TopologyConfig` to write its self-registration record. Namespace
frontend/backend TLS and static backend instances remain in the same immutable
CP-CFG snapshot for CP-ROUTE. Routing/balance policy is retained by
`EffectiveConfig` but is not a CP-TOPO input.

## CP-001 module executor and composition

CP-001 remains a feature-agnostic runtime foundation. It gains a small explicit
executor owned by the binary composition root:

```rust
let mut modules = ControlModuleSet::new(in_process.handle());
modules.spawn(config_module)?;
modules.spawn(topology_module)?;
```

`ControlModuleSet` has these semantics:

- `spawn` rejects duplicate stable module names and starts
  `ControlModule::run` with `RuntimeHandle::module_context()`;
- `join_next` returns the module name and bounded `ModuleError`; a panic is
  converted to the stable `module_panicked` class;
- readiness is marked only after every required module reports its initial
  last-good snapshot through its constructor-specific ready handle;
- signal or first module failure moves the shared lifecycle to shutdown; every
  module observes the lifecycle watch and returns;
- the composition root joins every module before `ControlRuntime::finish`;
  abort is only the bounded final backstop. The production binary gives the
  complete module set ten seconds to join, then aborts and still awaits every
  join handle. A stuck source therefore cannot leave the process hung or a
  module task detached.

Feature-specific sources are constructor dependencies, not fields added to
`ModuleContext`. Startup order is CP-CFG initial snapshot, CP-TOPO initial
snapshot, later CP-ROUTE, then SQL listener readiness. This establishes one
generation authority per domain without a protobuf or a global generation
shared across unrelated sources.

`ConfigNamespaceSource` never has a pre-initial empty view. Its constructor
synchronously parses and validates defaults plus the configured file/CLI
overrides and installs generation 1 before returning the source handle. The
etcd overlay loop may then advance later generations. Therefore CP-TOPO's first
`current()` is always a real generation-1-or-newer snapshot; module readiness
does not race initialization.

The first CP-CFG implementation adds this executor to `control-plane`. CP-TOPO
may build its module core in parallel and then use the same executor after
rebasing; it must not add a second runner.

## Static and dynamic configuration

The Rust model covers every field in Go `lib/config.Config`; no field is
silently dropped. The implementation maintains two layers:

1. `file_base`: defaults plus the last accepted partial TOML update and command
   line overrides.
2. `persistent_overlay`: legacy dynamic values from etcd.

A local reload decodes into a clone of the previous accepted `file_base`, so
omitted fields retain their previous values and explicitly supplied zero/empty
values overwrite them, matching `SetTOMLConfig`. Command-line advertise address
always wins. File removal/read failure and invalid candidates retain last-good.
Polling remains two seconds for Go parity.

After startup, changes to restart-required fields fail closed as one atomic
candidate. Dynamic fields are recomputed with the persistent overlay, fully
validated, canonically encoded, and published only when bytes change. The
field-level implementation table below is exhaustive for the current Go
`lib/config.Config` shape. `Dynamic` means CP-CFG accepts and publishes a new
immutable snapshot without restarting this process; it does not imply that the
downstream module named in the final column has already landed. `Restart`
means a post-generation-1 change rejects the whole candidate. `Unsupported`
means the source value still round-trips canonically, but a candidate that
would activate the value is rejected by the serving validator.

| Go TOML field(s) | Class | #146 behavior / next consumer |
| --- | --- | --- |
| `workdir` | Restart | Modeled and checksummed; process/filesystem ownership is fixed at startup. |
| `proxy.addr`, `proxy.advertise-addr`, `proxy.pd-addrs`, `proxy.port-range` | Restart | Modeled; SQL bind and topology identity are fixed at startup. |
| `api.addr` | Restart | The native process binds it. `--admin-addr` overrides only that bind. |
| `log.encoder`, `log.simple` | Restart | Modeled; encoder construction is fixed at startup. |
| `api.proxy-protocol` | Restart, `v2` unsupported | Retained for the checksum. `v2` is rejected because the native admin listener does not apply PROXY protocol. Any other value is inert. |
| `balance.routing-rule` | Dynamic | Accepted for routers created after the reload. An existing router keeps the rule it was built with, matching the Go process. |
| `ha.virtual-ip` | Restart, applied | Excluded when the process selects an advertise address. |
| `ha.interface`, `ha.garp-burst-count`, `ha.garp-refresh-count` | Restart, inert | Retained. VIP ownership and GARP are not implemented. |
| `ha.virtual-ip` with `ha.interface` | Restart, unsupported | Go binds the VIP only when both are set. That activation is rejected. |
| `metering.type`, `region`, `bucket`, `prefix`, `endpoint`, `shared-pool-id`; every field below `metering.aws`, `.oss`, `.cos`, `.azure`, `.localfs` | Restart | Modeled with secret-safe debug output; CP-METER consumes them at startup. |
| `rust-dataplane.enabled`, `.control-socket`, `.routing-shadow-socket`, `.allowed-uid`, `.tls-allowed-roots`, `.metrics-owner-port` | Restart | Modeled; process/transport, TLS trust roots, observer socket, and metrics-owner bind cannot change online. A nonzero metrics-owner port must differ from every SQL/API listener port. |
| `proxy.max-connections`, `proxy.high-memory-usage-reject-threshold`, `proxy.conn-buffer-size` | Dynamic | Applied by the Rust SQL serving snapshot. |
| all five fields `enabled`, `idle`, `cnt`, `intvl`, `timeout` below each of `proxy.frontend-keepalive`, `.backend-healthy-keepalive`, `.backend-unhealthy-keepalive` | Dynamic | Applied by the Rust SQL serving snapshot. |
| `proxy.proxy-protocol`, `.graceful-wait-before-shutdown`, `.graceful-close-conn-timeout`, `.public-endpoints` | Dynamic | Applied by the Rust SQL serving snapshot, including the latest graceful-close value at process drain. |
| every `name`, `pd-addrs`, `ns-servers` below `proxy.backend-clusters` | Dynamic | Projected in stable order for CP-TOPO. |
| `proxy.fail-backend-list`, `proxy.failover-timeout` | Dynamic | Retained for CP-ROUTE; no effect is claimed before #147. |
| `security.encryption-key-path` | Dynamic, unsupported when non-empty | Retained when empty. A non-empty path is rejected because traffic capture is not implemented. |
| `security.require-backend-tls` | Dynamic | Applied by the Rust SQL serving snapshot. |
| `cert`, `key`, `ca`, `min-tls-version`, `cert-allowed-cn`, `skip-ca` below each of `security.server-tls`, `.server-http-tls`, `.cluster-tls`, `.sql-tls` | Dynamic | `Config.Check` accepts every minimum version, a relative path, and a certificate without its key. The effective minimum version follows Go `GetMinTLSVer`; the native handshake floor is TLS 1.2, and only a `1.3` suffix selects TLS 1.3 alone. A relative path is resolved against the process working directory. A configured `--tls-root` or `tls-allowed-roots` still confines the read; an empty allowlist does not add a jail. A certificate and key are still required together when a server identity is built. `cluster-tls.skip-ca = true` keeps the etcd connection on TLS and does not require a CA. |
| `auto-certs` below global `server-tls` / `server-http-tls` and namespace `frontend.security` | Unsupported | Canonically round-tripped, then rejected explicitly because #146 does not generate server certificates. |
| `auto-certs` below global `cluster-tls` / `sql-tls` and namespace `backend.security` | Dynamic, inert | Canonically round-tripped; the legacy client-side shape has no serving meaning. |
| `rsa-key-size`, `autocert-expire-duration` below every global TLS block | Dynamic, inert unless auto-certs | Canonically round-tripped; because auto-certs is unsupported, these fields do not generate material in #146. |
| `log.level`, every `filename`, `max-size`, `max-days`, `max-backups` below `log.log-file` | Dynamic | Level is applied by CP-001; file sink changes are retained for CP-ADMIN/log ownership. |
| `balance.label-name`, `.policy`, `.routing-policy`; `migrations-per-second` below `.status`, `.health`, `.memory`, `.cpu`, `.location`, `.conn-count`; `.conn-count.count-ratio-threshold` | Dynamic | Validated and retained for CP-ROUTE. |
| every `labels.<key>` | Dynamic | Stable map projection for CP-TOPO/CP-ROUTE. |
| `enable-traffic-replay` | Dynamic, inert | The Go default `true` is accepted and SQL serving starts. Capture and replay are not implemented: the traffic API answers `unsupported` when the flag is true and `disabled` when it is false. `rust-dataplane.enabled` together with this flag still fails `Config.Check`. |

Namespace values have a separate, equally exact classification:

| `/config/ns/<name>` field(s) | Class | Ownership |
| --- | --- | --- |
| `namespace`, `frontend.user` | Dynamic | CP-CFG persists identity and publishes immutable generations; CP-ROUTE owns listener/user binding and conflicts. |
| every TLS field below `frontend.security` and `backend.security` | Dynamic, with auto-certs unsupported as above | CP-CFG validates and content-watches material; CP-ROUTE consumes it for new sessions. |
| every entry in `backend.instances` | Dynamic | CP-CFG persists and orders it; CP-ROUTE owns backend binding. |

There is deliberately no namespace `keyspace` field. CP-TOPO discovers
keyspaces and CP-ROUTE #147 binds them to sessions; #146 neither invents nor
infers that association.

The config checksum is CRC32-IEEE over canonical TOML of the effective full
config, matching Go. The namespace checksum is CRC32-IEEE over canonical JSON
of the name-sorted namespace array. Checksums are evidence, not ordering keys.

## Persistent key compatibility and fencing

The exact historical keys and JSON shapes remain readable and writable:

- `/config/proxy`: `ProxyServerOnline`
- `/config/log`: `LogOnline`
- `/config/ns/<namespace>`: `Namespace`

Values are persistent and must not be attached to a lease. On bootstrap a
linearizable prefix read at revision `R` creates one validated overlay. Missing
`proxy` or `log` keys are initialized from `file_base` only by the current
CP-ETCD election owner. The write is one etcd transaction that compares the
exact election ownership token and the target key's non-existence, then puts
without a lease. Namespace key mutations use the same owner-fenced persistent
transaction. `ElectionSession::fenced_put` is not reused because it attaches
the election lease and is intentionally ephemeral.

A successful process-local mutation is optimistically applied to the source
at the transaction response's exact etcd revision before its caller is
acknowledged. That makes back-to-back mutations validate against the first
committed value even if the watch delivery is still queued. The later full
watch candidate is idempotent; any candidate older than the already-applied
etcd revision is ignored and cannot roll the optimistic view back.

CP-CFG extends `control-etcd` with a bounded owner-fenced persistent transaction
primitive. It exposes neither a raw unaudited client nor a generic arbitrary
transaction API. Losing election ownership before commit fails closed. Readers
need only the process `OwnerToken`; writes additionally require the current
election fence.

After the bootstrap read, a prefix watch begins at `R + 1`. All events from one
watch response are applied as one candidate, sorted by key. A disconnect or
compaction performs a fresh linearizable relist and resumes after its header
revision. Invalid JSON, a namespace key/value name mismatch, a missing name,
or an invalid effective config rejects the complete candidate and preserves
last-good. The observed etcd revision still advances for watch recovery, so a
later corrective revision can be accepted without replay loops.

A malformed persistent namespace is logged as one bounded, payload-free JSON
record with exactly the stable component/event, source revision, offending
namespace key, and error class. Namespace values and their raw decode errors
are never logged. This keeps the operator-visible rejection attributable
without exposing credentials or customer configuration embedded in a value.

Namespace absence follows the accepted option-A compatibility rule. A
proxy/log-only watch or relist containing no namespace keys carries no
namespace mutation: the current name-sorted namespace set, object identities,
and incarnation checksums are preserved. Only an explicit namespace `PUT` or a
`DELETE` event observed by the live watch changes that set; deleting the last
namespace is valid and the one-shot startup seed must not resurrect it.

The implicit startup `default` namespace is a one-shot process-composition
seed, not a persisted `/config/ns/default` value. Consequently the first
explicit namespace mutation makes the concrete `/config/ns/*` key set
authoritative: if that set contains `ns-alpha` but no `default`, the seed is
replaced and clients which need the ordinary default fallback receive the exact
namespace-missing 1105/HY000 response. Operationally, before writing the first
explicit namespace, an installation that intends to retain `default` **must
first persist `/config/ns/default` in the same authoritative set**. The T4
runner does this explicitly; the paired regression proves both outcomes
(unmaterialized default disappears, materialized default coexists and retains
its incarnation).

This also defines the recovery limitation precisely: if a namespace key is
deleted while the watcher is disconnected and compaction forces a relist, bare
absence is not distinguishable from a config-only snapshot. The old namespace
therefore remains retained after that relist until the explicit delete is
reissued (or a future persisted tombstone/admin delete protocol supplies that
intent). Operators must not treat physical key absence during an outage as an
acknowledged namespace delete. This behavior avoids the more dangerous cliff
where an unrelated first proxy update empties all namespaces.

## TLS rotation

A relative TLS path is resolved against the process working directory, the
same way Go `os.ReadFile` resolves it. When `--tls-root` or
`tls-allowed-roots` is non-empty, the canonical path must stay under one of
those roots. An empty allowlist has no extra jail. Minimum versions follow
Go `GetMinTLSVer`, with a native floor of TLS 1.2. A certificate without its
key is accepted by `Config.Check` and rejected when a server identity is
built, so it cannot become a plaintext listener. Allowed common names are
bounded and canonicalized. Candidate validation loads and parses every
complete cert/key/CA set before publishing a source generation; the serving
adapter then constructs and atomically swaps one complete immutable connection
factory. Unreadable, invalid, or expired material is rejected without
publishing a partial set. A generation advances for an accepted
config/namespace value change **or for a byte-level change to any referenced
TLS material file**, even when every source revision and canonical config
checksum is unchanged. Existing sessions keep their prior `Arc`; only new
connections observe the new certificate generation.

## Migration and bridge accounting

CP-CFG first installs the Rust source and uses its accepted snapshot as the
authoritative config/namespace input. With
`CONTROL_CAPABILITY_RUST_ROUTE_OWNER`, CP-TOPO is authoritative too: Go shrinks
`StateSnapshot.config` to exactly `advertised_capability` and `server_version`
and sends empty `StateSnapshot.namespaces` **and**
`StateSnapshot.backends`. Rust rejects a nonempty route field instead of
silently composing two owners. It consumes only those two protocol/static
config facts and replaces all former Go routing inputs:

- `max_connections`, `high_memory_reject_threshold`,
  `connection_buffer_bytes`, all three keepalive messages, `proxy_protocol`,
  `require_backend_tls`, both graceful durations, `listeners`, `public_cidrs`,
  `frontend_tls`, `backend_tls`, and `traffic_replay_enabled`;
- every `NamespaceSnapshot` field (`name`, `users`, `backend_cluster`);
- every `BackendSnapshot` field (identity, address, cluster, keyspace, health,
  locality, draining, CIDRs, and labels).

Capabilities 5 and 6 are required by the bundled production pair, so an older
peer fails handshake instead of accepting an incomplete snapshot. The cap5
shape (Rust config/namespace with bridge backends) remains a compatibility-test
stage only; cap6 is the production ownership fence.

The legacy Go HTTP config/namespace endpoints remain part of CP-ADMIN #150,
not a second CP-CFG generation authority. Until that slice migrates them, their
process-local writes continue to feed only still-Go-owned managers; they cannot
overwrite the Rust source or SQL-serving generation. The Rust
`ConfigModuleHandle` is the sole owner-fenced persistent mutation surface and
is intentionally process-local until CP-ADMIN binds the external API to it.

An in-process composer combines CP-CFG, CP-TOPO routing, and the matching health
overlay with the residual bridge facts. A wake from config, routing, or health
re-pulls the complete current inputs, advances one process-local composition
generation, validates the complete candidate, and atomically swaps the serving
view. A failed candidate retains last-good.

The `state_snapshot`/`snapshot_result` message pair cannot be deleted while its
backend or protocol/static fields remain. The catalog therefore records this
field-level residual surface rather than claiming whole-message retirement.

## Required evidence

- Go/Rust differential fixtures for defaults, partial TOML, canonical checksum,
  every field classification, legacy PD fallback, namespaces, and legacy JSON
  keys.
- Invalid reload: last-good checksum and effective generation remain unchanged.
- Rejected revision `N` followed by valid `N+1`: `N` never applies and `N+1`
  publishes exactly once, including disconnect/relist recovery.
- TLS rotation: partial material rejects; an existing session survives on its
  old generation; a new connection presents the new generation.
- Namespace change: existing session binding is immutable, new session uses the
  new namespace generation, and cross-keyspace redirect is rejected after
  CP-TOPO supplies authoritative keyspace.
- Real embedded-etcd restart/compaction and old-owner write attempts prove
  watch recovery and owner-fenced persistent writes.
- Mutations must kill: skipped field projection, generation skip, invalid
  candidate overwrite, lease-attached persistent key, and old-owner write.
