# Rust management plane (CP-ADMIN #150)

`control-admin` reproduces the Go `pkg/server/api` surface inside the Rust
process so the operator-facing HTTP API, the diagnostics gRPC service and
`tiproxyctl` compatibility no longer depend on the Go helper. Delivery is
sliced; each slice lands with Go/Rust differential evidence, and this document
records the exact residual surface after every slice.

## Slice 1: listener, middleware, health, metrics, status

The Rust executable binds its management listener before it marks itself
ready and serves the table below on it. Since slice 5c (`RUST_API_OWNER`)
that listener is the configured `api.addr`: Go starts no API server beside
the Rust dataplane. `--admin-addr <host:port>` overrides the address for
harnesses that run a Go oracle on `api.addr` next to the Rust process
(slices 1–5b were developed that way). The address must be distinct from
`rust-dataplane.metrics-owner-port` and from every SQL port.

Middleware runs in the Go order: a global blocking rate limit (uber-go
`ratelimit` semantics, 100/s with ten slots of slack, never `429`), the
readiness gate (`500 "service not ready"` as JSON until the process is ready),
the handler, and an access log that records only method, path, status and
latency for non-2xx responses (Go logs successes at debug level, which the
Rust process does not emit). Unknown paths and unregistered methods answer
gin's `404 page not found` with `text/plain`; that includes `HEAD` on every
route, which gin never registers. Request bodies are bounded at
1 MiB; Go reads them unbounded.

TLS follows `security.server-http-tls` from the current accepted configuration
generation, read per connection so a rotated certificate applies without
rebinding. With TLS configured the first byte is sniffed like Go's `cmux`: a
TLS record reaches the full table, anything else reaches only `GET
/api/debug/health` and `GET /debug/health`. Without TLS every connection reaches
the full table over HTTP/1.1 or cleartext HTTP/2 (Go `UseH2C`). The
HTTP/1 request-head read timeout is 30 s (Go `DefConnTimeout`), and the same
30 s applies as an activity-based idle timeout to HTTP/1 and HTTP/2
connections (Go's `IdleTimeout`; hyper has no HTTP/2 equivalent, so the
listener stamps every successful read or write and closes a connection that
stays silent that long); connections are
bounded to 1024 and every connection task is tracked in a `JoinSet`, so
shutdown stops accepting, lets in-flight requests finish within 5 s, then
aborts and joins every remaining task, including peers still being sniffed,
handshaking or sending a slow body. The executable supervises the server task
like its other owners: if the listener fails, the process fails closed and
drains instead of continuing without an operator surface (Go only logs the
`Serve` error).

| Endpoint | Behaviour |
| --- | --- |
| `GET /api/debug/health`, `GET /debug/health` | `{"config_checksum":N}` with `unhealthy_reason` only when set; `502` in the Go order: manual override, closing, namespace owner not ready, no applied dataplane generation. `config_checksum` is the CRC32 of the Go-encoded TOML and is numerically identical to Go's. |
| `PUT /api/debug/health` | `{"healthy":bool,"reason":string}` decoded with `json.Decoder.Decode` semantics as gin's `ShouldBindJSON` applies them: one JSON value is read and trailing bytes are ignored, `null` is the zero value, keys match exactly or case-insensitively, unknown keys are ignored, every occurrence of a key is decoded so a wrong type anywhere is `400 "bad health override json"` and the last well-typed value wins; reason trimmed; `200 ""`. Strings in every JSON answer use Go's HTML-safe escaping (`\u003c`, `\u003e`, `\u0026`, `\u2028`, `\u2029`). |
| `DELETE /api/debug/health` | clears the override, `200 ""`. |
| `GET /metrics`, `/metrics/`, `/api/metrics`, `/api/metrics/` | the B0 native exposition, `text/plain; version=0.0.4; charset=utf-8; escaping=underscores`. |
| `GET /api/dataplane/status` | the Go key set in gin's sorted-map order; `desired_generation` and `sent_generation` are the latest composed generation (there is no transport hop in-process), `applied_generation`/`rejected_generation` come from SQL serving, `last_result_code` is `ERROR_CODE_INVALID_SNAPSHOT` when the latest rejection is newer than the latest apply, `ERROR_CODE_OK` after the first apply, `ERROR_CODE_UNSPECIFIED` before it. |
| `POST /api/traffic/{capture,replay,cancel}`, `GET /api/traffic/show` | `400 text/plain`. The body is `traffic <verb> is unsupported` when `enable-traffic-replay` is true, and Go's `traffic <verb> is disabled` when it is false. The default is true and SQL serving still starts. |

Health inputs: `closing` is the runtime lifecycle leaving `Ready`; the
namespace-owner readiness is the config module's initial persistent view;
`applied_generation` is the SQL serving generation. Go zeroes the applied
generation while the metering consumer is unhealthy; that coupling moves to the
native metering owner (#148) when it takes ownership.

## Slice 2: namespaces and configuration

`/api/admin/namespace/*` and `/api/admin/config/` keep the Go status codes and
bodies (`namespace.go`, `config.go`) over the owner-fenced config module:

| Endpoint | Behaviour |
| --- | --- |
| `GET /api/admin/namespace/` | name-sorted JSON array of namespaces; the JSON empty string `""` when there are none (Go's nil slice). |
| `GET /api/admin/namespace/{name}` | the namespace, or `500 "can not get namespace"` (Go reports not-found through the same 500). |
| `PUT /api/admin/namespace/{name}`, `PUT /api/admin/namespace/` | body decoded with `json.Decoder` semantics against the Go `config.Namespace` field schema (one value, trailing bytes ignored, `null` = empty value, case-insensitive keys, unknown members ignored whatever their shape, `null` leaves a scalar or struct unchanged but clears a slice, duplicate struct members merge field by field, later scalars and arrays win, a known member of the wrong kind is `400 "bad namespace json"`); the path only pre-fills the name and the body's `namespace` wins and is the stored key, exactly as Go's `SetNamespace(nsc.Namespace)`; an empty name is `500 "can not update config"`; persisted below `/config/ns/<name>` through `ConfigModuleHandle::set_namespace`; `200 ""` or `500 "can not update config"`. |
| `DELETE /api/admin/namespace/{name}` | `ConfigModuleHandle::delete_namespace`; deleting an absent name succeeds like Go's B-tree; `200 ""` / `500`. |
| `POST /api/admin/namespace/commit?namespace=a&namespace=b` | query values decoded like `url.ParseQuery` (a pair with a malformed escape is dropped); every named namespace must exist (`500 "failed to get namespace"`); the call returns once the SQL serving side has applied the current CP-CFG generation, within 5 s (`500 "failed to reload namespaces"` otherwise). The barrier follows the config lineage that serving actually installed: the composer stamps every composition with the CP-CFG generation it was composed from, that value travels inside the validated view, and every successful serving apply (initial bind, bridge apply, recomposition) publishes the value carried by the view it installed. Nothing else publishes it: composing, staging, a skipped recomposition, or a rejected apply cannot confirm a generation, and there is no composer-side history that a later compose at an unchanged composition counter could overwrite. The composer's own counter is never compared with a config generation (it also advances on topology wakes). Namespaces reach serving through the config watch, so the commit is a barrier, not a second write. |
| `GET /api/admin/config/` | TOML (`application/toml; charset=utf-8`), or JSON with `?format=json` (case-insensitive, first value only like gin's `c.Query`) or an exact `Accept: application/json`. |
| `PUT /api/admin/config/` | Go `SetTOMLConfig` through the config owner (`ConfigModuleHandle::apply_local_toml`): the partial document is merged onto this process's file base, validated as a whole, and published only when the encoded bytes change, so the health checksum moves exactly as Go's does. The mutation is instance-scoped like the Go API (labels and other per-instance fields stay per instance) and works without etcd; the persistent `/config` overlay still applies on top. |

Namespaces therefore persist (etcd `/config/ns/*`, cluster-wide, owner-fenced)
where Go keeps them in a per-process B-tree lost on restart. That is the
intended upgrade of CP-CFG, not a compatibility slip, and it is why the
handlers never touch a second store.

## Slice 3a: operator drain from inside the process

`POST /api/dataplane/drain` and `GET /api/dataplane/drain/{id}` keep the Go
handler (`dataplane.go`): the body decodes with `json.Decoder` semantics
against the Go `drainRequestBody` schema, `drain_id` is required (gin's
validator message), the raw millisecond budget is validated before any
conversion (each value and their sum within the 30-day cap, never negative),
`202 {"drain_id":…}` on issuance, `400`/`503`/`409`/`500` with Go's error
strings for `ErrInvalidDrainBudget`, `ErrNoDataplaneSession`/
`ErrSnapshotNotReady`, `ErrDrainInProgress`/`ErrForeignDrainActive` and
anything else, `404 {"enabled":false}` without a drain seam, and the status
body in gin's sorted-key order with `404 {"known":false}` for an unknown
label.

Behind the seam the drain is issued **inside the dispatch owner**: a local
issuer equivalent to Go's `DrainIssuer` lives next to the `CommandGate`. One
boot nonce qualifies every wire id (`<label>@<incarnation>`); like Go's
`NewDrainIssuer`, a failed entropy read refuses the issuer and the owner
refuses to start rather than degrade to a guessable nonce that could alias
two incarnations. Each operator label binds once to a wire id and to `gate
watermark + 1`, so bridge and local drains share one monotonic sequence
lineage; the same admission core (deadline validation, scope matching,
single-flight, tombstone replay, obsolete → synthetic `DUPLICATE_REQUEST`)
serves both paths, with the only difference that a local drain has no wire
requester, so its terminal is not pushed to Go. The issuer's record keeps
the latest answer and the terminal for the incarnation's lifetime (Go
`operation.latest/completed`), observed from the admission answer and from
the `session_closed` completion, so a status query never depends on the
gate's bounded tombstone ring, and a repeated POST for a completed label
answers its original binding without re-admitting (a terminal is absolute;
an evicted tombstone's synthetic `DUPLICATE_REQUEST` can never relabel it).
A local drain is marked as local before its admission runs, so a terminal the
admission produces inline (its force phase closing a session whose control
receiver is already gone) is never pushed to the wire as a bridge result; the
CLOSED lifecycle event is still emitted and the admission answer carries that
terminal into the label's record. A running bridge drain (or a previous
incarnation's) is a foreign conflict for the local issuer; a running local
drain is an in-progress conflict for another label. Without an applied generation the drain is refused before any
effect (Go `ErrSnapshotNotReady`); the gate's applied generation still comes
from the Go snapshot notices in the two-process phase and moves to the Rust
lineage at cutover (#153).

Evidence: dispatcher tests cover the graceful → force lifecycle with status
reporting, idempotent replay, local/bridge lineage sharing (a bridge command
at a consumed sequence is obsolete; a running bridge drain is foreign), a
restarted incarnation re-issuing the same label under a new wire id, the
fail-closed issuer under an injected entropy failure, terminal retention
across tombstone eviction on both completion paths, an inline terminal of a
gone session staying local, and the notice plumbing; the differential compares the HTTP mapping against Go's
handler over scripted drainer outcomes. The T4 M9 qualification row
(`tests/dataplane/integration/run.sh`, `DATAPLANE_T4_ROW=M9`) now issues both
of its operator drains through the Rust admin port (`--admin-addr`): the Rust
readiness probe reports the gate's `drain_watermark` (1 after the pre-restart
drain, 2 after the post-restart drain), the control tap proves the restored
reconcile watermark of 1 across the Go-only restarts and that zero
`drain_command` frames crossed the bridge, and the row is the executable
`CP-FAULT-ADMIN-DRAIN-REPLAY` runner: re-posting the completed
`m9-post-restart` label answers `202` with the original binding, the status
query returns the byte-identical retained terminal, the watermark stays at 2
and the targeted session is closed exactly once (observed fields `drain_id`,
`command_sequence` via the watermark, `terminal_count` via the connection log,
`http_status`). The row receipt records `admin_drain.issuer = rust-admin`,
`bridge_drain_commands = 0` and the replay outcome. Slice 3b retires the wire path:
`drain_command`/`drain_result` keep their v1 tags as non-actionable
tombstones (the route-family precedent); under `RUST_ROUTE_OWNER` the Rust
dispatcher and the Go residual handler answer either body with a nonfatal
`PROTOCOL_VIOLATION` on the one legacy-violation counter (whose count now
includes retired drain bodies) and act on nothing; the Go `DrainIssuer`, the
bridge's drain re-sync and the API's `DataplaneDrainer` injection are
deleted, so Go's `/api/dataplane/drain*` answers `404 {"enabled":false}`
(the handler and interface stay as the HTTP contract and the differential
oracle); `ReconcileRequest.last_drain_command_sequence` is still filled from
the Rust gate watermark for diagnostics and Go restores nothing from it; the
control tap catalogs both bodies as retired so every T4 row proves zero
drain traffic on the bridge.

### Declared divergences after slice 2

- A `PUT /api/admin/config/` that changes a restart-required field (`workdir`,
  `proxy.addr`, `proxy.advertise-addr`, `proxy.pd-addrs`, `proxy.port-range`,
  `api.*`, `log.encoder`, `log.simple`, `ha.*`, `metering.*`,
  `rust-dataplane.*`) is rejected as a whole with `500 "can not update
  config"`, even when the document also carries dynamic changes; Go accepts it
  in memory and the change silently has no effect until restart.
- Namespace JSON renders an empty instance list as `[]`; Go renders a nil
  slice as `null` and an empty slice as `[]` depending on what was PUT. The
  differential compares those bodies by decoded value with nil ≡ empty.
- Configuration TOML/JSON bodies are compared by decoded value: gin renders
  TOML through go-toml v2 (single-quoted strings) and JSON with `omitempty`
  tags; the Rust renderer emits every field with its value. Consumers that
  decode (tiproxyctl, TiDB Dashboard) see the same configuration.
- The bare `/api/admin/namespace` and `/api/admin/config` paths are served
  directly instead of gin's `301` to the slash form.

### Declared divergences after slice 1

The differential script marks each of these and fails if one stops differing:

- `/api/metrics` and `/metrics` without the trailing slash answer `200`
  directly; gin answers `301` to the slash form. Prometheus follows either.
- `/api/debug/pprof/*` answers `404`. Go serves `net/http/pprof`. Profiling of
  the Rust process is a residual CP-ADMIN item, not closed by this slice.

### Evidence

`make controlplane-cpadmin-evidence` runs `tests/controlplane/cpadmin/run.sh`:
the production gin engine (via the api package's capture test, its unit-test
mocks and the real `ConfigManager` for namespaces and configuration) and the
Rust router over the in-memory `ConfigAdmin` (Go's per-process store
semantics) answer the same script; status, content type and body must match
exactly outside the declared list (configuration and nil-slice bodies by
decoded value), and the Go `ConfigManager`
checksum must equal the Rust `control-config` checksum for the default
configuration, a partial update, the identical update again and a
namespace-only mutation. The listener split, sniff timeout and graceful stop
are covered by the crate's own tests against real sockets and a self-signed
certificate.

## Remaining slices

- **Slice 3** — complete: 3a (local issuer, M9 on the Rust port, the
  `CP-FAULT-ADMIN-DRAIN-REPLAY` runner) and 3b (drain wire bodies retired as
  tombstones, Go issuer deleted, `last_drain_command_sequence` kept as a
  Rust diagnostic).
- **Slice 4a (log line contract)** — done: every Rust log line carries the
  Go logger's header, so `sysutil`-style readers parse Rust and Go logs
  alike. The encoder follows Go's `buildEncoder` case-sensitively:
  `log.encoder = "json"` renders the zap object shape
  `{"level","ts",...body fields}`, `"console"` renders zap's
  `ts<TAB>LEVEL<TAB><body>`, and every other spelling (including the default
  `tidb`, `JSON`, `Console`, empty) renders
  `[2006/01/02 15:04:05.000 -07:00] [LEVEL] <json body>`. `log.simple`
  drops the header (Go drops the time, level, caller and message keys).
  `log.level` is parsed exactly like zap's `ParseLevel` (exact spelling, then
  lowercase; empty is `info`; no trimming; `dpanic`/`panic`/`fatal` are
  thresholds above `error` that suppress every line this process emits; a
  rejected spelling fails startup like Go's `BuildLogger`). Reload follows Go's
  `updateLoggerCfg`: the file output is rebuilt first, and only then is the
  level parsed and applied, so a failed file switch keeps the old level and a
  rejected level keeps the running one (both are logged as errors).
  Levels: lifecycle events are `INFO` (`ERROR` when they carry an error
  class), dataplane session records `INFO`, admin access records for
  non-success responses `WARN` (gin's error branch), rejected persistent
  candidates `WARN`, failed log reloads `ERROR`. These rules are pinned by
  `rust/crates/control-plane/testdata/log-format-go.json`, recorded from the
  production Go builder by `tests/controlplane/cplog/format-probe` (18 level
  spellings with their emitted-line matrix, 8 encoder spellings with and
  without `simple`); `make controlplane-cplog-evidence` regenerates and
  compares it. **Declared format differences:** the message part stays the
  structured JSON object the Rust process always produced (Go renders a
  bracketed message and `[key=value]` fields, and with `simple` an empty
  `[]` caller bracket); Rust lines carry no caller. Rotation, reload and the
  retention gate are unchanged (the header counts toward `max-size` like any
  other bytes).
- **Slice 4b (diagnostics gRPC, `SearchLog`)** — done: the
  `diagnosticspb.Diagnostics` service (the vendored kvproto binding in
  `control-external`) is served on the admin listener exactly where gin's
  `grpcServer` middleware sits: after the rate limit and the readiness gate,
  before the routes, for HTTP/2 requests whose `Content-Type` starts with
  `application/grpc` (an HTTP/1.1 request with that content type is an
  ordinary 404); under HTTP TLS the service lives behind the TLS branch of the
  sniff like the Go cmux `TLS()` branch. `SearchLog` is the Go `sysutil`
  algorithm over the process log file (`--log-file`; Go: `log.log-file.filename`,
  handled with Go's `filepath.Dir`/`Ext`/`Join` semantics, so a bare
  `tiproxy.log` scans the working directory):
  directory entries whose full path starts with the configured path minus its
  extension and end with that extension or `<ext>.gz`, each probed for its
  first valid line (ten attempts) and, unless compressed, its last valid line
  (ten attempts, backward chunked reads; a compressed file's end is
  unbounded), kept when it overlaps `[start_time, end_time]` (`end_time = 0`
  is unbounded), sorted by first timestamp with only the last file starting
  before the window retained; lines are `[2006/01/02 15:04:05.000 -07:00]
  [LEVEL] message` located by the first `[`/`]` pairs, an unparseable line
  after a valid item is a continuation carrying that item's time and level,
  the scan stops at the first item past the window, items whose level is
  `UNKNOWN` pass every level filter, every pattern must match the message, and
  responses carry 1024 messages each with a final batch holding the remainder
  or nothing. A stream the client drops stops the scan.
  Evidence: `make controlplane-cpdiag-evidence` runs
  `tests/controlplane/cpdiag/script.json` against the production Go API
  server (h2c engine plus cmux TLS branch with auto certificates,
  `pkg/server/api.TestCPDiagCapture`) and the Rust listener (plaintext and
  TLS, `examples/cpdiag_replay.rs`) over a real gRPC wire and compares every
  packet (rotated and gzip backups, inclusive windows, level bitmask with an
  unknown level, all-patterns matching, invalid pattern, empty result, 1024
  batching with the trailing partial and empty packet, client cancellation)
  plus the HTTP/1.1 rejection; crate tests pin the sysutil fixtures
  (`TestResolveFiles`, `TestLogIterator`, gzip), the batch loop, cancellation
  and the gRPC split. **Declared differences:** Go error texts from the file
  system and the regexp compiler are not reproduced (only Go's
  `empty log file location configuration` is); files with an identical first
  timestamp are ordered by name here while Go's `sort.Slice` order between
  them is unspecified; the Go TLS branch does not advertise `h2` through ALPN
  (the oracle runs grpc-go with `GRPC_ENFORCE_ALPN_ENABLED=false`); and Go
  `regexp` versus the `regex` crate (both RE2-syntax families): `\d`, `\w`,
  `\s` and `\b` are Unicode-aware here and ASCII-only in Go, Go's `\Q...\E`
  literal quoting is not supported here (`\C` is rejected by both), a pattern over the crate's
  compiled-size limit is rejected here while Go compiles it, and Go's `(?U)`
  ungreedy flag and `[[:word:]]` classes behave the same.
- **Slice 4d (`tiproxyctl` compatibility)** — the real `tiproxyctl` binary is
  the third oracle client: `make controlplane-cpctl-evidence` builds it and
  runs `tests/controlplane/cpctl/script.json` against the production Go API
  server (plaintext, and the cmux TLS branch with auto certificates driven
  with `--insecure`; `pkg/server/api.TestCPCtlCapture`) and against the Rust
  listener in the same two modes (`examples/cpctl_replay.rs`), comparing
  every command's exit code and stdout: `health`, `config get`/`config set
  --input`, `namespace list`/`put`/`get`/`import`/`commit`/`del` including
  missing namespaces, and `traffic show`/`capture`/`replay`/`cancel`, which
  both sides refuse. The evidence overlay sets `enable-traffic-replay = false`, so both answers are the disabled message. A default configuration answers `unsupported` on the native process.
  `config get` and `health` are compared semantically (the TOML renderers
  differ in layout and omitted zero fields); every other observation,
  including the CLI's `namespace list` failure on an empty set (the Go server
  answers `""`, which the CLI cannot decode as a namespace list, and the Rust
  server answers the same), matches byte for byte. No declared differences.
- **Slice 4c (`ServerInfo`)** — the Go `sysutil` inventory, fixed first with
  a real probe (`tests/controlplane/cpdiag/serverinfo-probe`, run on macOS
  and in a Linux container): `LoadInfo` = `cpu/cpu` (load1/5/15), `cpu/usage`
  (ten tick ratios over a 1s sample), `memory/virtual` and `memory/swap`
  (`NaN` percentages without swap), `net/<nic>` counters (Go's `bytes-ent`
  key) and disk I/O rates over a 0.5s sample filed under type `net`;
  `HardwareInfo` = `cpu/cpu` (arch, logical/physical cores, `%.2fMHz`,
  cache), `memory/memory`, `disk/<dev>` per `/dev/` mount (fstype, opts,
  path, sizes, percentages) and `net/<nic>` (mac, five flags, CIDR
  addresses); `SystemInfo` = `system/sysctl` from a lexical `/proc/sys` walk
  (or `sysctl -a` split on the first colon when the walk fails) plus
  `system/kernel` transparent hugepage; `All` concatenates, an unknown type
  answers nothing, and the answer is sorted by type then name (Go's unstable
  sort leaves the order of equal `cpu/cpu` and `net/<nic>` pairs unspecified;
  the port keeps collection order). Linux is read natively without unsafe
  code from the files `gopsutil` reads (`/proc/loadavg`, `/proc/stat`,
  `/proc/meminfo` with the `MemAvailable` fallbacks, `sysinfo(2)`,
  `/proc/net/dev`, `/proc/diskstats`, `/proc/cpuinfo` with the `cpufreq`
  override and the parse failures Go propagates, `sysfs` topology,
  `/proc/1/mountinfo` with the `mounts` and `/proc/self` fallbacks, the
  `/dev/mapper` and `/dev/root` resolutions and Go's `strconv.Unquote` of
  mount points, `statfs(2)`, and the rtnetlink `RTM_GETLINK`/`RTM_GETADDR`
  dumps Go's `net.Interfaces` performs, with its `IFA_LOCAL` rule). macOS
  cannot make the Mach and IOKit calls `gopsutil` makes through cgo (the
  workspace forbids unsafe code), so it uses the `sysctl`, `vm_stat`, `mount`
  and `ifconfig` commands plus the same `netstat -ibdnW` parse Go uses.
  Evidence: `make controlplane-cpdiag-evidence` now compares every request
  type on the plaintext side and `LoadInfo` under TLS: the item inventory and
  ordered pair keys must be identical, static values (hardware, mounts,
  interfaces, kernel setting) byte-identical, live values in Go's exact
  format on both sides, and `sysctl` keys identical with values equal
  outside a listed volatile set. **Declared differences (macOS only, per Go
  `GOOS`):** `cpu/usage` and the disk I/O rate items are not produced (Mach
  ticks, IOKit), `cpu-frequency` is `0.00MHz` where Go reads the Apple
  silicon P-core frequency through IOKit, `mount(8)` does not print
  `MNT_MULTILABEL` so `opts` lacks `multilabel`, and `sysctl -a` live
  counters are compared by key only. Linux declares nothing.
- **Slice 5a (`GET /api/backend/metrics`)** — Go `BackendMetrics` answers
  `200` with the bare `Content-Type: application/json` and the bytes of
  `BackendReader.GetBackendMetricsByCluster(c.Query("cluster"))`: the first
  decoded `cluster` value (Go `url.ParseQuery`, a pair with a malformed
  escape dropped) selects the cluster, the empty name means the primary
  cluster (exactly one configured), a missing cluster or no primary answers
  an empty body, and the bytes are the owner's `marshalHistory`
  (`map[rule]map[backend]{Step1History,Step2History}`, filtered to the
  backends this owner read). The same endpoint is what members fetch from
  the owner. The Rust process already serves it on the metric-owner listener
  of `control-topology`'s `MetricCollector`; the management plane reads the
  identical bytes through `MetricOverlayHandle::backend_metrics_reader` (the
  served response's capture, liveness and ownership checks, taken once; no
  second owner), so the answer is empty while this process is not serving.
  HEAD and other methods are gin's `404`; the trailing-slash form is gin's
  `301` (declared with the other trailing-slash redirects). Evidence: the
  CP-ADMIN comparator fills the Go reader mock and the Rust hook from the
  same script entry and compares the empty, filled, no-cluster, encoded and
  bad-escape forms; router tests pin the query selection.
- **Slice 5b (`POST /api/debug/redirect`)** — Go `DebugRedirect` calls
  `NamespaceManager.RedirectConnections()`, which asks every namespace's
  router; the score-based router (`group.go`) offers every connection that
  is not already in `phaseRedirectNotify` a redirect **to the backend it is
  on** (reason `test`, a test/management reconnect that exercises the
  migration machinery without moving score), sets the phase whether or not
  `Redirect` accepted, records `accepted=false` for a refused offer (a
  closing connection) and never applies the balancer's cooldown; the
  routers return nil, so the handler answers `200 ""` and `500 "redirect
  connections error"` only for a router-level error. Other methods are
  gin's `404`. The Rust route plane sweeps every current router
  (`RoutePlaneHandle::redirect_connections`): each active session without
  a pending redirect gets a `Redirect` prepared by
  `Ledger::prepare_self_redirect` (same account allowed, no cooldown,
  closing sessions refused like Go's `Redirect` returning false, ordinary
  redirect accounting with source and target being one account: the
  session leaves and re-enters the same physical order, no score moves),
  offered through the production migration queue; a refused offer counts
  as not accepted, and the only error is a terminated route plane. The
  ordinary balancer path keeps its same-backend refusal and cooldown. The
  sweep summary (active, offered, accepted) is logged. The Go boundary is
  pinned by `pkg/balance/router.TestRedirectConnectionsDebugBoundary`
  (pending skipped, refused offer without error, no cooldown, score
  unchanged) and mirrored by the ledger test. Evidence: the CP-ADMIN
  comparator compares the sweep answer, the mocked router-level error and
  `HEAD`. The plain Rust integration run (`tests/dataplane/integration`,
  after the MIG-01 live migration) drives the sweep on the live Rust
  process through its admin port: the answer is `200 ""`, the logged
  summary accepts the persistent session, the route ledger settles with
  the same active count, and the session is still on A1 on a **new**
  backend connection with its database and user variable restored; the
  same row fixes the metric-owner port (`rust-dataplane.metrics-owner-port`)
  and requires the admin `backend/metrics` answer (`200`,
  `application/json`) to be byte-identical to the metric-owner endpoint's
  for both cluster names and the empty name.
- **Slice 5c (Go API retirement, final composition)** — capability 8
  (`RUST_API_OWNER`) is advertised and required by both processes like
  capabilities 6 and 7. Under it the Go process starts no `api.Server`
  (`pkg/server.NewServer` skips it when `rust-dataplane.enabled`, logging the
  address it left to Rust) and the Rust process binds the configured
  `api.addr` itself as its management listener (`--admin-addr` remains an
  override for harnesses that run a Go oracle beside it); the readiness
  gate, TLS branch, rate limit and route table are the slice 1–5b ones.
  `MetricsBatch` (tag 35) is a retired wire body: Rust's exporter keeps
  aggregating into the native registry every second and drops the per-tick
  deltas instead of sending them, and Go answers a batch with
  `PROTOCOL_VIOLATION` ("retired metrics message under RUST_API_OWNER"),
  counting it in `tiproxy_server_err{type="rust_legacy_metrics_violation"}`
  (`CompositeControlHandler.legacyMetricsViolations`; the control tap
  catalogs the body as legacy `metrics_batch`). `NativeAPIOwner` requires
  `RouteOwner` in `BridgeConfig`. The Rust binary advertises
  `rust-api-owner` in `--integration-capabilities`; the integration harness
  therefore drives every management call (namespace, config, metrics,
  dataplane status/drain, backend metrics, debug redirect) at the API port
  of the Rust process and no longer passes `--admin-addr`. Startup keeps
  the native metering owner's order (capability negotiation, `Service::open`,
  WAL replay with the SQL gate closed) with the operator listeners
  (health, metrics owner, management) bound before the control peer wait,
  so a bind conflict still fails fast; shutdown marks the process closing
  first and keeps the management plane readable through the session drain,
  sampler stop and final export. The profiling residual (`pprof`) stays open.

### Retired Go-only Prometheus series

`tests/controlplane/cpadmin/go-series-probe` enumerates 35 metric families: 20
are already served natively by Rust, and the 15 below were produced only by Go
and therefore disappear from the exposition once Rust owns `/metrics`.

| Group | Families |
| --- | --- |
| backend | `b_status`, `backend_metric`, `health_check_seconds`, `ping_duration_seconds` |
| balance | `b_conn`, `b_score`, `migrate_total`, `migrate_duration_seconds`, `pending_migrate` |
| monitor | `keep_alive_total`, `time_jump_back_total` |
| replay | `pending_cmds`, `wait_time` |
| server | `maxprocs`, `owner` |

The Go runtime collectors (`go_*`, `process_*`) describe the Go process and go
with them.

**Decision: retire them here and reimplement the ones that still describe
something real from Rust's own state in a later slice.** Go keeps no listener
for them, and they are not carried over the control bridge: that path is the
`MetricsBatch` body this slice retires, so reviving it would undo slice 5c.

Scope of the later slice, 12 of the 15:

- `backend` (4 families) come from the health-check loop, so Rust reproduces
  them from its own topology state.
- `balance` (5 families) must be re-sourced rather than copied. The Go values
  stopped describing real routing when Rust took ownership under capability 6:
  no Go `RouterAdapter` is constructed, so its connection ledger is never fed.
  The observables themselves remain meaningful and Rust holds the data — the
  route ledger carries per-session `sessions`/`active`/`incoming`/`outgoing`
  counts and the migration and redirect paths run in Rust.
- `monitor` (2 families) come from one loop, not two sources. Go samples the
  wall clock every 100ms, counts a jump when it reads earlier than before the
  wait, calls back every tenth tick and raises the keepalive every fifth
  callback, so the keepalive is that monitor's heartbeat rather than a lease.
  Rust reproduces the loop and both counters together.
- `server_owner` is read from live local ownership, not from a snapshot or a
  peer record, and follows Go in *deleting* the series when the process
  retires rather than zeroing it, so a retired owner leaves the exposition.
  Go's family also covers `vip/<address>` elections; whether Rust has an
  equivalent producer for those is not yet established.

Two families are retired outright rather than reimplemented:

- `server_maxprocs` reports `GOMAXPROCS`, a Go runtime concept with no Rust
  equivalent. Reusing the name for the async runtime's worker count would keep
  the name while changing its meaning, so the name retires with the Go
  process. A worker-count gauge, if wanted later, gets a name of its own.
- `replay` (2 families): traffic capture and replay are unsupported in Rust
  mode, so the series has no producer on either side.

Until that slice lands, these observations are absent; this document is their
declaration, not an implementation.
