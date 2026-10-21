# Rust offline replayer ownership

This document freezes the `CP-REPLAYER` (`#152`) ownership boundary before
the Go `cmd/replayer` implementation moves to Rust. The target is a separate
`tiproxy-replayer` executable. It is deliberately not linked into the
`tiproxy-rs` SQL-serving process and does not weaken `PARITY-EXCL-001`.

## Ownership and cutover

The Rust executable owns the standalone one-shot replayer and its replay-only
service mode. The one-shot CLI keeps the existing flag names, defaults, and
validation. Service mode keeps these endpoints and response shapes:

- `POST /api/traffic/replay`;
- `POST /api/traffic/cancel`;
- `GET /api/traffic/show`.

The standalone server does not advertise or accept traffic capture. Capture
and capture-file production belong to `CP-CAPTURE` (`#151`); the integrated
control API belongs to `CP-ADMIN` (`#150`). The old Go command is removed only
after the Rust release artifact and the evidence below are green. The Go
`pkg/sqlreplay/capture` package and the temporarily integrated Go traffic API
are not made Rust owners by this slice and remain covered by
`PARITY-EXCL-001` until their owning issues cut them over.

The implementation is split into:

- `replayer-core`: bounded config, format decoders, storage, ordering,
  scheduling, checkpointing, direct MySQL execution, and reports;
- `tiproxy-replayer`: CLI, signals, replay-only HTTP lifecycle, and version
  output.

Neither crate depends on `control-proto`, the legacy bridge, the topology
owner, or the SQL-serving dataplane. There is no replay payload over IPC or
FFI. A replayer process has exactly one job owner and at most one active job.
That invariant is process-local: during the migration window the standalone
Rust process and the integrated Go traffic API are not mechanically fenced
from each other. Deployments must not point both at the same replay input,
checkpoint, and target at once. Cross-process coordination is an operational
precondition, not an exactly-once lock supplied by this executable.

The first implementation PR is deliberately a **dry-run foundation**, not the
ownership cutover described by this target contract. It enables bounded input,
decode, filter, deterministic materialized ordering, and checkpoint evidence.
Backend execution, replay scheduling, SQL/report output, dynamic input,
service mode, and the Go-command cutover remain fail-closed until the relevant
evidence below is present. Where this document says the replayer "supports" or
"owns" those deferred paths, it describes the required cutover state rather
than behavior exposed by the foundation binary.

## Input contracts

### Native capture format

Native files preserve the current file and record contract:

- `traffic-2006-01-02T15-04-05.999.log[.gz]`, ordered by the timestamp in the
  filename and then by full path;
- optional `meta` JSON with version `v1`, duration, command counts, filtered
  counts, and optional `aes256-ctr` method;
- one record headed by `# Time`, `# Conn_ID`, optional `# Cmd_type`,
  `# Success`, `# Captured_ps_id`, `# Prepared_stmt`, and `# Payload_len`,
  followed by exactly `Payload_len` binary bytes and one newline;
- the decoded payload starts with the MySQL command byte. Query is the default
  when `Cmd_type` is absent.

Timestamps use RFC3339Nano. Unknown fields are ignored for forward
compatibility, but duplicate required fields, invalid integers/timestamps,
negative lengths, truncated payloads, oversized records, or a missing final
newline fail closed with path and byte-offset context. The default per-record
payload cap is 64 MiB and is never allocated before the length is validated.

### Audit log plugin format

The plugin decoder accepts the current bracketed key/value grammar and its
timestamp layout `2006/01/02 15:04:05.999 -07:00`. It preserves current
handling of `GENERAL`, `TABLE_ACCESS`, and `CONNECTION` events; per-decoder
connection remapping; retry filtering; case-insensitive user allowlists;
duplicate-write suppression; current database; prepared statement text/ID;
and execute parameters. Malformed quoting, nesting, timestamps, or a line
larger than 64 MiB fails closed rather than scanning without a bound.

### Audit log extension format

The extension decoder accepts `_LOG_TIME` plus the quoted tuple `EVENT`
contract for `QUERY`, `EXECUTE`, `CONNECTION`, and `DISCONNECT`. It preserves
the current `always`/`never` prepared-close strategies, skips redacted
parameters, and supports the existing end-time filter. For Go compatibility,
the command-start filter maps onto that same frontier; an explicit command-end
filter takes precedence. `directed` close and retry filtering remain rejected
for this format.

### Ordering and filtering

Multiple readers are merged by `(command_start_timestamp, connection_id,
source_ordinal, record_ordinal, command_ordinal)`. The first two keys preserve
the observable Go contract; the final keys make previously unstable exact ties
deterministic, including `PREPARE` / `EXECUTE` / `CLOSE` commands expanded from
one audit record.
Audit inputs use the configured bounded reorder buffer. Native input rejects
multiple roots and a non-directed prepared-close strategy.

`--read-only` uses a TiDB-aware bounded tokenizer/normalizer, not a generic SQL
parser. It freezes Go `Normalize` / `NormalizeDigest` output for the statements
used by reporting and the current read-only lexical decision. Tokens inside
comments, quoted strings, identifiers, and executable comments cannot become
keywords. Invalid UTF-8, unterminated constructs, nesting/token-count overflow,
and inputs over 64 MiB are classified explicitly and never panic or loop.

## Storage contract

`replayer-core` exposes a TiProxy-owned object-store trait implemented by
Apache OpenDAL. The dependency is exact-pinned, has default features disabled,
and enables only `fs`, `s3`, `gcs`, `azblob`, `oss`, and `cos` plus the Tokio
executor. Provider clients never escape this module.

The URI adapter accepts a provider-specific allowlist of the Go-compatible
read-side query parameters and rejects unknown, duplicate, bucket/container,
and root overrides before constructing OpenDAL. It preserves provider
root/path rules, ordered pagination, and directory listing behavior.
Credentials are accepted only through that configuration boundary and are
redacted from errors, diagnostics, job JSON, and logs. The foundation performs
one bounded storage attempt and fails closed. Before service, backend, or
dynamic-input mode is enabled, transient reads/lists must use the bounded,
cancellation-aware retry classifier from the CP-002 policy; invalid
configuration, permission failures, missing objects, corrupt data, and decode
failures remain terminal.

Local checkpoint and metadata writes use create-in-the-same-directory,
`sync_all`, atomic rename, and parent-directory sync. The current Go checkpoint
and SQL-output flags are filesystem paths, so `CP-REPLAYER` uses OpenDAL cloud
services only for read/list/stat and rejects remote checkpoint or output URLs.
It therefore has no shared remote writer and makes no cross-provider atomic
publication claim. If a later owner adds remote publication, it must query
OpenDAL's `Capability.write_with_if_not_exists` at runtime and use a provider's
native atomic conditional-create primitive; read-then-write emulation is not
acceptable. Providers without that capability must reject shared-writer mode.
Capture-side remote publication remains owned by `CP-CAPTURE` (`#151`).

Native streams apply AES-256-CTR and gzip in the same layer order as Go. The
first 16 bytes are the random IV and the key is exactly the first 32 bytes of
the key file. Short keys, short IVs, invalid gzip streams, wrong metadata, and
trailing/truncated records fail closed. Local AES key-file contents use a
zeroizing buffer. URI credentials are redacted from diagnostics, but complete
provider-config zeroization is a cutover gate before untrusted service mode.

Dynamic input initially supports the Go-compatible local and S3 modes. A
directory is assigned with FNV-1a modulo `replayer-count`; `replayer-index`
must be lower than that count. Listings are paginated and deduplicated. EOF
polling is driven by a cancellation-aware timer rather than an unconditional
sleep.

## Replay engine contract

The engine maintains a bounded queue per logical captured connection and one
socket owner per live replay connection. It preserves per-connection order,
uses the first captured timestamp and `--speed` to derive absolute deadlines,
and sends late commands immediately. Buffer, connection, pending-command, and
slowdown thresholds are explicit; overflow cancels the job with a typed error.

Direct MySQL connections support TCP and backend TLS with bounded dial,
handshake, packet, and response deadlines. Authentication supports the TiDB
paths used by the Go replayer (`mysql_native_password` and
`caching_sha2_password`, including TLS full-auth and auth switch). Passwords,
scrambles, SQL payloads, and storage credentials are absent from `Debug` and
error output.

Each command writes the exact captured payload and drains the complete response
before that connection advances. `COM_STMT_PREPARE` records the replay ID and
parameter count. `EXECUTE`, `FETCH`, `CLOSE`, `RESET`, and `SEND_LONG_DATA`
rewrite only the four-byte statement ID, using the captured-to-replay mapping.
The configured prepared-close strategy remains `directed`, `always`, or
`never`. A backend disconnect retires only its socket; the next command
reconnects and restores the command's current database. `--ignore-errs`
controls command failures, never configuration, corruption, authentication,
or ownership failures.

`--dry-run` performs the read/decrypt/decompress/decode/order/filter path
without a backend. The foundation materializes and globally sorts decoded
commands, so it does not yet exercise the configured reorder-buffer bound.
Backend/execution modes stay fail-closed until a bounded merge/scheduler uses
the same ordering and proves equal command/filter counts and checkpoint
frontier.

Checkpoints contain schema-v2's full durable frontier
`(command_start_timestamp, connection_id, source_ordinal, record_ordinal,
command_ordinal)`, the command end timestamp, and an input identity digest that
also binds the frontier schema. Loading a checkpoint with an older schema or
different inputs, format, filters, partitioning, or encryption metadata fails
closed. SIGINT/SIGTERM
stops new scheduling, drains only when graceful cancellation was requested,
publishes the final checkpoint/report atomically, joins all owned tasks, and
then exits. The restart contract is explicitly bounded at-least-once, not
exactly-once: a command whose request was sent but whose response/frontier was
not committed at process death may execute once again after restart. Bare
MySQL supplies no idempotency token or transaction with which the replayer
could rule that out, so write traffic can have a duplicate effect at the
frontier.

## Reports and service mode

Job JSON preserves the observable fields `type`, `status`, `start_time`,
`end_time`, `progress`, `error`, `last_cmd_ts`, `input`, `username`, `format`,
`speed`, `readonly`, and `addr`; URLs are credential-redacted. History is
bounded to ten jobs. Progress never decreases. Only one job may be running or
stopping.

Failed commands are grouped by TiDB-compatible normalized digest and written
to the existing `tiproxy_traffic_replay.fail` and `other_errors` schemas. SQL
output is opt-in, written with bounded records, and never includes passwords or
cloud credentials. Service-mode form parsing keeps current names and defaults,
rejects unknown or duplicate scalar fields, and bounds request bodies. Cancel
supports immediate and graceful modes with one absolute timeout.

## Differential and fault evidence

The ownership move requires all of the following from the exact cutover PR
head. The current foundation evidence covers well-formed native, audit-plugin,
and audit-extension command observations (including allocation, duplicate and
retry filtering, prepared expansion, and binary `KindBytes`), plus Rust-local
malformed/boundary, gzip/AES, storage-mapping, and checkpoint tests. It does not
claim the complete corpus or live/provider/restart coverage listed below.

1. A Go oracle and the production Rust decoders consume the same native,
   plugin, and extension corpus. Semantic command observations, normalized
   output/digests, filtering, ordering, prepared mappings, counts, and public
   errors compare exactly. The corpus includes TiDB syntax, malformed quoting,
   invalid UTF-8, boundary lengths, timestamp ties, encryption, and gzip.
2. Local and MinIO/OpenDAL runs compare URI mapping, multi-page ordering,
   not-found/permission/transient errors, credential redaction, cancellation,
   and atomic publication. A page-token and retry-class mutation must be killed.
3. A real TiDB run covers plain and TLS authentication, query/init-db/quit,
   prepare/execute/long-data/reset/fetch/close, read-only filtering, reconnect,
   ignore-errors, SQL/report output, and equal per-connection order at 0.5x,
   1x, and 2x. A statement-ID and deadline mutation must be killed.
4. A process-death run kills the replayer during decode and during an in-flight
   response, restarts from the checkpoint, and proves no command before the
   committed frontier is replayed twice while allowing only the one boundary
   in-flight command to repeat. Dynamic-input owner partitioning is stable
   across restart. A non-atomic-checkpoint mutation must be killed.
5. Rust format, lint, unit, doc, release (amd64/arm64), supply-chain, stale-lock,
   negative, and `PARITY-EXCL-001` gates are green. The released artifact
   reports the repository version/commit/build time and the Go `cmd/replayer`
   build target resolves to the Rust artifact after cutover.

The PR must list the stopped Go command/manager ownership and the remaining
Go traffic surface. No completion claim is made from fixture-only or dry-run
evidence.
