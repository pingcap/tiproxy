# TiProxy Query Interaction Latency Design

## 1. Background

As a TiDB gateway, TiProxy already exposes command-level total duration metrics. However, those metrics cannot directly answer:

- Whether one MySQL interaction (`request -> first backend response packet`) is slow.
- Whether a latency spike comes from TiDB backend latency or TiProxy overhead.
- Whether backend label cardinality can grow indefinitely when backend addresses change.

To solve this, we add interaction latency observability, slow interaction logs, and backend metrics label GC.

## 2. Goals and Non-goals

### 2.1 Goals

- Provide per-interaction latency metrics with dimensions `backend`, `cmd_type`, and `sql_type`.
- Support dynamic updates for slow interaction threshold without restarting TiProxy.
- Support username-pattern filtering for interaction metrics to reduce troubleshooting-time metric pressure.
- Control metrics-label memory growth with TTL-based GC.
- Keep production stable: interaction metrics remain disabled by default; options are dynamically reloadable.

### 2.2 Non-goals

- Full SQL tracing/span storage.
- Changing semantics of existing `query_duration_seconds`.
- Storing full per-request raw details externally.

## 3. Terminology

- Interaction: one MySQL command round from forwarding request to TiDB until receiving the first response packet.
- Command Duration: existing full command processing duration metric.
- Interaction Duration: first-response latency introduced by this feature.

## 4. Configuration

New `[advance]` options:

- `query-interaction-metrics` (bool)
  - Enable interaction latency metrics.
  - Default: `false`.
- `query-interaction-slow-log-threshold-ms` (int)
  - Slow interaction log threshold in milliseconds.
  - `0` disables slow interaction logs.
  - Default: `200`.
- `query-interaction-slow-log-only-digest` (bool)
  - When `true`, slow interaction logs print `sql_digest` and omit the normalized `query` text.
  - Default: `false`.
- `query-interaction-user-patterns` (string)
  - Comma-separated, case-sensitive glob patterns for usernames.
  - Examples: `app_*`, `readonly`, `tenant_??`.
  - Empty means no filtering (collect all usernames).
  - Default: `""`.
- `backend-metrics-gc-interval-seconds` (int)
  - Backend metrics GC sweep interval.
  - `0` disables GC.
  - Default: `300`.
- `backend-metrics-gc-idle-seconds` (int)
  - Idle TTL for backend labels; stale labels are removed after TTL.
  - `0` disables GC.
  - Default: `3600`.

All options support dynamic updates through `PUT /api/admin/config`.

## 5. Metrics and Logs

### 5.1 New metric

- `tiproxy_session_query_interaction_duration_seconds` (HistogramVec)
  - Labels: `backend`, `cmd_type`, `sql_type`
  - Buckets aligned with `query_duration_seconds`
  - Collected only when username matches `query-interaction-user-patterns`
  - `sql_type` is fixed: `select|insert|update|delete|replace|begin|commit|rollback|set|use|other`

### 5.2 Slow interaction logs

When `interaction_duration >= threshold`, TiProxy logs `Warn`:

- Base fields:
  - `interaction_time`
  - `interaction_duration`
  - `connection_id`
  - `cmd`
  - `sql_type`
  - `username`
  - `backend_addr`
  - `sql_digest` (only for `COM_QUERY`)
- Filter fields:
  - `username_pattern_matched`
  - `username_matched_pattern`
- Conditional fields:
  - `query` for `COM_QUERY` (normalized and truncated), unless `query-interaction-slow-log-only-digest=true`
  - `stmt_id` for `COM_STMT_*` when statement id exists

When slow interaction also matches username pattern, TiProxy emits an extra `Warn` log:

- `slow mysql interaction matched username pattern`

## 6. Data path and instrumentation

Instrumentation is in the `CmdProcessor` forwarding path, sampled per interaction:

- Single-response commands: observed once when first response packet arrives.
- `COM_QUERY`/`COM_STMT_EXECUTE` with multiple results: observed per result round.
- `COM_QUERY`: lightweight first-keyword classification fills fixed `sql_type`; unrecognized SQL is `other`.
- `LOAD DATA LOCAL INFILE`: includes the final response after file transfer phase.
- `COM_CHANGE_USER`: includes multi-round auth switch interactions.
- No-response commands (for example `COM_QUIT`) are not observed.

## 7. Backend metrics GC

`backend metrics cache` tracks `lastSeen`:

1. Refresh `lastSeen` on each metric update.
2. Run sweep when GC interval arrives.
3. If `now - lastSeen > idleTTL`:
   - Delete cache entry.
   - Call Prometheus `DeleteLabelValues` to remove metric labels.

GC coverage:

- `query_total`
- `query_duration_seconds`
- `query_interaction_duration_seconds`
- `handshake_duration_seconds`
- traffic counters

## 8. Performance and stability

- Interaction metrics are disabled by default to avoid default overhead.
- Main incremental cost when enabled:
  - monotonic time calculation
  - histogram observe
  - slow-threshold evaluation and logging
- GC uses low-frequency + TTL sweep to avoid per-request full scans.
- Username filter uses pre-parsed glob patterns; cost is linear to pattern count.

Recommended initial reservation before rollout:

- CPU: +15%
- Memory: +10%

Validate with workload-specific benchmarks.

## 9. Testing strategy

- Config tests: defaults, serialization, invalid value checks.
- Dynamic update tests: runtime updates effective under `WatchConfig`.
- Metrics tests:
  - interaction histogram sample count increments
  - TTL GC removes stale backend labels
- Protocol-path regression: full `pkg/proxy/backend` tests pass.

## 10. Compatibility

- Existing metrics and semantics stay unchanged.
- New options have defaults, so upgrades are backward-compatible.
- To fully disable GC, set `backend-metrics-gc-interval-seconds=0` or `backend-metrics-gc-idle-seconds=0`.
