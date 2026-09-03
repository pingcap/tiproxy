---
description: 
alwaysApply: true
---

# AGENTS.md

This file provides guidance to agents (human and AI) when working with code in this repository.

The goal is to:

- Keep code structure consistent.
- Make changes safe and testable.
- Avoid common pitfalls around concurrency, logging, and configuration.

## Development Environment

- **Go version**
  - Use the Go version specified in `go.mod` or the CI configuration.
  - Do not downgrade or upgrade the Go version casually; if a version change is needed, treat it as a dedicated change and update CI accordingly.

- **Module and dependencies**
  - Use `go mod tidy` to manage dependencies; do not edit `go.sum` manually.
  - Avoid committing temporary or experimental dependencies; prefer small, focused changes that clearly justify new modules.

- **Formatting and linting**
  - Always run `make lint` after your changes.

- **Rust toolchain and dependencies**
  - Use the pinned toolchain in `rust-toolchain.toml`; do not change the MSRV or edition as part of an unrelated change.
  - Add dependencies at workspace scope in `rust/Cargo.toml`, use `workspace = true` from member crates, and keep default features empty unless the feature is required in every deployment.
  - Keep `rust/Cargo.lock` committed and use `--locked` for builds and tests. Do not edit the lockfile manually.
  - Run `make rust-lint`, `make rust-test`, and `make rust-build` after Rust changes.

## Code Organization

When adding or modifying features, prefer extending existing packages before creating new ones. If a new package is required, document it here so future contributors understand its purpose.

**Package structure:**

- `cmd/replayer/` - Entry point for the client-mode traffic replayer.
- `cmd/tiproxy/` - Entry point for the TiProxy service.
- `cmd/tiproxyctl/` - Entry point for the controller that operates TiProxy based on the TiProxy API.
- `lib/cli/` - Implementation for `tiproxyctl`.
- `lib/config/` - Definition and default values of TiProxy configurations.
- `lib/util/` - Utilities, including error wrapping, logging helpers, retrial process, certification, etc.
- `pkg/balance/factor/` - Factor-based load-balance policy. The load balancer calculates backend scores based on these factors.
- `pkg/balance/metricsreader/` - Collects backend metrics so that factors can calculate scores based on metrics.
- `pkg/balance/observation/` - Read-only routing lifecycle values, mixed record/byte capture leases and bounded journal admission; owns no production router, policy, connection or transport.
- `pkg/balance/observer/` - Queries backend list and checks backend health, then notifies the router.
- `pkg/balance/policy/` - The `BalancePolicy` interface and implementations such as `SimpleBalancePolicy`.
- `pkg/balance/router/` - Load-balance and routing service. Rebalances and routes connections based on a `BalancePolicy`.
- `pkg/controlbridge/shadow/` - Strict v2 lifecycle/v3 native factor observation codecs and optional local UDS; has no production command or accounting authority.
- `pkg/controlbridge/` - Versioned Go/Rust control-protocol codecs, bounded transport, and control-plane adapters; SQL packet payloads never enter this package.
- `pkg/manager/cert/` - Auto-reloads certificates and provides interfaces to query them.
- `pkg/manager/config/` - Auto-reloads configuration files and provides interfaces to query them.
- `pkg/manager/elect/` - Manages TiProxy owner elections (for example, metrics reader and VIP modules need an owner).
- `pkg/manager/id/` - Generates global IDs.
- `pkg/manager/backendcluster/` - Manages cluster-scoped backend runtimes and shared resources such as PD or etcd clients.
- `pkg/manager/infosync/` - Queries the topology of TiDB and Prometheus from PD and updates TiProxy information to PD.
- `pkg/manager/logger/` - Manages the logger service.
- `pkg/manager/memory/` - Records heap and goroutine profiles when memory usage is high.
- `pkg/manager/meter/` - Outputs outbound and cross-AZ network bytes to external storage.
- `pkg/manager/namespace/` - Manages multiple namespaces.
- `pkg/manager/vip/` - Elects, binds, and unbinds the virtual IP address for high availability.
- `pkg/metrics/` - Defines Prometheus metrics.
- `pkg/proxy/` - Starts and stops the proxy service.
- `pkg/proxy/backend/` - Handshakes and forwards messages between clients and backends; also migrates connections among backends if the load balancer requests it.
- `pkg/proxy/client/` - Wrapper of the client connection.
- `pkg/proxy/keepalive/` - Configures TCP keep-alive settings.
- `pkg/proxy/net/` - Parses and packages MySQL-wire packets.
- `pkg/proxy/proxyprotocol/` - Parses and fills the Proxy Protocol header.
- `pkg/sctx/` - Defines the global context.
- `pkg/server/` - Starts and stops the TiProxy server, including proxy service, API service, and other managers.
- `pkg/server/api/` - Starts and stops the TiProxy API service.
- `pkg/sqlreplay/` - Captures and replays traffic.
- `pkg/testkit/` - Common test utilities.
- `pkg/util/` - Utilities, including buffered IO, etcd wrapper, HTTP wrapper, simple MySQL lexer, and others.
- `rust/crates/mysql-wire/` - MySQL wire-format types and codec boundaries; it does not own sockets or routing decisions.
- `rust/crates/proxy-io/` - Client/backend transport ownership, including future TLS, compression, and PROXY protocol support.
- `rust/crates/replayer-core/` - Bounded offline traffic replay primitives: input storage, native/audit decoding, deterministic ordering, filtering, and local checkpoints; it has no control-plane or SQL-serving dependency.
- `rust/crates/session-core/` - Protocol-independent session lifecycle and migration state.
- `rust/crates/control-router/` - Staged namespace-scoped routing selection, exact config/topology/health capture, and reservation accounting. Production dataplane/binary wiring remains prohibited until full routing parity is complete.
- `rust/crates/control-routing/` - Protocol-independent route values, pure group matching/port-conflict primitives, and Go-compatible time arithmetic; it owns no reservation or production selector yet and carries no wire payloads.
- `rust/crates/legacy-router-shadow/` - Temporary read-only routing observation codecs, native factor read-window decoding and bounded UDS consumption inside the existing Rust process. Domain mirror state stays in `control-router::shadow`; it has no production routing authority.
- `rust/crates/control-proto/` - Versioned Go/Rust control-plane contracts; MySQL packet payloads must never cross this boundary.
- `rust/crates/control-topology/src/metric_collector/` - Opt-in in-process metrics rounds, scoped owner tasks, qualified overlay and bounded real owner HTTP service; production composition remains off until the control-plane integration slice.
- `rust/crates/control-topology/src/api_replay.rs` - Test-feature-only whole observer result input through existing publishers; never seeds router state or runs in the production binary.
- `rust/crates/control-topology/src/metrics/` - Bounded query/history data core for staged backend resource metrics; runtime source/election authority is supplied separately by the collector.
- `rust/crates/control-meter/` - Native durable absolute-meter consumer/outbox, Go SDK-compatible gzip export, LocalFS adapter and in-process intake/export lifecycle; no control-proto dependency. Cloud adapters and production sampler handoff remain staged.
- `rust/crates/control-plane/` - Process-local Rust control-domain types, ownership fencing, config/TLS views, lifecycle/shutdown, and bounded observability; it must not depend on `control-proto`.
- `rust/crates/control-admin/` - Rust management plane (CP-ADMIN #150): the Go `pkg/server/api` HTTP surface, middleware order, TLS/plaintext split and health semantics on `--admin-addr`; process state arrives through hooks, never payloads.
- `rust/crates/control-external/` - Fenced, bounded Rust clients for control-plane external dependencies plus the minimal diagnostics gRPC binding; generated etcd protobuf types stay private to `etcd-client`.
- `rust/crates/control-etcd/` - Stateful PD-etcd lease, session, election, resumable-watch, and transaction ownership fenced by the Rust process generation.
- `rust/crates/dataplane/` - Rust dataplane orchestration across wire, transport, session, and control components.
- `rust/crates/differential-runner/` - Payload-free Rust consumer for the immutable Go protocol corpus; it drives merged wire/session parsers and emits sharded semantic observations.
- `rust/crates/tiproxy-rs/` - Rust dataplane executable and build/version metadata.
- `rust/crates/tiproxy-replayer/` - Standalone Rust offline traffic replayer composition root; replay payloads never enter the SQL-serving process or control bridge.
- `tests/compatibility/` - Versioned client-driver and MySQL capability contract shared by the Go/Rust dataplane integration tests.
- `tests/controlplane/cproute/api-differential/` - One test-only external router API input/output runner and its frozen acceptance/retirement inventories; synthetic adapter smoke does not qualify recorded-trace acceptance. Its `metrics/` package shares value-only query packets and decoding between recording and API replay; it owns no factor state. Its `clock/` package supplies the shared public event clock to test-build overlays only.
- `tests/controlplane/` - Versioned Go control-plane contract and fault catalogs plus the payload-free observation comparator used by every Rust control-plane migration slice.
- `tests/controlplane/cpadmin/` - Go gin engine (api package capture test with its unit-test mocks) vs Rust `control-admin` router on one request script, plus Go/Rust config-checksum scenarios; declared divergences are listed in `docs/design/rust-control-admin.md` and fail closed when they stop differing.
- `tests/controlplane/cplog/` - Go lumberjack oracle and TZ-parameterized harness that pins the Rust log-rotation retention (local-time names, UTC age parse) to the Go logger.
- `tests/dataplane/metrics/` - Recorded Rust metrics batches plus the Go `promhttp` oracle that pins the Rust native `/metrics` exposition to the Go output for the same series.
- `tests/dataplane/corpus/` - Deterministic, language-neutral Go dataplane protocol corpus.
- `tests/dataplane/differential/` - Go/Rust corpus comparator, first-divergence report, parity coverage/exclusion gate, mutation self-check, and sharded CI entrypoint.
- `tests/dataplane/drift/` - Semantic CI gate that keeps Go dataplane changes synchronized with Rust parity artifacts.
- `tests/dataplane/integration/` - Real TiDB topology, fault injection, readiness, diagnostics, and owned cleanup for Go/Rust dataplane validation.

Guidelines:

- New load-balance factors should go to `pkg/balance/factor/`.
- New management logic that needs coordination or auto-reload should go under the corresponding `pkg/manager/*` package where possible.
- Shared helpers should go to `lib/util` or `pkg/util` instead of being duplicated across packages.
- After adding a new top-level package or command, update this file to describe it.

## Code Style

- **File headers**
  - When creating new source files (for example `*.go`), include the TiProxy copyright and Apache 2.0
    license header at the top.
  - Copy the header from an existing file in the same directory and update the year if needed.
  - Rust source and build-script files use the same header in line-comment form.

- **Structs and pointers**
  - Use pointers for large structs or structs that are frequently passed around.
  - Avoid pointer-to-interface patterns; prefer passing interfaces by value.
  - Avoid copying structs that contain mutexes or other synchronization primitives; instead, pass pointers or provide methods on the owning type.

- **Error handling**
  - Prefer wrapping errors with context so they are actionable (what operation failed, which component, key parameters).
  - Reuse common error helpers from `lib/util` / `pkg/util` when available.
  - Add `errors.WithStack()` only at the lowest layer that first creates or captures the concrete error. Higher layers should usually add context with `errors.Wrap()` / `errors.Wrapf()` instead of attaching another stack trace.
  - Do not silently ignore errors; either handle them explicitly or return them to callers.

- **Comments for non-obvious logic**
  - Add succinct comments around code paths whose correctness depends on subtle concurrency, networking, ownership, or failure-handling assumptions.
  - The goal is to explain *why* the code is written that way so future contributors do not "simplify" it into a regression.
  - Avoid repeating what the code already says; focus comments on invariants, trade-offs, and external behavior.

- **Logging**
  - Use the shared logging facilities (for example, logger manager) rather than creating ad-hoc loggers.
  - Include important identifiers (such as namespace, connection ID, cluster information) in logs when they help debugging.
  - Use log levels consistently: reserve `Error` for unexpected failures, `Warn` for recoverable or noisy situations, and `Info` for normal but important events.

- **Concurrency and context**
  - Always pass `context.Context` through call chains where operations may block, allocate resources, or perform I/O.
  - Do not start goroutines without a clear lifetime; ensure there is a way to stop them (via context cancellation or explicit shutdown).
  - Avoid bare `go func()` for managed background work. Prefer the repository `waitgroup` helpers (for example `pkg/util/waitgroup.WaitGroup.Run` or `RunWithRecover`) so goroutine lifecycle and shutdown are tracked consistently.
  - If a background goroutine should recover from panic instead of crashing the whole process, use `waitgroup.RunWithRecover()` and handle recovery through the shared helper rather than ad-hoc `recover()` logic.
  - If a wait or sleep may delay shutdown, owner handoff, or other cancellation-sensitive flows, do not use an unconditional `time.Sleep`. Prefer `timer + context` (or an equivalent cancellable wait) so the code can exit promptly.
  - Avoid sharing mutable state across goroutines without proper synchronization.
  - Be careful when exposing channels and mutexes in public APIs; clearly document ownership and who is responsible for closing channels.

## Testing Tips

### How to run unit tests

- **Per-package tests**

```bash
# in the root directory of the repository
cd pkg/<package_name> && go test -run <TestName>
```

- **All unit tests**

```bash
go test ./...
```

Use `pkg/testkit` helpers where possible to keep tests consistent and avoid duplicating setup logic.

### Unit test expectations

- Make sure all unit tests in the same package pass after a package is modified.
- Reuse existing tests as much as possible; extend or adapt them when changing behavior instead of writing entirely new, overlapping tests.
- For tests that require external components (such as TiDB, PD, or Prometheus), follow the instructions in the test files or documentation (for example, comments or scripts in the repository) and avoid hard-coding environment-specific paths.

## Change Management and Safety

- **Scope of changes**
  - Keep changes focused: small, targeted modifications are easier to review and reason about.
  - If implementing a new feature requires refactoring, it is acceptable to adjust nearby code (rename, extract helpers, clarify interfaces), but avoid large unrelated refactors in the same change.

- **Running checks**
  - At minimum, run unit tests for any packages you touch.
  - For critical paths (such as `pkg/proxy`, `pkg/server`, and `pkg/manager/*`), consider running a broader subset of tests or end-to-end scenarios when available.

- **Files that should not be committed**
  - Do not commit local environment files, certificates, logs, or replay outputs (for example `.env`, `server.key`, `server.crt`, `*.log`, `replay.log`, local config or test data) unless they are explicitly part of the repository.

- **Keeping this guide up to date**
  - When introducing new top-level packages, commands, or significant components, update the **Code Organization** section.
  - When introducing new conventions (for logging, testing, or concurrency), document them here so future contributors and agents can follow the same rules.
