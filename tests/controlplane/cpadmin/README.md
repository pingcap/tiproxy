# CP-ADMIN differential evidence

`run.sh` answers the same request script on both sides and compares the
observations exactly:

- **Go**: `pkg/server/api.TestCPAdminCapture` builds the production gin engine
  through the package's own `createServerWithConfig` helper (real listener,
  the unit-test mocks, `enable-traffic-replay = false` so both sides answer
  the traffic routes with `disabled`) and replays `script.json`. Actions in the script toggle
  the readiness gate, the namespace manager readiness, the dataplane status
  reader and `PreClose`, exactly as the Go unit tests do.
- **Rust**: `control-admin`'s `cpadmin_replay` example drives the same script
  through `control_admin::full_router` with hooks that apply the same actions.
- **Checksums**: the Go `ConfigManager` CRC32 and the Rust `control-config`
  `go_checksum` are compared for the default configuration, a partial TOML
  update, the identical update again (no change) and a namespace-only mutation.

A step's `action` drives both sides before its request (`ready`,
`namespaces_ready`, `preclose`, `drainer`, and `backend_metrics`, which fills
the Go `BackendReader` mock and the Rust `backend_metrics` hook with the same
history bytes for every cluster name). Every step compares `status`,
`content_type` and `body` unless its entry lists `compare`. An entry with `declared` names a divergence the design document
owns (`docs/design/rust-control-admin.md`); it is reported, and the comparison
fails if such a step stops differing, so declarations cannot go stale.

`compare.py` decodes TOML with the standard `tomllib`, so the runner needs a
Python 3.11+ interpreter (it probes `python3.13`, `python3.12`, `python3.11`,
`python3`, or honours `PYTHON`).

```sh
make controlplane-cpadmin-evidence
# or
CPADMIN_EVIDENCE_DIR=/tmp/cpadmin tests/controlplane/cpadmin/run.sh
```
