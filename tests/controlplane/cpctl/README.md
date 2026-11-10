# tiproxyctl compatibility (CP-ADMIN slice 4d)

`make controlplane-cpctl-evidence` builds the real `tiproxyctl` binary and runs
`script.json` twice against the production Go API server
(`pkg/server/api.TestCPCtlCapture`: plaintext, then the cmux TLS branch with
auto certificates driven with `--insecure`) and twice against the Rust admin
listener (`rust/crates/control-admin/examples/cpctl_replay.rs`, same modes),
then compares every command's exit code and stdout: `health`, `config get/set`,
`namespace list/put/get/import/commit/del` (including missing namespaces) and
the four `traffic` subcommands, which both sides refuse. The evidence
overlay leaves traffic replay disabled, so both answers use that message. `config get` and the JSON outputs are compared semantically (the TOML
renderers differ in layout); everything else must match byte for byte.
`@name` arguments refer to the fixture files the script writes on both sides.
The CLI's `namespace list` on an empty set fails on both sides (the server
answers `""`, which the CLI cannot decode as a namespace list); that failure
is part of the compared behaviour.
