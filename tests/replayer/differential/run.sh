#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
corpus="$repo_root/tests/replayer/differential/corpus.v1.json"
tmp_dir=$(mktemp -d)

cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

cd "$repo_root"
go run ./tests/replayer/differential/go-observer -corpus "$corpus" >"$tmp_dir/go.json"
CP_REPLAYER_CORPUS="$corpus" cargo run --locked --quiet --manifest-path rust/Cargo.toml \
    -p replayer-core --example cp_replayer_observer >"$tmp_dir/rust.json"
go run ./tests/replayer/differential/go-observer \
    -baseline "$tmp_dir/go.json" -candidate "$tmp_dir/rust.json"

CP_REPLAYER_CORPUS="$corpus" CP_REPLAYER_MUTATE_CONNECTION_ID=1 \
    cargo run --locked --quiet --manifest-path rust/Cargo.toml \
    -p replayer-core --example cp_replayer_observer >"$tmp_dir/rust-mutated.json"
set +e
go run ./tests/replayer/differential/go-observer \
    -baseline "$tmp_dir/go.json" -candidate "$tmp_dir/rust-mutated.json"
status=$?
set -e
if [[ "$status" -eq 0 ]]; then
    echo "CP-REPLAYER connection-ID mutation was not killed" >&2
    exit 1
fi
