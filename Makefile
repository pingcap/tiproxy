# Copyright 2020 Ipalfish, Inc.
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO := go
CARGO := cargo
GOBIN := $(shell pwd)/bin
VERSION ?= $(shell git describe --tags --dirty --always)
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
COMMIT ?= $(shell git describe --match=NeVeRmAtCh --always --abbrev=40 --dirty)
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
DEBUG ?=
DOCKERPREFIX ?=
BUILD_TAGS ?=
LDFLAGS ?=
LDFLAGS += -X "github.com/pingcap/tiproxy/pkg/util/versioninfo.TiProxyVersion=$(VERSION)"
LDFLAGS += -X "github.com/pingcap/tiproxy/pkg/util/versioninfo.TiProxyGitBranch=$(BRANCH)"
LDFLAGS += -X "github.com/pingcap/tiproxy/pkg/util/versioninfo.TiProxyGitHash=$(COMMIT)"
LDFLAGS += -X "github.com/pingcap/tiproxy/pkg/util/versioninfo.TiProxyBuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"

BUILDFLAGS ?= -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '$(BUILD_TAGS)'
ifneq ("$(DEBUG)", "")
	BUILDFLAGS += -race
endif
IMAGE_TAG ?= latest
EXECUTABLE_TARGETS := $(patsubst cmd/%,cmd_%,$(wildcard cmd/*))
RUST_MANIFEST := rust/Cargo.toml
RUST_TARGET ?= $(shell rustc -vV | sed -n 's/^host: //p')
RUST_BUILD_ENV := TIPROXY_VERSION=$(VERSION) TIPROXY_COMMIT=$(COMMIT) TIPROXY_BUILD_TIME=$(BUILD_TIME)
CARGO_AUDIT_VERSION := 0.22.2
CARGO_DENY_VERSION := 0.20.2
RUST_TOOL_ROOT ?= $(GOBIN)/rust-tools
RUST_TOOL_BIN := $(RUST_TOOL_ROOT)/bin

.PHONY: cmd_% test lint parity-drift parity-drift-weekly docker docker-release golangci-lint gocovmerge clean rust-build rust-test rust-doc-test rust-lint rust-release rust-install-tools rust-supply-chain rust-negative-tests control-proto-generate control-proto-generate-check controlplane-contracts controlplane-differential controlplane-differential-self-test controlplane-cp001-evidence controlplane-cplog-evidence controlplane-cpadmin-evidence controlplane-cpdiag-evidence controlplane-cpctl-evidence controlplane-cpmeter-evidence controlplane-cp002-evidence controlplane-cp003-evidence controlplane-cp003-authority-evidence controlplane-cp004-evidence controlplane-cproute-evidence controlplane-cpmetrics-evidence controlplane-cpmetrics-applied-evidence controlplane-cpmetrics-collector-evidence controlplane-cpmetrics-factor-evidence controlplane-cproute-resource-evidence controlplane-cproute-migration-evidence controlplane-cproute-balance-evidence controlplane-cproute-worker-evidence controlplane-cproute-shadow-evidence controlplane-cproute-recorder-evidence controlplane-cproute-read-foundation-evidence controlplane-cproute-time-evidence controlplane-cproute-native-evidence controlplane-cptopo-evidence replayer-differential-evidence dataplane-differential dataplane-differential-coverage dataplane-differential-mutation dataplane-integration dataplane-t2-integration dataplane-t3-integration dataplane-t4-integration dataplane-t4-qualification dataplane-integration-go dataplane-integration-self-test

default: cmd

dev: build lint test

cmd: $(EXECUTABLE_TARGETS)

cmd_%: OUTPUT=$(patsubst cmd_%,./bin/%,$@)
cmd_%: SOURCE=$(patsubst cmd_%,./cmd/%,$@)
cmd_%:
	$(GO) build $(BUILDFLAGS) -o $(OUTPUT) $(SOURCE)

golangci-lint:
	GOBIN=$(GOBIN) GOTOOLCHAIN=go1.25.12 $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8

go-header:
	GOBIN=$(GOBIN) $(GO) install github.com/denis-tingaikin/go-header/cmd/go-header@latest

header: go-header
	NEW_GO_FILES=$(git diff --cached --diff-filter=A --name-only | grep -E '.*\.go')
	[ ! $(NEW_GO_FILES) ] || $(GOBIN)/go-header $(NEW_GO_FILES)

lint: golangci-lint tidy header
	cd lib && $(GOBIN)/golangci-lint run -c ../.golangci.yaml
	$(GOBIN)/golangci-lint run -c .golangci.yaml

gocovmerge:
	GOBIN=$(GOBIN) $(GO) install github.com/djshow832/gocovmerge@master

tidy:
	cd lib && $(GO) mod tidy
	$(GO) mod tidy

build:
	cd lib && $(GO) build ./...
	$(GO) build ./...

rust-build:
	$(RUST_BUILD_ENV) $(CARGO) build --locked --workspace --manifest-path $(RUST_MANIFEST)

rust-test:
	$(RUST_BUILD_ENV) $(CARGO) test --locked --workspace --manifest-path $(RUST_MANIFEST)

rust-doc-test:
	$(RUST_BUILD_ENV) $(CARGO) test --locked --workspace --doc --manifest-path $(RUST_MANIFEST)

rust-lint:
	$(CARGO) fmt --all --manifest-path $(RUST_MANIFEST) -- --check
	$(RUST_BUILD_ENV) $(CARGO) clippy --locked --workspace --all-targets --all-features --manifest-path $(RUST_MANIFEST) -- -D warnings

rust-release:
	$(RUST_BUILD_ENV) $(CARGO) build --locked --workspace --release --target $(RUST_TARGET) --manifest-path $(RUST_MANIFEST)

rust-install-tools:
	$(CARGO) install --locked --root $(RUST_TOOL_ROOT) cargo-audit --version $(CARGO_AUDIT_VERSION)
	$(CARGO) install --locked --root $(RUST_TOOL_ROOT) cargo-deny --version $(CARGO_DENY_VERSION)

rust-supply-chain:
	PATH=$(RUST_TOOL_BIN):$(PATH) rust/ci/check-supply-chain.sh

rust-negative-tests:
	PATH=$(RUST_TOOL_BIN):$(PATH) rust/ci/run-negative-tests.sh

control-proto-generate:
	./scripts/generate-control-proto.sh

control-proto-generate-check:
	./scripts/generate-control-proto.sh
	git diff --exit-code -- pkg/controlbridge/pb/control.pb.go rust/crates/control-proto/src/generated proto/dataplane/v1/testdata

controlplane-contracts:
	$(GO) run ./tests/controlplane/differential/cmd/controlplane-differential -mode validate

CONTROLPLANE_BASELINE ?=
CONTROLPLANE_CANDIDATE ?=

controlplane-differential:
	$(GO) run ./tests/controlplane/differential/cmd/controlplane-differential -mode compare -baseline "$(CONTROLPLANE_BASELINE)" -candidate "$(CONTROLPLANE_CANDIDATE)"

controlplane-differential-self-test:
	$(GO) run ./tests/controlplane/differential/cmd/controlplane-differential -mode self-test

# Paired observations come from the production Go ConfigManager and the actual
# Rust control-plane crate. The second Rust run deliberately mutates one public
# owner-generation observation and must be rejected by the exact comparator.
controlplane-cp001-evidence:
	@set -eu; \
	tmp_dir=$$(mktemp -d); \
	trap 'rm -rf "$$tmp_dir"' EXIT INT TERM; \
	$(GO) run ./tests/controlplane/cp001/go-observer > "$$tmp_dir/go.json"; \
	$(RUST_BUILD_ENV) $(CARGO) run --locked --quiet --manifest-path $(RUST_MANIFEST) -p control-plane --example cp001_observer > "$$tmp_dir/rust.json"; \
	$(GO) run ./tests/controlplane/differential/cmd/controlplane-differential -mode compare -baseline "$$tmp_dir/go.json" -candidate "$$tmp_dir/rust.json"; \
	CP001_MUTATE_OWNER_GENERATION=1 $(RUST_BUILD_ENV) $(CARGO) run --locked --quiet --manifest-path $(RUST_MANIFEST) -p control-plane --example cp001_observer > "$$tmp_dir/rust-mutated.json"; \
	set +e; \
	$(GO) run ./tests/controlplane/differential/cmd/controlplane-differential -mode compare -baseline "$$tmp_dir/go.json" -candidate "$$tmp_dir/rust-mutated.json"; \
	status=$$?; \
	set -e; \
	if [ "$$status" -ne 1 ]; then echo "CP-001 owner-generation mutation was not killed (status=$$status)" >&2; exit 1; fi

controlplane-cpadmin-evidence:
	GO=$(GO) CARGO=$(CARGO) $(RUST_BUILD_ENV) ./tests/controlplane/cpadmin/run.sh

controlplane-cpdiag-evidence:
	GO=$(GO) CARGO=$(CARGO) $(RUST_BUILD_ENV) ./tests/controlplane/cpdiag/run.sh

controlplane-cpctl-evidence:
	GO=$(GO) CARGO=$(CARGO) $(RUST_BUILD_ENV) ./tests/controlplane/cpctl/run.sh

controlplane-cp002-evidence:
	$(RUST_BUILD_ENV) tests/controlplane/cp002/run.sh

controlplane-cp003-evidence:
	$(RUST_BUILD_ENV) tests/controlplane/cp003/run.sh

controlplane-cp003-authority-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cp003/authority.sh

controlplane-cp004-evidence:
	$(RUST_BUILD_ENV) tests/controlplane/cp004/run.sh

controlplane-cproute-evidence:
	$(RUST_BUILD_ENV) tests/controlplane/cproute/run.sh

controlplane-cpmetrics-evidence:
	bash tests/controlplane/cpmetrics/run.sh

controlplane-cpmetrics-applied-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cpmetrics/applied.sh

controlplane-cpmetrics-collector-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cpmetrics/collector.sh

controlplane-cpmetrics-factor-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cpmetrics/factors/run.sh

controlplane-cproute-resource-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/resource/run.sh

controlplane-cproute-recorder-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/shadow/live-run.sh

controlplane-cproute-read-foundation-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/shadow/read-foundation-run.sh

controlplane-cproute-time-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/shadow/time-run.sh

controlplane-cproute-native-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/shadow/native-run.sh

controlplane-cproute-shadow-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/shadow/run.sh

controlplane-cproute-worker-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/worker/run.sh

controlplane-cproute-balance-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/balance/run.sh

controlplane-cproute-migration-evidence:
	$(RUST_BUILD_ENV) bash tests/controlplane/cproute/migration/run.sh

controlplane-cptopo-evidence:
	$(RUST_BUILD_ENV) tests/controlplane/cptopo/run.sh

replayer-differential-evidence:
	$(RUST_BUILD_ENV) tests/replayer/differential/run.sh

DIFFERENTIAL_SHARD_INDEX ?= 0
DIFFERENTIAL_SHARD_COUNT ?= 4

dataplane-differential:
	$(GO) run ./tests/dataplane/differential/cmd/differential -mode compare -shard-index $(DIFFERENTIAL_SHARD_INDEX) -shard-count $(DIFFERENTIAL_SHARD_COUNT)

dataplane-differential-coverage:
	$(GO) run ./tests/dataplane/differential/cmd/differential -mode coverage

dataplane-differential-mutation:
	$(GO) run ./tests/dataplane/differential/cmd/differential -mode mutation-self-check -shard-index 0 -shard-count $(DIFFERENTIAL_SHARD_COUNT)

# The default remains the intended Rust topology and fails its capability
# preflight until the real bridge/dataplane exists. The Go target is a truthful
# end-to-end baseline for the shared topology infrastructure.
dataplane-integration:
	./tests/dataplane/integration/run.sh --mode rust --variant all

# T2's local-route/lease/NamespaceMissing regression is intentionally separate
# from the later T4 namespace matrix. CI invokes this public target directly;
# callers never need to know the script's internal focused switch.
dataplane-t2-integration:
	DATAPLANE_T2_FOCUSED=1 ./tests/dataplane/integration/run.sh --mode rust --variant plain

# T3's production migration/force-close gate deliberately severs the legacy
# bridge after a local session is active. It remains separate from T4's full
# ownership/tombstone matrix.
dataplane-t3-integration:
	DATAPLANE_T3_FOCUSED=1 ./tests/dataplane/integration/run.sh --mode rust --variant plain

# T4's capability fence starts through a compatible cap6 bridge, replaces it
# with an incomplete Go peer, and proves Rust-local admission remains live.
dataplane-t4-integration:
	DATAPLANE_T4_FOCUSED=1 ./tests/dataplane/integration/run.sh --mode rust --variant plain

# Immutable, first-failure-stop 36-logical/48-physical CP-ROUTE recorder.
dataplane-t4-qualification:
	./tests/dataplane/integration/qualify-route-owner.sh

dataplane-integration-go:
	./tests/dataplane/integration/run.sh --mode go --variant all

controlplane-cplog-evidence:
	GO=$(GO) CARGO=$(CARGO) ./tests/controlplane/cplog/run.sh

dataplane-metrics-parity:
	go run ./tests/dataplane/metrics/gen > /tmp/tiproxy-metrics-parity-expected.txt
	diff -u tests/dataplane/metrics/parity-expected.txt /tmp/tiproxy-metrics-parity-expected.txt

dataplane-integration-self-test:
	./tests/dataplane/integration/self-test.sh

metrics:
	$(GO) install github.com/google/go-jsonnet/cmd/jsonnet@latest
	[ -e "grafonnet-lib" ] || git clone --depth=1 https://github.com/grafana/grafonnet-lib
	JSONNET_PATH=grafonnet-lib jsonnet ./pkg/metrics/grafana/tiproxy_summary.jsonnet > ./pkg/metrics/grafana/tiproxy_summary.json

test: gocovmerge
	rm -f .cover.*
	$(GO) test -coverprofile=.cover.pkg ./...
	cd lib && $(GO) test -coverprofile=../.cover.lib ./...
	$(GOBIN)/gocovmerge .cover.* > coverage.dat
	$(GO) tool cover -func=coverage.dat -o .cover.func
	tail -1 .cover.func
	rm -f .cover.*
#	$(GO) tool cover -html=.cover -o .cover.html

PARITY_DRIFT_BASE ?= origin/main
PARITY_DRIFT_HEAD ?= HEAD

parity-drift:
	$(GO) run ./tests/dataplane/drift/cmd/drift -mode check -base "$(PARITY_DRIFT_BASE)" -head "$(PARITY_DRIFT_HEAD)"

parity-drift-weekly:
	tests/dataplane/drift/report-weekly.sh

clean:
	rm -rf bin dist grafonnet-lib rust/target

docker:
	docker build -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg "GOPROXY=$(shell $(GO) env GOPROXY)" --build-arg "VERSION=$(VERSION)" --build-arg "COMMIT=$(COMMIT)" --build-arg "BRANCH=$(BRANCH)" -f docker/Dockerfile .

docker-release:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg "GOPROXY=$(shell $(GO) env GOPROXY)" --build-arg "VERSION=$(VERSION)" --build-arg "COMMIT=$(COMMIT)" --build-arg "BRANCH=$(BRANCH)" -f docker/Dockerfile .

controlplane-cpmeter-evidence:
	GO=$(GO) CARGO=$(CARGO) ./tests/controlplane/cpmeter/run.sh
