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

GOBIN := $(shell pwd)/bin
VERSION ?= $(shell git describe --tags --dirty --always)
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
COMMIT ?= $(shell git describe --match=NeVeRmAtCh --always --abbrev=40 --dirty)
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

.PHONY: cmd_% test lint docker docker-release golangci-lint gocovmerge clean

default: cmd

dev: build lint test

cmd: $(EXECUTABLE_TARGETS)

cmd_%: OUTPUT=$(patsubst cmd_%,./bin/%,$@)
cmd_%: SOURCE=$(patsubst cmd_%,./cmd/%,$@)
cmd_%:
	go build $(BUILDFLAGS) -o $(OUTPUT) $(SOURCE)

golangci-lint:
	GOBIN=$(GOBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.1

go-header:
	GOBIN=$(GOBIN) go install github.com/denis-tingaikin/go-header/cmd/go-header@latest

header: go-header
	NEW_GO_FILES=$(git diff --cached --diff-filter=A --name-only | grep -E '.*\.go')
	[ ! $(NEW_GO_FILES) ] || $(GOBIN)/go-header $(NEW_GO_FILES)

lint: golangci-lint tidy header
	cd lib && $(GOBIN)/golangci-lint run -c ../.golangci.yaml
	$(GOBIN)/golangci-lint run -c .golangci.yaml

gocovmerge:
	GOBIN=$(GOBIN) go install github.com/wadey/gocovmerge@master

tidy:
	cd lib && go mod tidy
	go mod tidy

build:
	cd lib && go build ./...
	go build ./...

metrics:
	go install github.com/google/go-jsonnet/cmd/jsonnet@latest
	[ -e "grafonnet-lib" ] || git clone --depth=1 https://github.com/grafana/grafonnet-lib
	JSONNET_PATH=grafonnet-lib jsonnet ./pkg/metrics/grafana/tiproxy_summary.jsonnet > ./pkg/metrics/grafana/tiproxy_summary.json

test: gocovmerge
	rm -f .cover.*
	go test -race -coverprofile=.cover.pkg ./...
	cd lib && go test -race -coverprofile=../.cover.lib ./...
	$(GOBIN)/gocovmerge .cover.* > .cover
	go tool cover -func=.cover -o .cover.func
	tail -1 .cover.func
	rm -f .cover.*
	go tool cover -html=.cover -o .cover.html

clean:
	rm -rf bin dist grafonnet-lib

docker:
	docker build -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg "GOPROXY=$(shell go env GOPROXY)" --build-arg "VERSION=$(VERSION)" --build-arg "COMMIT=$(COMMIT)" --build-arg "BRANCH=$(BRANCH)" -f docker/Dockerfile .

docker-release:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg "GOPROXY=$(shell go env GOPROXY)" --build-arg "VERSION=$(VERSION)" --build-arg "COMMIT=$(COMMIT)" --build-arg "BRANCH=$(BRANCH)" -f docker/Dockerfile .
