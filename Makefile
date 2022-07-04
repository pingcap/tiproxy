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

PROJECTNAME = $(shell basename "$(PWD)")
TOOL_BIN_PATH := $(shell pwd)/.tools/bin
GOBASE = $(shell pwd)
BUILD_TAGS ?=
LDFLAGS ?= 
export GOBIN := $(TOOL_BIN_PATH)
export PATH := $(TOOL_BIN_PATH):$(PATH)

default: weirproxy

weirproxy:
ifeq ("$(WITH_RACE)", "1")
	go build -race -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}' -o bin/weirproxy cmd/weirproxy/main.go
else
	go build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}' -o bin/weirproxy cmd/weirproxy/main.go
endif

go-test:
	go test -coverprofile=.coverage.out ./...
	go tool cover -func=.coverage.out -o .coverage.func
	tail -1 .coverage.func
	go tool cover -html=.coverage.out -o .coverage.html

go-lint-check: install-tools
	golangci-lint run

go-lint-fix: install-tools
	golangci-lint run --fix

install-tools:
	@mkdir -p $(TOOL_BIN_PATH)
	@test -e $(TOOL_BIN_PATH)/golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOL_BIN_PATH) v1.30.0
