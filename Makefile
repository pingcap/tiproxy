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
DEBUG ?=
DOCKERPREFIX ?=
BUILD_TAGS ?=
LDFLAGS ?= 
BUILDFLAGS := $(BUILDFLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}'
ifneq ("$(DEBUG)", "")
	BUILDFLAGS += -race
endif
IMAGE_TAG ?= latest
EXECUTABLE_TARGETS := $(patsubst cmd/%,cmd_%,$(wildcard cmd/*))

.PHONY: cmd_% test lint docker docker-release golangci-lint gocovmerge

default: cmd

dev: cmd lint test

cache: build lint test

cmd: $(EXECUTABLE_TARGETS)

cmd_%: OUTPUT=$(patsubst cmd_%,./bin/%,$@)
cmd_%: SOURCE=$(patsubst cmd_%,./cmd/%,$@)
cmd_%:
	go build $(BUILDFLAGS) -o $(OUTPUT) $(SOURCE)

golangci-lint:
	GOBIN=$(GOBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

lint: golangci-lint
	$(GOBIN)/golangci-lint run
	cd lib && $(GOBIN)/golangci-lint run

gocovmerge:
	GOBIN=$(GOBIN) go install github.com/wadey/gocovmerge@master

test: gocovmerge
	rm -f .cover.*
	go test -coverprofile=.cover.pkg ./...
	cd lib && go test -coverprofile=../.cover.lib ./...
	$(GOBIN)/gocovmerge .cover.* > .cover
	go tool cover -func=.cover -o .cover.func
	tail -1 .cover.func
	rm -f .cover.*
	go tool cover -html=.cover -o .cover.html

docker:
	docker build -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg 'GOPROXY=$(shell go env GOPROXY),BUILDFLAGS=$(BUILDFLAGS),' -f docker/Dockerfile .

docker-release:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t "$(DOCKERPREFIX)tiproxy:$(IMAGE_TAG)" --build-arg 'GOPROXY=$(shell go env GOPROXY),BUILDFLAGS=$(BUILDFLAGS),' -f docker/Dockerfile .
