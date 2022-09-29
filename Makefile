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
GOBIN := $(shell pwd)/bin
DOCKERFLAG ?=
RELEASE ?=
ifneq ($(RELEASE), "")
	DOCKERFLAG ?= --squash
endif
BUILD_TAGS ?=
LDFLAGS ?= 
DEBUG ?=
BUILDFLAGS := $(BUILDFLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}'
ifeq ("$(WITH_RACE)", "1")
	BUILDFLAGS += -race
endif
IMAGE_TAG ?= latest
EXECUTABLE_TARGETS := $(patsubst cmd/%,cmd_%,$(wildcard cmd/*))

.PHONY: cmd_% build test docker ./bin/golangci-lint ./bin/gocovmerge

default: cmd

cmd: $(EXECUTABLE_TARGETS)

build: cmd
	go build $(BUILDFLAGS) ./...
	cd lib && go build $(BUILDFLAGS) ./...

cmd_%: OUTPUT=$(patsubst cmd_%,./bin/%,$@)
cmd_%: SOURCE=$(patsubst cmd_%,./cmd/%,$@)
cmd_%:
	go build $(BUILDFLAGS) -o $(OUTPUT) $(SOURCE)

lint: ./bin/golangci-lint
	$(GOBIN)/golangci-lint run
	cd lib && $(GOBIN)/golangci-lint run

test: ./bin/gocovmerge
	rm -f .cover.*
	go test -coverprofile=.cover.pkg ./...
	cd lib && go test -coverprofile=../.cover.lib ./...
	$(GOBIN)/gocovmerge .cover.* > .cover
	rm -f .cover.*
	go tool cover -html=.cover -o .cover.html

./bin/golangci-lint:
	GOBIN=$(GOBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

./bin/gocovmerge:
	GOBIN=$(GOBIN) go install github.com/wadey/gocovmerge@master

docker:
	docker build $(DOCKERFLAG) -t "tiproxy:${IMAGE_TAG}" --build-arg='GOPROXY=$(shell go env GOPROXY),BUILDFLAGS=$(BUILDFLAGS),' -f docker/Dockerfile .
