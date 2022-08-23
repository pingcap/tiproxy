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
BUILD_TAGS ?=
LDFLAGS ?= 
GOFLAGS ?= -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}'
ifeq ("$(WITH_RACE)", "1")
	GOFLAGS = $(GOFLAGS) -race
endif
EXECUTABLE_TARGETS := $(patsubst cmd/%,cmd_%,$(wildcard cmd/*))

default: cmd

cmd: $(EXECUTABLE_TARGETS)

build: cmd
	go build $(GOFLAGS) ./...

cmd_%: OUTPUT=$(patsubst cmd_%,bin/%,$@)
cmd_%: SOURCE=$(patsubst cmd_%,cmd/%/main.go,$@)
cmd_%:
	go build $(GOFLAGS) -o $(OUTPUT) $(SOURCE)

test:
	go test -coverprofile=.coverage.out ./...
	go tool cover -func=.coverage.out -o .coverage.func
	tail -1 .coverage.func
	go tool cover -html=.coverage.out -o .coverage.html
