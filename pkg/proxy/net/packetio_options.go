// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net

import (
	"net"

	"go.uber.org/atomic"
)

type PacketIOption = func(*PacketIO)

func WithProxy(pi *PacketIO) {
	pi.proxyInited = atomic.NewBool(true)
}

func WithWrapError(err error) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.wrap = err
	}
}

// WithRemoteAddr
var _ net.Addr = &oriRemoteAddr{}

type oriRemoteAddr struct {
	addr string
}

func (o *oriRemoteAddr) Network() string {
	return "tcp"
}

func (o *oriRemoteAddr) String() string {
	return o.addr
}

func WithRemoteAddr(readdr string) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.remoteAddr = &oriRemoteAddr{addr: readdr}
	}
}
