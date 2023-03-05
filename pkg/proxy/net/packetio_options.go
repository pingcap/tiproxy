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
)

type PacketIOption = func(*PacketIO)

func WithProxy(pi *PacketIO) {
	pi.proxyInited.Store(true)
}

func WithWrapError(err error) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.wrap = err
	}
}

// WithRemoteAddr
var _ net.Addr = &originAddr{}

type originAddr struct {
	net.Addr
	addr string
}

func (o *originAddr) String() string {
	return o.addr
}

func WithRemoteAddr(readdr string, addr net.Addr) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.remoteAddr = &originAddr{Addr: addr, addr: readdr}
	}
}

func unwrapOriginAddr(addr net.Addr) net.Addr {
	if oaddr, ok := addr.(*originAddr); ok {
		return oaddr.Addr
	}
	return addr
}
