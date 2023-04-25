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

	"github.com/pingcap/TiProxy/pkg/proxy/proxyprotocol"
)

type PacketIOption = func(*PacketIO)

func WithProxy(pi *PacketIO) {
	pi.proxyInited.Store(true)
}

func WithOnTraffic(f func(pi *PacketIO)) func(*PacketIO) {
	return func(pi *PacketIO) {
		pi.onTraffic = f
	}
}

func WithWrapError(err error) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.wrap = err
	}
}

// WithRemoteAddr
var _ proxyprotocol.AddressWrapper = &originAddr{}

type originAddr struct {
	net.Addr
	addr string
}

func (o *originAddr) Unwrap() net.Addr {
	return o.Addr
}

func (o *originAddr) String() string {
	return o.addr
}

func WithRemoteAddr(readdr string, addr net.Addr) func(pi *PacketIO) {
	return func(pi *PacketIO) {
		pi.remoteAddr = &originAddr{Addr: addr, addr: readdr}
	}
}
