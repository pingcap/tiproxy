// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"net"

	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
)

type PacketIOption = func(*PacketIO)

func WithProxy(pi *PacketIO) {
	pi.proxyInited.Store(false)
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
