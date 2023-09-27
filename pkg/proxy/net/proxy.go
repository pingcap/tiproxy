// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
)

func (p *PacketIO) EnableProxyClient(proxy *proxyprotocol.Proxy) {
	p.readWriter = newProxyClient(p.readWriter, proxy)
}

func (p *PacketIO) EnableProxyServer() {
	p.readWriter = newProxyServer(p.readWriter)
}

// Proxy returned parsed proxy header from clients if any.
func (p *PacketIO) Proxy() *proxyprotocol.Proxy {
	return p.readWriter.Proxy()
}

var _ packetReadWriter = (*proxyReadWriter)(nil)

type proxyReadWriter struct {
	packetReadWriter
	proxyInited atomic.Bool
	proxy       *proxyprotocol.Proxy
	addr        net.Addr
	client      bool
}

func newProxyClient(rw packetReadWriter, proxy *proxyprotocol.Proxy) *proxyReadWriter {
	prw := &proxyReadWriter{
		packetReadWriter: rw,
		proxy:            proxy,
		client:           true,
	}
	return prw
}

func newProxyServer(rw packetReadWriter) *proxyReadWriter {
	prw := &proxyReadWriter{
		packetReadWriter: rw,
		client:           false,
	}
	return prw
}

func (prw *proxyReadWriter) Read(b []byte) (int, error) {
	// probe proxy V2
	if !prw.client && !prw.proxyInited.Load() {
		// We don't know whether the client has enabled proxy protocol.
		// If it doesn't, reading data of len(MagicV2) may block forever.
		header, err := prw.Peek(4)
		if err != nil {
			return 0, errors.Wrap(ErrReadConn, err)
		}
		if bytes.Equal(header[:], proxyprotocol.MagicV2[:4]) {
			if _, err = prw.Discard(4); err != nil {
				return 0, err
			}
			proxyHeader, err := prw.parseProxyV2()
			if err != nil {
				return 0, errors.Wrap(ErrReadConn, err)
			}
			if proxyHeader != nil {
				prw.proxy = proxyHeader
			}
		}
		prw.proxyInited.Store(true)
	}
	return prw.packetReadWriter.Read(b)
}

func (prw *proxyReadWriter) Write(p []byte) (n int, err error) {
	// The proxy header should be written at the beginning of connection, before any write operations.
	if prw.client && !prw.proxyInited.Load() {
		buf, err := prw.proxy.ToBytes()
		if err != nil {
			return 0, errors.Wrap(ErrWriteConn, err)
		}
		if _, err := io.Copy(prw.packetReadWriter, bytes.NewReader(buf)); err != nil {
			return 0, errors.Wrap(ErrWriteConn, err)
		}
		// according to the spec, we better flush to avoid server hanging
		if err := prw.packetReadWriter.Flush(); err != nil {
			return 0, err
		}
		prw.proxyInited.Store(true)
	}
	return prw.packetReadWriter.Write(p)
}

func (prw *proxyReadWriter) parseProxyV2() (*proxyprotocol.Proxy, error) {
	rem, err := prw.packetReadWriter.Peek(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}
	if !bytes.Equal(rem, proxyprotocol.MagicV2[4:]) {
		return nil, nil
	}

	// yes, it is proxyV2
	_, err = prw.packetReadWriter.Discard(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}

	m, _, err := proxyprotocol.ParseProxyV2(prw.packetReadWriter)
	if err == nil {
		// set RemoteAddr in case of proxy.
		prw.addr = m.SrcAddress
	}
	return m, err
}

func (prw *proxyReadWriter) RemoteAddr() net.Addr {
	if prw.addr != nil {
		return prw.addr
	}
	return prw.packetReadWriter.RemoteAddr()
}

func (prw *proxyReadWriter) Proxy() *proxyprotocol.Proxy {
	return prw.proxy
}
