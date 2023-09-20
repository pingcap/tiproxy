// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bufio"
	"crypto/tls"
	"net"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
)

// tlsHandshakeConn is only used for TLS handshake.
// TLS handshake must read from the buffered reader because the handshake data may be already buffered in the reader.
// TLS handshake can not use the buffered writer directly because it assumes the data is always flushed immediately.
type tlsHandshakeConn struct {
	packetReadWriter
}

func (br *tlsHandshakeConn) Write(p []byte) (n int, err error) {
	if n, err = br.packetReadWriter.Write(p); err != nil {
		return
	}
	return n, br.packetReadWriter.Flush()
}

func (p *PacketIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsHandshakeConn{p.readWriter}
	tlsConn := tls.Server(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return tls.ConnectionState{}, p.wrapErr(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn)
	return tlsConn.ConnectionState(), nil
}

func (p *PacketIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsHandshakeConn{p.readWriter}
	tlsConn := tls.Client(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn)
	return nil
}

func (p *PacketIO) TLSConnectionState() tls.ConnectionState {
	return p.readWriter.tlsConnectionState()
}

var _ packetReadWriter = (*tlsReadWriter)(nil)

type tlsReadWriter struct {
	rdbufConn
	rw packetReadWriter
}

func newTLSReadWriter(rw packetReadWriter, tlsConn net.Conn) *tlsReadWriter {
	// Can not modify rw and reuse it because tlsConn is using rw internally.
	// So we must create another buffer.
	buf := bufio.NewReadWriter(bufio.NewReaderSize(tlsConn, defaultReaderSize), bufio.NewWriterSize(tlsConn, defaultWriterSize))
	return &tlsReadWriter{
		rdbufConn: rdbufConn{
			Conn:       tlsConn,
			ReadWriter: buf,
			inBytes:    rw.getInBytes(),
			outBytes:   rw.getOutBytes(),
		},
		rw: rw,
	}
}

func (trw *tlsReadWriter) afterRead() {
	trw.rw.afterRead()
}

func (trw *tlsReadWriter) reset() {
	trw.rw.reset()
}

func (trw *tlsReadWriter) getProxy() *proxyprotocol.Proxy {
	return trw.rw.getProxy()
}
