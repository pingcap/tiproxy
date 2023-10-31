// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bufio"
	"crypto/tls"
	"io"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

// tlsHandshakeConn is only used as the underlying connection in tls.Conn.
// TLS handshake must read from the buffered reader because the handshake data may be already buffered in the reader.
// TLS handshake can not use the buffered writer directly because it assumes the data will be flushed automatically,
// however buffered writer may not flush without calling `Flush`.
type tlsInternalConn struct {
	packetReadWriter
}

func (br *tlsInternalConn) Write(p []byte) (n int, err error) {
	return br.packetReadWriter.DirectWrite(p)
}

func (p *PacketIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsInternalConn{p.readWriter}
	tlsConn := tls.Server(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return tls.ConnectionState{}, p.wrapErr(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn)
	return tlsConn.ConnectionState(), nil
}

func (p *PacketIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsInternalConn{p.readWriter}
	tlsConn := tls.Client(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn)
	return nil
}

func (p *PacketIO) TLSConnectionState() tls.ConnectionState {
	return p.readWriter.TLSConnectionState()
}

var _ packetReadWriter = (*tlsReadWriter)(nil)

type tlsReadWriter struct {
	packetReadWriter
	buf  *bufio.ReadWriter
	conn *tls.Conn
}

func newTLSReadWriter(rw packetReadWriter, tlsConn *tls.Conn) *tlsReadWriter {
	// Can not modify rw and reuse it because tlsConn is using rw internally.
	// We must create another buffer.
	buf := bufio.NewReadWriter(bufio.NewReaderSize(tlsConn, DefaultConnBufferSize), bufio.NewWriterSize(tlsConn, DefaultConnBufferSize))
	return &tlsReadWriter{
		packetReadWriter: rw,
		buf:              buf,
		conn:             tlsConn,
	}
}

func (trw *tlsReadWriter) Read(b []byte) (n int, err error) {
	// inBytes and outBytes are updated internally in trw.packetReadWriter.
	return trw.buf.Read(b)
}

func (trw *tlsReadWriter) ReadFrom(r io.Reader) (int64, error) {
	return trw.buf.ReadFrom(r)
}

func (trw *tlsReadWriter) Write(p []byte) (int, error) {
	return trw.buf.Write(p)
}

func (trw *tlsReadWriter) DirectWrite(p []byte) (int, error) {
	return trw.conn.Write(p)
}

func (trw *tlsReadWriter) Peek(n int) ([]byte, error) {
	return trw.buf.Peek(n)
}

func (trw *tlsReadWriter) Discard(n int) (int, error) {
	return trw.buf.Discard(n)
}

func (trw *tlsReadWriter) Flush() error {
	return trw.buf.Flush()
}

func (trw *tlsReadWriter) TLSConnectionState() tls.ConnectionState {
	return trw.conn.ConnectionState()
}
