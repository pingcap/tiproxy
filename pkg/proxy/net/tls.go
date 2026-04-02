// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"crypto/tls"
	"io"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/util/bufio"
)

const (
	// The TLS layer keeps its own post-handshake buffers, so follow the caller's
	// connBufferSize proportionally but cap the sizes to avoid recreating the same
	// memory pressure as the base connection buffers.
	minTLSReadBufferSize  = 1 * 1024
	maxTLSReadBufferSize  = 4 * 1024
	minTLSWriteBufferSize = 1 * 1024
	maxTLSWriteBufferSize = 16 * 1024
	tlsReadBufferDivisor  = 4
	tlsWriteBufferDivisor = 2
)

func clampBufferSize(size, minSize, maxSize int) int {
	if size < minSize {
		return minSize
	}
	if size > maxSize {
		return maxSize
	}
	return size
}

// TLS reads mostly serve packet-header peeks after the handshake, so a smaller
// reader is usually enough. TLS writes are more sensitive to fragmentation, so
// keep the writer relatively larger. Clamp both ends so tiny custom connection
// buffers do not make TLS unusably small, and huge connection buffers do not
// reintroduce excessive per-connection TLS memory.
func tlsBufferSizes(connBufferSize int) (readSize int, writeSize int) {
	connBufferSize = normalizeConnBufferSize(connBufferSize)
	return clampBufferSize(connBufferSize/tlsReadBufferDivisor, minTLSReadBufferSize, maxTLSReadBufferSize),
		clampBufferSize(connBufferSize/tlsWriteBufferDivisor, minTLSWriteBufferSize, maxTLSWriteBufferSize)
}

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

func (p *packetIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsInternalConn{p.readWriter}
	tlsConn := tls.Server(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return tls.ConnectionState{}, p.wrapErr(errors.Wrap(errors.WithStack(err), ErrHandshakeTLS))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn, p.connBufferSize)
	return tlsConn.ConnectionState(), nil
}

func (p *packetIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	tlsConfig = tlsConfig.Clone()
	conn := &tlsInternalConn{p.readWriter}
	tlsConn := tls.Client(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return p.wrapErr(errors.Wrap(errors.WithStack(err), ErrHandshakeTLS))
	}
	p.readWriter = newTLSReadWriter(p.readWriter, tlsConn, p.connBufferSize)
	return nil
}

func (p *packetIO) TLSConnectionState() tls.ConnectionState {
	return p.readWriter.TLSConnectionState()
}

var _ packetReadWriter = (*tlsReadWriter)(nil)

type tlsReadWriter struct {
	packetReadWriter
	buf  *bufio.ReadWriter
	conn *tls.Conn
}

func newTLSReadWriter(rw packetReadWriter, tlsConn *tls.Conn, connBufferSize int) *tlsReadWriter {
	// Can not modify rw and reuse it because tlsConn is using rw internally.
	// We must create another buffer. Size it from the base connection buffer so
	// custom connBufferSize values keep a consistent memory profile after TLS.
	readBufferSize, writeBufferSize := tlsBufferSizes(connBufferSize)
	buf := bufio.NewReadWriter(
		bufio.NewReaderSize(tlsConn, readBufferSize),
		bufio.NewWriterSize(tlsConn, writeBufferSize),
	)
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
