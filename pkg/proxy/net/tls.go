// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bufio"
	"crypto/tls"

	"github.com/pingcap/TiProxy/lib/util/errors"
)

func (p *PacketIO) ServerTLSHandshake(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	tlsConfig = tlsConfig.Clone()
	tlsConn := tls.Server(p.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return tls.ConnectionState{}, p.wrapErr(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.conn = tlsConn
	p.buf.Writer.Reset(tlsConn)
	// Wrap it with another buffer to enable Peek.
	p.buf = bufio.NewReadWriter(bufio.NewReaderSize(tlsConn, defaultReaderSize), p.buf.Writer)
	return tlsConn.ConnectionState(), nil
}

func (p *PacketIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	tlsConfig = tlsConfig.Clone()
	tlsConn := tls.Client(p.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.conn = tlsConn
	p.buf.Writer.Reset(tlsConn)
	// Wrap it with another buffer to enable Peek.
	p.buf = bufio.NewReadWriter(bufio.NewReaderSize(tlsConn, defaultReaderSize), p.buf.Writer)
	return nil
}
