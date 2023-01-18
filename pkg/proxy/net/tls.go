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
	p.buf.Writer.Reset(p.conn)
	// Wrap it with another buffer to enable Peek.
	p.buf = bufio.NewReadWriter(bufio.NewReaderSize(p.conn, defaultReaderSize), p.buf.Writer)
	return tlsConn.ConnectionState(), nil
}

func (p *PacketIO) ClientTLSHandshake(tlsConfig *tls.Config) error {
	tlsConfig = tlsConfig.Clone()
	tlsConn := tls.Client(p.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.conn = tlsConn
	p.buf.Writer.Reset(p.conn)
	// Wrap it with another buffer to enable Peek.
	p.buf = bufio.NewReadWriter(bufio.NewReaderSize(p.conn, defaultReaderSize), p.buf.Writer)
	return nil
}
