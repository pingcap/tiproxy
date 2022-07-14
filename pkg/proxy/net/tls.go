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
	"crypto/tls"
	"net"

	"github.com/pingcap/TiProxy/pkg/util/errors"
)

func (p *PacketIO) UpgradeToServerTLS(tlsConfig *tls.Config) (tls.ConnectionState, error) {
	tlsConn := tls.Server(p.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return tlsConn.ConnectionState(), errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.buf.Reader.Reset(tlsConn)
	p.buf.Writer.Reset(tlsConn)
	return tlsConn.ConnectionState(), nil
}

func (p *PacketIO) UpgradeToClientTLS(tlsConfig *tls.Config) error {
	host, _, err := net.SplitHostPort(p.conn.RemoteAddr().String())
	if err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	tlsConfig = tlsConfig.Clone()
	tlsConfig.ServerName = host
	tlsConn := tls.Client(p.conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return errors.WithStack(errors.Wrap(ErrHandshakeTLS, err))
	}
	p.buf.Reader.Reset(tlsConn)
	p.buf.Writer.Reset(tlsConn)
	return nil
}
