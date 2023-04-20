// Copyright 2023 PingCAP, Inc.
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

package proxyprotocol

import (
	"bytes"
	"net"
)

var _ net.Listener = (*Listener)(nil)
var _ net.Conn = (*proxyConn)(nil)

type Listener struct {
	net.Listener
}

func NewListener(o net.Listener) *Listener {
	return &Listener{o}
}

func (n *Listener) Accept() (net.Conn, error) {
	conn, err := n.Listener.Accept()
	return &proxyConn{Conn: conn, buf: new(bytes.Buffer)}, err
}

type proxyConn struct {
	net.Conn
	buf    *bytes.Buffer
	proxy  *Proxy
	inited bool
}

func (c *proxyConn) Read(b []byte) (n int, err error) {
	if !c.inited {
		_, err = c.buf.ReadFrom(c.Conn)
		if err != nil {
			return
		}
		if bytes.HasPrefix(MagicV2, c.buf.Bytes()) {
			if !bytes.Equal(MagicV2, c.buf.Bytes()) {
				// prefix matches, maybe proxy header
				// read again later
				return 0, nil
			}
			// it is proxy protocol
			c.buf = nil
			c.proxy, _, err = ParseProxyV2(c.Conn)
			if err != nil {
				return 0, err
			}
		}
		// prefixes mismatched
		c.inited = true
	}
	if c.buf.Len() > 0 {
		n = copy(b, c.buf.Bytes())
		_ = c.buf.Next(n)
		return n, nil
	}
	return c.Conn.Read(b)
}

func (c *proxyConn) RemoteAddr() net.Addr {
	if c.proxy != nil && c.proxy.DstAddress != nil {
		return c.proxy.DstAddress
	}
	return c.Conn.RemoteAddr()
}
