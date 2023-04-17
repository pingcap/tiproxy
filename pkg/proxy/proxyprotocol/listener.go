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
	"io"
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
	return &proxyConn{Conn: conn}, err
}

type proxyConn struct {
	net.Conn
	proxyHdr []byte
	proxy    *Proxy
	inited   bool
}

func (c *proxyConn) Read(b []byte) (n int, err error) {
	if !c.inited {
		if len(c.proxyHdr) == 0 {
			c.proxyHdr = make([]byte, len(MagicV2))
		}
		n, err = io.ReadFull(c.Conn, c.proxyHdr)
		if err != nil {
			return
		}
		if bytes.Equal(c.proxyHdr, MagicV2) {
			c.proxyHdr = nil
			c.proxy, _, err = ParseProxyV2(c.Conn)
			if err != nil {
				return 0, err
			}
		}
		c.inited = true
	}
	if len(c.proxyHdr) > 0 {
		n = copy(b, c.proxyHdr)
		c.proxyHdr = c.proxyHdr[n:]
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
