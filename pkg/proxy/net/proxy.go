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
	"bytes"
	"io"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/pkg/proxy/proxyprotocol"
)

func (p *PacketIO) parseProxyV2() (*proxyprotocol.Proxy, error) {
	rem, err := p.buf.Peek(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}
	if !bytes.Equal(rem, proxyprotocol.MagicV2[4:]) {
		return nil, nil
	}

	// yes, it is proxyV2
	_, err = p.buf.Discard(8)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}
	p.inBytes += 8

	m, n, err := proxyprotocol.ParseProxyV2(p.buf)
	p.inBytes += uint64(n)
	if err == nil {
		// set RemoteAddr in case of proxy.
		p.remoteAddr = m.SrcAddress
	}
	return m, err
}

// WriteProxyV2 should only be called at the beginning of connection, before any write operations.
func (p *PacketIO) WriteProxyV2(m *proxyprotocol.Proxy) error {
	buf, err := m.ToBytes()
	if err != nil {
		return errors.Wrap(ErrWriteConn, err)
	}
	if _, err := io.Copy(p.buf, bytes.NewReader(buf)); err != nil {
		return errors.Wrap(ErrWriteConn, err)
	}
	p.outBytes += uint64(len(buf))
	// according to the spec, we better flush to avoid server hanging
	return p.Flush()
}
