// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
