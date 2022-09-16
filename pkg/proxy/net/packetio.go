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

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package net

import (
	"bufio"
	"bytes"
	"io"
	"net"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
	"go.uber.org/atomic"
)

var (
	errInvalidSequence = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)

	proxyV2Magic = []byte{0xD, 0xA, 0xD, 0xA, 0x0, 0xD, 0xA, 0x51, 0x55, 0x49, 0x54, 0xA}
)

const (
	defaultWriterSize = 16 * 1024
	defaultReaderSize = 16 * 1024
)

// rdbufConn will buffer read for non-TLS connections.
// While TLS connections have internal buffering, we still need to pass *rdbufConn to `tls.XXX()`.
// Because TLS handshake data may already be buffered in `*rdbufConn`.
// TODO: only enable writer buffering for TLS connections, otherwise enable read/write buffering.
type rdbufConn struct {
	net.Conn
	*bufio.Reader
}

func (f *rdbufConn) Read(b []byte) (int, error) {
	return f.Reader.Read(b)
}

// PacketIO is a helper to read and write sql and proxy protocol.
type PacketIO struct {
	conn        net.Conn
	buf         *bufio.ReadWriter
	sequence    uint8
	proxyInited *atomic.Bool
	proxy       *Proxy
}

func NewPacketIO(conn net.Conn) *PacketIO {
	buf := bufio.NewReadWriter(
		bufio.NewReaderSize(conn, defaultReaderSize),
		bufio.NewWriterSize(conn, defaultWriterSize),
	)
	p := &PacketIO{
		conn: &rdbufConn{
			conn,
			buf.Reader,
		},
		sequence: 0,
		// TODO: disable it by default now
		proxyInited: atomic.NewBool(true),
		buf:         buf,
	}
	return p
}

// Proxy returned parsed proxy header from clients if any.
func (p *PacketIO) Proxy() *Proxy {
	if p.proxyInited.Load() {
		return p.proxy
	}
	return nil
}

func (p *PacketIO) LocalAddr() net.Addr {
	return p.conn.LocalAddr()
}

func (p *PacketIO) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}

func (p *PacketIO) ResetSequence() {
	p.sequence = 0
}

// GetSequence is used in tests to assert that the sequences on the client and server are equal.
func (p *PacketIO) GetSequence() uint8 {
	return p.sequence
}

func (p *PacketIO) ReadOnePacket() ([]byte, bool, error) {
	var header [4]byte

	if _, err := io.ReadFull(p.conn, header[:]); err != nil {
		return nil, false, errors.Wrap(ErrReadConn, err)
	}

	// probe proxy V2
	refill := false
	if !p.proxyInited.Load() {
		if bytes.Compare(header[:], proxyV2Magic[:4]) == 0 {
			proxyHeader, err := p.parseProxyV2()
			if err != nil {
				return nil, false, errors.Wrap(ErrReadConn, err)
			}
			if proxyHeader != nil {
				p.proxy = proxyHeader
				refill = true
			}
		}
		p.proxyInited.Store(true)
	}

	// refill mysql headers
	if refill {
		if _, err := io.ReadFull(p.conn, header[:]); err != nil {
			return nil, false, errors.Wrap(ErrReadConn, err)
		}
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, false, errInvalidSequence.GenWithStack("invalid sequence %d != %d", sequence, p.sequence)
	}
	p.sequence++
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if _, err := io.ReadFull(p.conn, data); err != nil {
		return nil, false, errors.Wrap(ErrReadConn, err)
	}
	return data, length == mysql.MaxPayloadLen, nil
}

// ReadPacket reads data and removes the header
func (p *PacketIO) ReadPacket() ([]byte, error) {
	var data []byte
	for {
		buf, more, err := p.ReadOnePacket()
		if err != nil {
			return nil, err
		}

		data = append(data, buf...)

		if !more {
			break
		}
	}
	return data, nil
}

func (p *PacketIO) WriteOnePacket(data []byte) (int, bool, error) {
	more := false
	length := len(data)
	if length >= mysql.MaxPayloadLen {
		length = mysql.MaxPayloadLen
		more = true
	}

	var header [4]byte
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = p.sequence
	p.sequence++

	if _, err := io.Copy(p.buf, bytes.NewReader(header[:])); err != nil {
		return 0, more, errors.Wrap(ErrWriteConn, err)
	}

	if _, err := io.Copy(p.buf, bytes.NewReader(data[:length])); err != nil {
		return 0, more, errors.Wrap(ErrWriteConn, err)
	}

	return length, more, nil
}

// WritePacket writes data without a header
func (p *PacketIO) WritePacket(data []byte, flush bool) error {
	// The original data might be empty.
	for {
		n, more, err := p.WriteOnePacket(data)
		if err != nil {
			return err
		}
		data = data[n:]
		// if the last packet ends with a length of MaxPayloadLen exactly
		// we need another zero-length packet to end it
		if len(data) == 0 {
			if more {
				if _, _, err := p.WriteOnePacket(nil); err != nil {
					return err
				}
			}
			break
		}
	}
	if flush {
		return p.Flush()
	}
	return nil
}

func (p *PacketIO) Flush() error {
	if err := p.buf.Flush(); err != nil {
		return errors.Wrap(ErrFlushConn, err)
	}
	return nil
}

func (p *PacketIO) Close() error {
	var errs []error
	/*
		TODO: flush when we want to smoothly exit
		if err := p.Flush(); err != nil {
			errs = append(errs, err)
		}
	*/
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Collect(ErrCloseConn, errs...)
}
