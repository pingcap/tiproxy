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
	"fmt"
	"io"
	"net"

	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	serverVersion      = fmt.Sprintf("5.7.25-WEIR")
	utf8mb4BinID  byte = 46

	errInvalidSequence = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)

	proxyV2Magic = []byte{0xD, 0xA, 0xD, 0xA, 0x0, 0xD, 0xA, 0x51, 0x55, 0x49, 0x54, 0xA}
)

const (
	defaultWriterSize = 16 * 1024
	defaultReaderSize = 16 * 1024
)

// PacketIO is a helper to read and write sql and proxy protocol.
type PacketIO struct {
	conn     net.Conn
	tlsConn  net.Conn
	buf      *bufio.ReadWriter
	sequence uint8
	proxy    *Proxy
}

func NewPacketIO(conn net.Conn) *PacketIO {
	p := &PacketIO{
		conn:     conn,
		sequence: 0,
		buf: bufio.NewReadWriter(
			bufio.NewReaderSize(conn, defaultReaderSize),
			bufio.NewWriterSize(conn, defaultWriterSize),
		),
	}
	return p
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

func (p *PacketIO) ReadOnePacket() ([]byte, bool, error) {
	var header [4]byte
	var err error

	if _, err := io.ReadFull(p.buf, header[:]); err != nil {
		return nil, false, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}

	// probe proxy V2
	refill := false
	if p.proxy == nil && bytes.Compare(header[:], proxyV2Magic[:4]) == 0 {
		refill, err = p.parseProxyV2()
		if err != nil {
			return nil, false, errors.Wrap(ErrReadConn, err)
		}
	}

	// refill mysql headers
	if refill {
		if _, err := io.ReadFull(p.buf, header[:]); err != nil {
			return nil, false, errors.WithStack(errors.Wrap(ErrReadConn, err))
		}
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, false, errInvalidSequence.GenWithStack("invalid sequence %d != %d", sequence, p.sequence)
	}
	p.sequence++
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if _, err := io.ReadFull(p.buf, data); err != nil {
		return nil, false, errors.WithStack(errors.Wrap(ErrReadConn, err))
	}
	return data, length == 0xffffff, nil
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

func (p *PacketIO) WriteOnePacket(data []byte) (int, error) {
	length := len(data)
	if length >= 0xffffff {
		length = 0xffffff
	}

	var header [4]byte
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = p.sequence
	p.sequence++

	if _, err := io.Copy(p.buf, bytes.NewReader(header[:])); err != nil {
		return 0, errors.WithStack(errors.Wrap(ErrWriteConn, err))
	}

	if _, err := io.Copy(p.buf, bytes.NewReader(data)); err != nil {
		return 0, errors.WithStack(errors.Wrap(ErrWriteConn, err))
	}

	return length, nil
}

// WritePacket writes data without a header
func (p *PacketIO) WritePacket(data []byte) error {
	for len(data) > 0 {
		n, err := p.WriteOnePacket(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return p.Flush()
}

func (p *PacketIO) Flush() error {
	if err := p.buf.Flush(); err != nil {
		return errors.WithStack(errors.Wrap(ErrFlushConn, err))
	}
	return nil
}

func (p *PacketIO) Close() error {
	var errs []error
	if err := p.Flush(); err != nil {
		errs = append(errs, err)
	}
	if p.tlsConn != nil {
		if err := p.tlsConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Collect(ErrCloseConn, errs...)
}
