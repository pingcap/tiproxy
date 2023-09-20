// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

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
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/proxy/keepalive"
	"github.com/pingcap/tiproxy/pkg/proxy/proxyprotocol"
	"go.uber.org/zap"
)

var (
	ErrInvalidSequence = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)
)

const (
	defaultWriterSize = 16 * 1024
	defaultReaderSize = 16 * 1024
)

type buffer interface {
	Peek(n int) ([]byte, error)
	Discard(n int) (int, error)
	Flush() error
}

type packetReadWriter interface {
	net.Conn
	buffer
	afterRead()
	reset()
	getProxy() *proxyprotocol.Proxy
	tlsConnectionState() tls.ConnectionState
	getInBytes() uint64
	getOutBytes() uint64
}

// rdbufConn buffers read for non-TLS connections.
type rdbufConn struct {
	net.Conn
	*bufio.ReadWriter
	inBytes  uint64
	outBytes uint64
}

func newRdbufConn(conn net.Conn) *rdbufConn {
	return &rdbufConn{
		Conn:       conn,
		ReadWriter: bufio.NewReadWriter(bufio.NewReaderSize(conn, defaultReaderSize), bufio.NewWriterSize(conn, defaultWriterSize)),
	}
}

func (f *rdbufConn) Read(b []byte) (n int, err error) {
	n, err = f.ReadWriter.Read(b)
	f.inBytes += uint64(n)
	return n, err
}

func (f *rdbufConn) Write(p []byte) (n int, err error) {
	n, err = f.ReadWriter.Write(p)
	f.outBytes += uint64(n)
	return n, err
}

func (f *rdbufConn) afterRead() {
}

func (f *rdbufConn) reset() {
}

func (f *rdbufConn) getProxy() *proxyprotocol.Proxy {
	return nil
}

func (f *rdbufConn) getInBytes() uint64 {
	return f.inBytes
}

func (f *rdbufConn) getOutBytes() uint64 {
	return f.outBytes
}

func (f *rdbufConn) tlsConnectionState() tls.ConnectionState {
	if conn, ok := f.Conn.(*tls.Conn); ok {
		return conn.ConnectionState()
	}
	return tls.ConnectionState{}
}

// PacketIO is a helper to read and write sql and proxy protocol.
type PacketIO struct {
	lastKeepAlive config.KeepAlive
	rawConn       net.Conn
	readWriter    packetReadWriter
	logger        *zap.Logger
	remoteAddr    net.Addr
	wrap          error
	sequence      uint8
}

func NewPacketIO(conn net.Conn, lg *zap.Logger, opts ...PacketIOption) *PacketIO {
	p := &PacketIO{
		rawConn:    conn,
		logger:     lg,
		sequence:   0,
		readWriter: newRdbufConn(conn),
	}
	p.ApplyOpts(opts...)
	return p
}

func (p *PacketIO) ApplyOpts(opts ...PacketIOption) {
	for _, opt := range opts {
		opt(p)
	}
}

func (p *PacketIO) wrapErr(err error) error {
	return errors.WithStack(errors.Wrap(p.wrap, err))
}

func (p *PacketIO) LocalAddr() net.Addr {
	return p.readWriter.LocalAddr()
}

func (p *PacketIO) RemoteAddr() net.Addr {
	if p.remoteAddr != nil {
		return p.remoteAddr
	}
	return p.readWriter.RemoteAddr()
}

func (p *PacketIO) ResetSequence() {
	p.sequence = 0
	p.readWriter.reset()
}

// GetSequence is used in tests to assert that the sequences on the client and server are equal.
func (p *PacketIO) GetSequence() uint8 {
	return p.sequence
}

func (p *PacketIO) readOnePacket() ([]byte, bool, error) {
	var header [4]byte
	defer p.readWriter.afterRead()
	if _, err := io.ReadFull(p.readWriter, header[:]); err != nil {
		return nil, false, errors.Wrap(ErrReadConn, err)
	}
	sequence := header[3]
	if sequence != p.sequence {
		return nil, false, ErrInvalidSequence.GenWithStack("invalid sequence, expected %d, actual %d", p.sequence, sequence)
	}
	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	data := make([]byte, length)
	if _, err := io.ReadFull(p.readWriter, data); err != nil {
		return nil, false, errors.Wrap(ErrReadConn, err)
	}
	return data, length == MaxPayloadLen, nil
}

// ReadPacket reads data and removes the header
func (p *PacketIO) ReadPacket() (data []byte, err error) {
	for more := true; more; {
		var buf []byte
		buf, more, err = p.readOnePacket()
		if err != nil {
			err = p.wrapErr(err)
			return
		}
		data = append(data, buf...)
	}
	return data, nil
}

func (p *PacketIO) writeOnePacket(data []byte) (int, bool, error) {
	more := false
	length := len(data)
	if length >= MaxPayloadLen {
		// we need another packet, this is true even if
		// the current packet is of len(MaxPayloadLen) exactly
		length = MaxPayloadLen
		more = true
	}

	var header [4]byte
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = p.sequence
	p.sequence++

	if _, err := io.Copy(p.readWriter, bytes.NewReader(header[:])); err != nil {
		return 0, more, errors.Wrap(ErrWriteConn, err)
	}

	if _, err := io.Copy(p.readWriter, bytes.NewReader(data[:length])); err != nil {
		return 0, more, errors.Wrap(ErrWriteConn, err)
	}

	return length, more, nil
}

// WritePacket writes data without a header
func (p *PacketIO) WritePacket(data []byte, flush bool) (err error) {
	for more := true; more; {
		var n int
		n, more, err = p.writeOnePacket(data)
		if err != nil {
			err = p.wrapErr(err)
			return
		}
		data = data[n:]
	}
	if flush {
		return p.Flush()
	}
	return nil
}

func (p *PacketIO) InBytes() uint64 {
	return p.readWriter.getInBytes()
}

func (p *PacketIO) OutBytes() uint64 {
	return p.readWriter.getOutBytes()
}

func (p *PacketIO) Flush() error {
	if err := p.readWriter.Flush(); err != nil {
		return p.wrapErr(errors.Wrap(ErrFlushConn, err))
	}
	return nil
}

// IsPeerActive checks if the peer connection is still active.
// This function cannot be called concurrently with other functions of PacketIO.
// This function normally costs 1ms, so don't call it too frequently.
// This function may incorrectly return true if the system is extremely slow.
func (p *PacketIO) IsPeerActive() bool {
	if err := p.readWriter.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
		return false
	}
	active := true
	if _, err := p.readWriter.Peek(1); err != nil {
		active = !errors.Is(err, io.EOF)
	}
	if err := p.readWriter.SetReadDeadline(time.Time{}); err != nil {
		return false
	}
	return active
}

func (p *PacketIO) SetKeepalive(cfg config.KeepAlive) error {
	if cfg == p.lastKeepAlive {
		return nil
	}
	p.lastKeepAlive = cfg
	return keepalive.SetKeepalive(p.rawConn, cfg)
}

// LastKeepAlive is used for test.
func (p *PacketIO) LastKeepAlive() config.KeepAlive {
	return p.lastKeepAlive
}

func (p *PacketIO) GracefulClose() error {
	if err := p.readWriter.SetDeadline(time.Now()); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
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
	if err := p.readWriter.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, err)
	}
	return p.wrapErr(errors.Collect(ErrCloseConn, errs...))
}
