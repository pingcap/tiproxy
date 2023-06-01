// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import "github.com/pingcap/TiProxy/lib/util/errors"

var (
	ErrExpectSSLRequest = errors.New("expect a SSLRequest packet")
	ErrReadConn         = errors.New("failed to read the connection")
	ErrWriteConn        = errors.New("failed to write the connection")
	ErrFlushConn        = errors.New("failed to flush the connection")
	ErrCloseConn        = errors.New("failed to close the connection")
	ErrHandshakeTLS     = errors.New("failed to complete tls handshake")
)
