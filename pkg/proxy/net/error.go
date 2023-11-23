// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"github.com/pingcap/tiproxy/lib/util/errors"
)

var (
	ErrReadConn     = errors.New("failed to read the connection")
	ErrWriteConn    = errors.New("failed to write the connection")
	ErrRelayConn    = errors.New("failed to relay the connection")
	ErrFlushConn    = errors.New("failed to flush the connection")
	ErrCloseConn    = errors.New("failed to close the connection")
	ErrHandshakeTLS = errors.New("failed to complete tls handshake")
)
