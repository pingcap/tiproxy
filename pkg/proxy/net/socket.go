// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"net"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

var (
	ErrSetNoDelay = errors.New("failed to set tcp no delay")
)

func SetNoDelay(conn net.Conn, noDelay bool) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return errors.Wrapf(ErrSetNoDelay, "not net.TCPConn")
	}
	if err := tcpConn.SetNoDelay(noDelay); err != nil {
		return errors.Wrap(ErrSetNoDelay, err)
	}
	return nil
}
