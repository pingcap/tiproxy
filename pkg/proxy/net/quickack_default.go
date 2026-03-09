// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package net

import "net"

func setQuickAck(net.Conn) error {
	return nil
}
