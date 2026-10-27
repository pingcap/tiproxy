//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris

// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package peeractive

import "net"

func isPeerActiveWithRawConn(net.Conn) (active bool, supported bool) {
	return false, false
}
