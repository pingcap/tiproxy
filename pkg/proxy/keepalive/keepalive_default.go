//go:build !(linux || netbsd || freebsd || dragonfly || aix || darwin)

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"github.com/pingcap/tiproxy/lib/config"
)

func setKeepalive(fd uintptr, cfg config.KeepAlive) error {
	return nil
}
