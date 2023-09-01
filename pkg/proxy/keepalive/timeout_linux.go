//go:build linux

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"syscall"

	"github.com/pingcap/tiproxy/lib/config"
)

const _TCP_USER_TIMEOUT = 0x12

func setTimeout(fd uintptr, cfg config.KeepAlive) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, _TCP_USER_TIMEOUT, int(cfg.Timeout.Milliseconds()))
}
