//go:build windows

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"syscall"

	"github.com/pingcap/tiproxy/lib/config"
)

const _TCP_MAXRT = 5

func setTimeout(fd uintptr, cfg config.KeepAlive) error {
	return syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_TCP, _TCP_MAXRT, int(cfg.Timeout.Seconds()))
}
