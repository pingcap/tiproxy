//go:build darwin

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"syscall"

	"github.com/pingcap/TiProxy/lib/config"
)

func setTimeout(fd uintptr, cfg config.KeepAlive) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_RXT_CONNDROPTIME, int(cfg.Timeout.Seconds()))
}
