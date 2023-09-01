//go:build linux || netbsd || freebsd || dragonfly || aix

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"syscall"

	"github.com/pingcap/tiproxy/lib/config"
)

func setKeepalive(fd uintptr, cfg config.KeepAlive) error {
	if val := cfg.Idle.Seconds(); val > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, int(val)); err != nil {
			return err
		}
	}
	if cfg.Cnt > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, cfg.Cnt); err != nil {
			return err
		}
	}
	if val := cfg.Intvl.Seconds(); val > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, int(val)); err != nil {
			return err
		}
	}
	return nil
}
