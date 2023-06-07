//go:build darwin

// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"syscall"

	"github.com/pingcap/TiProxy/lib/config"
)

const (
	// missing in older darwin
	_TCP_KEEPINTVL = 0x101
	_TCP_KEEPCNT   = 0x102
)

func setKeepalive(fd uintptr, cfg config.KeepAlive) error {
	if val := cfg.Idle.Seconds(); val > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPALIVE, int(val)); err != nil {
			return err
		}
	}
	if cfg.Cnt > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, _TCP_KEEPCNT, cfg.Cnt); err != nil {
			return err
		}
	}
	if val := cfg.Intvl.Seconds(); val > 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, _TCP_KEEPINTVL, int(val)); err != nil {
			return err
		}
	}
	return nil
}
