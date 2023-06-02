// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package keepalive

import (
	"net"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
)

var (
	ErrKeepAlive = errors.New("failed to set keepalive and timeout")
)

func SetKeepalive(conn net.Conn, cfg config.KeepAlive) error {
	tcpcn, ok := conn.(*net.TCPConn)
	if !ok {
		return errors.Wrapf(ErrKeepAlive, "not net.TCPConn")
	}

	if err := tcpcn.SetKeepAlive(cfg.Enabled); err != nil {
		return errors.Wrap(ErrKeepAlive, err)
	}

	syscn, err := tcpcn.SyscallConn()
	if err != nil {
		return errors.Wrap(ErrKeepAlive, err)
	}

	var kerr, terr error
	return errors.Collect(ErrKeepAlive, kerr, terr, syscn.Control(func(fd uintptr) {
		if cfg.Enabled {
			kerr = setKeepalive(fd, cfg)
		}
		terr = setTimeout(fd, cfg)
	}))
}
