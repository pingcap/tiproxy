// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
