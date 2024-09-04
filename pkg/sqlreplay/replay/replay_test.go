// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestManageConns(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	replay := NewReplay(lg, nil, nil, nil)
	defer replay.Close()
	var conn *mockConn
	replay.connCreator = func(connID uint64, cmdCh chan *cmd.Command, exceptionCh chan Exception) Conn {
		conn = &mockConn{
			cmdCh:       cmdCh,
			exceptionCh: exceptionCh,
			connID:      connID,
		}
		return conn
	}
	cmd := &cmd.Command{
		ConnID: 1,
		Type:   pnet.ComQuery,
	}
	replay.executeCmd(context.Background(), cmd)
	wrapper, ok := replay.conns[1]
	require.True(t, ok)
	require.NotNil(t, wrapper)

	cmd.ConnID = 2
	replay.executeCmd(context.Background(), cmd)
	wrapper, ok = replay.conns[2]
	require.True(t, ok)
	require.NotNil(t, wrapper)

	conn.exceptionCh <- &otherException{err: errors.New("mock error")}
	replay.executeCmd(context.Background(), cmd)
	wrapper, ok = replay.conns[2]
	require.True(t, ok)
	require.Nil(t, wrapper)
}
