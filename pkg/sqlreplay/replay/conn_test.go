// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestConnectError(t *testing.T) {
	tests := []struct {
		connErr error
		execErr error
	}{
		{
			connErr: errors.New("mock error"),
		},
		{
			execErr: io.EOF,
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	for i, test := range tests {
		exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
		conn := newConn(lg, "u1", "", nil, nil, 1, &backend.BCConfig{}, exceptionCh, closeCh)
		backendConn := &mockBackendConn{connErr: test.connErr, execErr: test.execErr}
		conn.backendConn = backendConn
		wg.RunWithRecover(func() {
			conn.Run(context.Background())
		}, nil, lg)
		if test.connErr == nil {
			conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComQuery})
		}
		e := <-exceptionCh
		require.Equal(t, uint64(1), e.ConnID(), "case %d", i)
		require.Equal(t, uint64(1), <-closeCh, "case %d", i)
		require.True(t, backendConn.close.Load(), "case %d", i)
		wg.Wait()
	}
}

func TestExecuteCmd(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	var wg waitgroup.WaitGroup
	exceptionCh, closeCh := make(chan Exception, 1), make(chan uint64, 1)
	conn := newConn(lg, "u1", "", nil, nil, 1, &backend.BCConfig{}, exceptionCh, closeCh)
	backendConn := &mockBackendConn{}
	conn.backendConn = backendConn
	childCtx, cancel := context.WithCancel(context.Background())
	wg.RunWithRecover(func() {
		conn.Run(childCtx)
	}, nil, lg)
	for i := 0; i < 100; i++ {
		conn.ExecuteCmd(&cmd.Command{ConnID: 1, Type: pnet.ComQuery})
	}
	require.Eventually(t, func() bool {
		return backendConn.cmds.Load() == 100
	}, 3*time.Second, time.Millisecond)
	cancel()
	wg.Wait()
}
