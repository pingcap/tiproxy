// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cmdCh, exceptionCh := make(chan *cmd.Command), make(chan Exception)
	conn := newConn(lg, "u1", "", nil, nil, 100, &backend.BCConfig{}, cmdCh, exceptionCh)
	conn.backendConn = &mockBackendConn{connErr: errors.New("mock error")}
	conn.Run(context.Background())
	require.NotNil(t, <-exceptionCh)
	conn.Close()
}
