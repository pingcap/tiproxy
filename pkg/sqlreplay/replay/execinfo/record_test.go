// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"testing"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/stretchr/testify/require"
)

func TestNewRecord(t *testing.T) {
	startTime := time.Date(2025, 9, 6, 17, 3, 50, 222000000, time.UTC)
	rec, ok := NewRecord(conn.ExecInfo{
		Command: &cmd.Command{
			CurDB:   "db1",
			Type:    pnet.ComQuery,
			Payload: append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...),
		},
		StartTime: startTime,
		CostTime:  time.Second,
	})
	require.True(t, ok)
	require.Equal(t, Record{
		SQL:    "select ?",
		DB:     "db1",
		Cost:   "1000.000",
		ExTime: "20250906 17:03:50.222",
	}, rec)

	_, ok = NewRecord(conn.ExecInfo{
		Command: &cmd.Command{Type: pnet.ComInitDB},
	})
	require.False(t, ok)
}
