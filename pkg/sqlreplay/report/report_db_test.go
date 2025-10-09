// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/stretchr/testify/require"
)

func TestInitDB(t *testing.T) {
	tests := []struct {
		connErr error
		execErr error
	}{
		{nil, nil},
		{errors.New("connect error"), nil},
		{nil, errors.New("execute error")},
		{errors.New("connect error"), errors.New("execute error")},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		connCreator := func() conn.BackendConn {
			return &mockBackendConn{connErr: test.connErr, execErr: test.execErr}
		}
		db := NewReportDB(lg, connCreator)
		checkErr := func(err error) {
			if test.connErr != nil {
				require.ErrorIsf(t, err, test.connErr, "case %d", i)
			} else if test.execErr != nil {
				require.ErrorIsf(t, err, test.execErr, "case %d", i)
			} else {
				require.NoErrorf(t, err, "case %d", i)
			}
		}
		err := db.Init(context.Background())
		checkErr(err)
		err = db.reconnect(context.Background())
		checkErr(err)
		db.Close()
	}
}

func TestInsertExceptions(t *testing.T) {
	now := time.Now()
	cmd := cmd.NewCommand(append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), now, 1)
	cmd.FileName = "my/file"
	cmd.Line = 100
	failSample := conn.NewFailException(errors.New("mock error"), cmd)
	otherSample1 := conn.NewOtherException(errors.Wrapf(errors.New("mock error"), "wrap"), 1)
	otherSample2 := conn.NewOtherException(errors.New("mock error"), 1)
	otherSample3 := conn.NewOtherException(errors.New("another error"), 2)
	tests := []struct {
		tp     conn.ExceptionType
		colls  map[string]*expCollection
		stmtID []uint32
		args   [][]any
	}{
		{
			tp: conn.Other,
			colls: map[string]*expCollection{
				"mock error": {
					count:  1,
					sample: otherSample1,
				},
			},
			stmtID: []uint32{2},
			args:   [][]any{{now.String(), "mock error", "wrap: mock error", otherSample1.Time().String(), uint64(1), uint64(1)}},
		},
		{
			tp: conn.Other,
			colls: map[string]*expCollection{
				"mock error": {
					count:  2,
					sample: otherSample2,
				},
				"another error": {
					count:  2,
					sample: otherSample3,
				},
			},
			stmtID: []uint32{2, 2},
			args: [][]any{{now.String(), "mock error", "mock error", otherSample2.Time().String(), uint64(2), uint64(2)},
				{now.String(), "another error", "another error", otherSample3.Time().String(), uint64(2), uint64(2)}},
		},
		{
			tp: conn.Fail,
			colls: map[string]*expCollection{
				"\x03e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471": {
					count:  1,
					sample: failSample,
				},
			},
			stmtID: []uint32{1},
			args: [][]any{{now.String(), "Query", "e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471", "select 1", "mock error",
				uint64(1), "my/file", 100, now.String(), failSample.Time().String(), uint64(1), uint64(1)}},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	for i, test := range tests {
		cn := &mockBackendConn{}
		connCreator := func() conn.BackendConn {
			return cn
		}
		db := NewReportDB(lg, connCreator)
		err := db.Init(context.Background())
		require.NoErrorf(t, err, "case %d", i)
		cn.clear()
		err = db.InsertExceptions(now, test.tp, test.colls)
		require.NoErrorf(t, err, "case %d", i)
		require.Equal(t, test.stmtID, cn.stmtID, "case %d", i)
		if len(test.args) > 1 {
			sort.Slice(test.args, func(i, j int) bool { return test.args[i][1].(string) < test.args[j][1].(string) })
			sort.Slice(cn.args, func(i, j int) bool { return cn.args[i][1].(string) < cn.args[j][1].(string) })
		}
		require.Equal(t, test.args, cn.args, "case %d", i)
		db.Close()
	}
}
