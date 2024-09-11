// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/stretchr/testify/require"
)

func TestReadExceptions(t *testing.T) {
	now := time.Now()
	tests := []struct {
		exceptions []conn.Exception
		finalExps  map[conn.ExceptionType]map[string]*expCollection
	}{
		{
			exceptions: []conn.Exception{
				conn.NewOtherException(errors.New("mock error"), 1),
			},
			finalExps: map[conn.ExceptionType]map[string]*expCollection{
				conn.Other: {
					"mock error": &expCollection{
						count:  1,
						sample: conn.NewOtherException(errors.New("mock error"), 1),
					},
				},
				conn.Fail: {},
			},
		},
		{
			exceptions: []conn.Exception{
				conn.NewOtherException(errors.New("mock error"), 2),
				conn.NewOtherException(errors.New("mock error"), 3),
			},
			finalExps: map[conn.ExceptionType]map[string]*expCollection{
				conn.Other: {
					"mock error": &expCollection{
						count:  3,
						sample: conn.NewOtherException(errors.New("mock error"), 1),
					},
				},
				conn.Fail: {},
			},
		},
		{
			exceptions: []conn.Exception{
				conn.NewOtherException(errors.New("another error"), 1),
			},
			finalExps: map[conn.ExceptionType]map[string]*expCollection{
				conn.Other: {
					"mock error": &expCollection{
						count:  3,
						sample: conn.NewOtherException(errors.New("mock error"), 1),
					},
					"another error": &expCollection{
						count:  1,
						sample: conn.NewOtherException(errors.New("another error"), 1),
					},
				},
				conn.Fail: {},
			},
		},
		{
			exceptions: []conn.Exception{
				conn.NewFailException(errors.New("another error"), cmd.NewCommand(append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), now, 1)),
			},
			finalExps: map[conn.ExceptionType]map[string]*expCollection{
				conn.Other: {
					"mock error": &expCollection{
						count:  3,
						sample: conn.NewOtherException(errors.New("mock error"), 1),
					},
					"another error": &expCollection{
						count:  1,
						sample: conn.NewOtherException(errors.New("another error"), 1),
					},
				},
				conn.Fail: {
					"\x03e1c71d1661ae46e09b7aaec1c390957f0d6260410df4e4bc71b9c8d681021471": &expCollection{
						count: 1,
						sample: conn.NewFailException(errors.New("another error"), cmd.NewCommand(
							append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...), now, 1)),
					},
				},
			},
		},
	}

	lg, _ := logger.CreateLoggerForTest(t)
	exceptionCh := make(chan conn.Exception, 1)
	report := NewReport(lg, exceptionCh, nil)
	db := &mockReportDB{}
	report.db = db
	defer report.Close()
	require.NoError(t, report.Start(context.Background(), ReportConfig{flushInterval: 10 * time.Millisecond}))

	for i, test := range tests {
		for _, exp := range test.exceptions {
			report.exceptionCh <- exp
		}
		require.Eventually(t, func() bool {
			db.Lock()
			defer db.Unlock()
			return reflect.DeepEqual(db.exceptions, test.finalExps)
		}, 3*time.Second, 10*time.Millisecond, "case %d", i)
	}
}
