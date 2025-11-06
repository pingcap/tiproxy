// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/retry"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

type BackendConnCreator func() conn.BackendConn

type ReportDB interface {
	Init(ctx context.Context) error
	InsertExceptions(startTime time.Time, tp conn.ExceptionType, m map[string]*expCollection) error
	Close()
}

var _ ReportDB = (*reportDB)(nil)

type reportDB struct {
	stmtIDs     map[conn.ExceptionType]uint32
	connCreator BackendConnCreator
	conn        conn.BackendConn
	lg          *zap.Logger
}

func NewReportDB(lg *zap.Logger, connCreator BackendConnCreator) *reportDB {
	return &reportDB{
		lg:          lg,
		connCreator: connCreator,
	}
}

func (rdb *reportDB) Init(ctx context.Context) error {
	if err := rdb.connect(ctx); err != nil {
		return err
	}
	if err := rdb.initTables(ctx); err != nil {
		return err
	}
	return rdb.initStmts(ctx)
}

func (rdb *reportDB) reconnect(ctx context.Context) error {
	if err := rdb.connect(ctx); err != nil {
		return err
	}
	return rdb.initStmts(ctx)
}

func (rdb *reportDB) connect(ctx context.Context) error {
	if rdb.conn != nil {
		rdb.conn.Close()
	}
	// Connect to the backend using backendConn instead of the go driver,
	// because the backend host is assigned by the router and adapting to the router needs some work.
	rdb.conn = rdb.connCreator()
	if err := rdb.conn.Connect(ctx, ""); err != nil {
		return err
	}
	// Set sql_mode to non-strict mode so that inserted data can be truncated automatically.
	resp := rdb.conn.ExecuteCmd(ctx, append([]byte{pnet.ComQuery.Byte()}, hack.Slice("set sql_mode=''")...))
	return resp.Err
}

func (rdb *reportDB) initTables(ctx context.Context) error {
	// Do not truncate the database or tables in case that multiple TiProxy instances are running.
	// If checking tables fails, it means that the table was created by an older TiProxy version.
	for _, stmt := range []string{createDatabase, createFailTable, checkFailTable, createOtherTable, checkOtherTable} {
		if err := rdb.conn.Query(ctx, stmt); err != nil {
			return errors.Wrapf(errors.WithStack(err), "initialize report database and tables failed, sql: %s", stmt)
		}
	}
	return nil
}

func (rdb *reportDB) initStmts(ctx context.Context) (err error) {
	rdb.stmtIDs = make(map[conn.ExceptionType]uint32)
	if rdb.stmtIDs[conn.Fail], err = rdb.conn.PrepareStmt(ctx, insertFailTable); err != nil {
		return err
	}
	if rdb.stmtIDs[conn.Other], err = rdb.conn.PrepareStmt(ctx, insertOtherTable); err != nil {
		return err
	}
	return
}

func (rdb *reportDB) InsertExceptions(startTime time.Time, tp conn.ExceptionType, m map[string]*expCollection) error {
	for _, value := range m {
		var args []any
		switch tp {
		case conn.Fail:
			sample := value.sample.(*conn.FailException)
			command := sample.Command()
			args = []any{startTime.String(), command.Type.String(), command.Digest(), command.QueryText(), sample.Error(), sample.ConnID(),
				command.FileName, command.Line, command.StartTs.String(), sample.Time().String(), value.count, value.count}
		case conn.Other:
			sample := value.sample.(*conn.OtherException)
			args = []any{startTime.String(), sample.Key(), sample.Error(), sample.Time().String(), value.count, value.count}
		default:
			return errors.WithStack(errors.New("unknown exception type"))
		}
		// retry in case of disconnection
		ctx := context.Background()
		err := retry.Retry(func() error {
			err := rdb.conn.ExecuteStmt(ctx, rdb.stmtIDs[tp], args)
			if err == nil {
				return nil
			}
			if pnet.IsDisconnectError(err) {
				if err := rdb.reconnect(ctx); err != nil {
					return backoff.Permanent(err)
				}
			}
			return err
		}, ctx, 100*time.Millisecond, 3)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rdb *reportDB) Close() {
	if rdb.conn != nil {
		rdb.conn.Close()
	}
}
