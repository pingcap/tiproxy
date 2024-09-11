// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"go.uber.org/zap"
)

type BackendConnCreator func() conn.BackendConn

type ReportDB interface {
	Init(ctx context.Context) error
	InsertExceptions(ctx context.Context, tp conn.ExceptionType, m map[string]*expCollection) error
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
	rdb.conn = rdb.connCreator()
	return rdb.conn.Connect(ctx)
}

func (rdb *reportDB) initTables(ctx context.Context) error {
	for _, stmt := range []string{dropDatabase, createDatabase, createFailTable, createOtherTable} {
		if err := rdb.conn.Query(ctx, stmt); err != nil {
			return errors.Wrapf(err, "initialize report database and tables failed")
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

func (rdb *reportDB) InsertExceptions(ctx context.Context, tp conn.ExceptionType, m map[string]*expCollection) error {
	for _, value := range m {
		var args []any
		switch tp {
		case conn.Fail:
			sample := value.sample.(*conn.FailException)
			command := sample.Command()
			// TODO: fill capture and replay time
			args = []any{command.Type.String(), command.Digest(), command.QueryText(), sample.Error(), sample.ConnID(), nil, nil, value.count, value.count}
		case conn.Other:
			sample := value.sample.(*conn.OtherException)
			args = []any{sample.Key(), sample.Error(), nil, value.count, value.count}
		default:
			return errors.WithStack(errors.New("unknown exception type"))
		}
		// retry in case of disconnection
		for i := 0; i < 3; i++ {
			err := rdb.conn.ExecuteStmt(ctx, rdb.stmtIDs[tp], args)
			if err == nil {
				break
			}
			if pnet.IsDisconnectError(err) {
				if err = rdb.reconnect(ctx); err != nil {
					return err
				}
				continue
			}
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
