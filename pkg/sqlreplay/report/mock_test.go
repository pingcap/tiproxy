// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
)

var _ ReportDB = (*mockReportDB)(nil)

type mockReportDB struct {
	sync.Mutex
	exceptions map[conn.ExceptionType]map[string]*expCollection
}

func (db *mockReportDB) Close() {
}

func (db *mockReportDB) Init(ctx context.Context) error {
	db.clear()
	return nil
}

func (db *mockReportDB) clear() {
	db.Lock()
	defer db.Unlock()
	exceptions := make(map[conn.ExceptionType]map[string]*expCollection, conn.Total)
	for i := 0; i < int(conn.Total); i++ {
		exceptions[conn.ExceptionType(i)] = make(map[string]*expCollection)
	}
	db.exceptions = exceptions
}

func (db *mockReportDB) InsertExceptions(startTime time.Time, tp conn.ExceptionType, m map[string]*expCollection) error {
	db.Lock()
	defer db.Unlock()
	exps := db.exceptions[tp]
	for k, v := range m {
		if exp, ok := exps[k]; ok {
			exp.count += v.count
		} else {
			exps[k] = v
		}
	}
	return nil
}

var _ conn.BackendConn = (*mockBackendConn)(nil)

type mockBackendConn struct {
	connErr error
	execErr error
	curID   uint32
	stmtID  []uint32
	args    [][]any
}

func (c *mockBackendConn) Connect(ctx context.Context) error {
	return c.connErr
}

func (c *mockBackendConn) ConnID() uint64 {
	return 1
}

func (c *mockBackendConn) ExecuteCmd(ctx context.Context, request []byte) (err error) {
	return c.execErr
}

func (c *mockBackendConn) Query(ctx context.Context, stmt string) error {
	return c.execErr
}

func (c *mockBackendConn) PrepareStmt(ctx context.Context, stmt string) (uint32, error) {
	c.curID++
	return c.curID, c.execErr
}

func (c *mockBackendConn) ExecuteStmt(ctx context.Context, stmtID uint32, args []any) error {
	c.stmtID = append(c.stmtID, stmtID)
	c.args = append(c.args, args)
	return c.execErr
}

func (c *mockBackendConn) GetPreparedStmt(stmtID uint32) (string, int, []byte) {
	return "", 0, nil
}

func (c *mockBackendConn) clear() {
	c.stmtID = nil
	c.args = nil
}

func (c *mockBackendConn) Close() {
}
