package driver

import (
	"context"
	"hash/crc32"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/tidb-incubator/weir/pkg/proxy/constant"
	wast "github.com/tidb-incubator/weir/pkg/util/ast"
)

func (q *QueryCtxImpl) isStmtDenied(ctx context.Context, sqlDigest uint32) bool {
	return q.ns.IsDeniedSQL(sqlDigest)
}

func (q *QueryCtxImpl) isStmtAllowed(ctx context.Context, sqlDigest uint32) bool {
	return q.ns.IsAllowedSQL(sqlDigest)
}

func (q *QueryCtxImpl) getBreakerName(ctx context.Context, sql string, breaker Breaker) (string, bool) {
	switch breaker.GetBreakerScope() {
	case "namespace":
		return q.ns.Name(), true
	case "db":
		return q.currentDB, true
	case "table":
		firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
		return firstTableName, true
	case "sql":
		_, digest := parser.NormalizeDigest(sql)
		sqlDigest := crc32.ChecksumIEEE([]byte(digest))
		return string(wast.UInt322Bytes(sqlDigest)), true
	default:
		return "", false
	}
}

func (q *QueryCtxImpl) getRateLimiterKey(ctx context.Context, rateLimiter RateLimiter) (string, bool) {
	switch rateLimiter.Scope() {
	case "namespace":
		return q.ns.Name(), true
	case "db":
		return q.currentDB, true
	case "table":
		firstTableName, _ := wast.GetAstTableNameFromCtx(ctx)
		return firstTableName, true
	default:
		return "", false
	}
}

func (q *QueryCtxImpl) executeStmt(ctx context.Context, sql string) (*gomysql.Result, error) {
	ctx = context.WithValue(ctx, constant.ContextKeySessionVariable, q.sessionVars.GetAllSystemVars())

	result, err := q.connMgr.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (q *QueryCtxImpl) useDB(ctx context.Context, db string) error {
	if !q.ns.IsDatabaseAllowed(db) {
		return mysql.NewErrf(mysql.ErrDBaccessDenied, "db %s access denied", db)
	}
	q.currentDB = db
	return nil
}
