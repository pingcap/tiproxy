// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
)

func countSQLPlaceholders(sql string) int {
	count := 0
	for i := 0; i < len(sql); {
		if isSQLSpace(sql[i]) {
			i++
			continue
		}
		if next, ok := skipSQLToken(sql, i); ok {
			i = next
			continue
		}
		if sql[i] == '?' {
			count++
		}
		i++
	}
	return count
}

func validateExecuteRequest(sql string, args []any, executeReq []byte) (err error) {
	paramNum := countSQLPlaceholders(sql)
	if len(args) != paramNum {
		return errors.Errorf("execute param count mismatch: sql has %d placeholders, got %d args", paramNum, len(args))
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("parse execute request panicked: %v", r)
		}
	}()

	_, _, _, parseErr := pnet.ParseExecuteStmtRequest(executeReq, paramNum, nil)
	if parseErr != nil {
		return errors.Wrapf(parseErr, "parse execute request failed")
	}
	return nil
}

func logAndSkipInvalidExecuteRequest(lg *zap.Logger, auditLine, sql string, args []any, err error) {
	lg.Warn("skip invalid audit log",
		zap.Error(err),
		zap.String("audit_line", auditLine),
		zap.String("sql", sql),
		zap.Any("params", args),
	)
}
