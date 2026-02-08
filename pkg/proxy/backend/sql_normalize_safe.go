// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import "github.com/pingcap/tidb/pkg/parser"

var normalizeSQLFn = func(sqlText string) string {
	return parser.Normalize(sqlText, "ON")
}

func normalizeSQLSafe(sqlText string) (normalized string, ok bool) {
	ok = true
	defer func() {
		if r := recover(); r != nil {
			ok = false
			normalized = ""
		}
	}()
	return normalizeSQLFn(sqlText), ok
}
