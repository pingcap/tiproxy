// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

var sensitiveKeywords = [][]string{
	// contain passwords
	{
		"CREATE", "USER",
	},
	{
		"ALTER", "USER",
	},
	{
		"SET", "PASSWORD",
	},
	{
		"GRANT",
	},
	// contain cloud storage url
	{
		"BACKUP",
	},
	{
		"RESTORE",
	},
	{
		"IMPORT",
	},
	// not supported yet
	{
		"LOAD", "DATA",
	},
}

// ignore prepared statements in text-protocol
// ignore EXPLAIN (including EXPLAIN ANALYZE) and TRACE
// include SELECT FOR UPDATE because it releases locks immediately in auto-commit transactions
// include SET because SET SESSION_STATES and SET session variables should be executed
var readOnlyKeywords = []string{
	"SELECT", "SHOW", "WITH", "SET", "USE", "DESC", "DESCRIBE", "TABLE", "DO",
}

func IsSensitiveSQL(sql string) bool {
	lexer := NewLexer(sql)
	keyword := lexer.NextToken()
	if len(keyword) == 0 {
		return false
	}
	for _, kw := range sensitiveKeywords {
		if keyword != kw[0] {
			continue
		}
		if len(kw) <= 1 {
			return true
		}
		keyword = lexer.NextToken()
		if keyword == kw[1] {
			return true
		}
	}
	return false
}

func IsReadOnly(sql string) bool {
	lexer := NewLexer(sql)
	keyword := lexer.NextToken()
	if len(keyword) == 0 {
		return false
	}
	for _, kw := range readOnlyKeywords {
		if keyword == kw {
			return true
		}
	}
	return false
}
