// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

func startsWithKeyword(sql string, keywords [][]string) bool {
	lexer := NewLexer(sql)
	tokens := make([]string, 0, 2)
	for _, kw := range keywords {
		match := true
		for i, t := range kw {
			if len(tokens) <= i {
				tokens = append(tokens, lexer.NextToken())
			}
			if tokens[i] != t {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

var sensitiveKeywords = [][]string{
	// contain passwords
	{"CREATE", "USER"},
	{"ALTER", "USER"},
	{"SET", "PASSWORD"},
	{"GRANT"},
	// contain cloud storage url
	{"BACKUP"},
	{"RESTORE"},
	{"IMPORT"},
	// not supported yet
	{"LOAD", "DATA"},
}

func IsSensitiveSQL(sql string) bool {
	return startsWithKeyword(sql, sensitiveKeywords)
}

// ignore prepared statements in text-protocol
// ignore EXPLAIN, EXPLAIN ANALYZE, and TRACE
// include SELECT FOR UPDATE because it doesn't require write privilege
// include SET because SET SESSION_STATES and SET session variables should be executed
// include BEGIN / COMMIT in case the user sets autocommit to false, either in SET SESSION_STATES or SET @@autocommit
var readOnlyKeywords = [][]string{
	{"SELECT"},
	{"SHOW"},
	{"WITH"},
	{"SET"},
	{"USE"},
	{"DESC"},
	{"DESCRIBE"},
	{"TABLE"},
	{"DO"},
	{"BEGIN"},
	{"COMMIT"},
	{"ROLLBACK"},
	{"START", "TRANSACTION"},
}

func IsReadOnly(sql string) bool {
	return startsWithKeyword(sql, readOnlyKeywords)
}
