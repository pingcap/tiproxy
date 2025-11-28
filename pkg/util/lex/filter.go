// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
)

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
func IsReadOnly(sql string) bool {
	lexer := NewLexer(sql)
	switch lexer.NextToken() {
	case "SELECT":
		for {
			token := lexer.NextToken()
			if token == "" {
				break
			}
			if token == "FOR" && lexer.NextToken() == "UPDATE" {
				return false
			}
		}
		return true
	case "SHOW", "WITH", "USE", "DESC", "DESCRIBE", "TABLE", "DO", "BEGIN", "COMMIT", "ROLLBACK":
		return true
	case "START":
		return lexer.NextToken() == "TRANSACTION"
	case "SET":
		// Filter `set global`, `set @@global.`, `set password`, and other unknown statements.
		normalized := parser.Normalize(sql, "ON")
		switch {
		case strings.HasPrefix(normalized, "set session_states "):
			return true
		case strings.HasPrefix(normalized, "set session "):
			return true
		case strings.HasPrefix(normalized, "set names "):
			return true
		case strings.HasPrefix(normalized, "set char "):
			return true
		case strings.HasPrefix(normalized, "set charset "):
			return true
		case strings.HasPrefix(normalized, "set character "):
			return true
		case strings.HasPrefix(normalized, "set transaction "):
			return true
		case strings.HasPrefix(normalized, "set @@global."):
			return false
		case strings.HasPrefix(normalized, "set @"):
			return true
		}
		return false

	}
	return false
}

var startTxnKeywords = [][]string{
	{"START", "TRANSACTION"},
	{"BEGIN"},
}

func IsStartTxn(sql string) bool {
	return startsWithKeyword(sql, startTxnKeywords)
}
