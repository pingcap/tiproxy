// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
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

type selectIntoFileVisitor struct {
	found bool
}

func (v *selectIntoFileVisitor) Enter(node ast.Node) bool {
	selectStmt, ok := node.(*ast.SelectStmt)
	if !ok || selectStmt.SelectIntoOpt == nil {
		return false
	}
	v.found = selectStmt.SelectIntoOpt.Tp == ast.SelectIntoOutfile || selectStmt.SelectIntoOpt.Tp == ast.SelectIntoDumpfile
	return v.found
}

func (v *selectIntoFileVisitor) Leave(ast.Node) bool {
	return !v.found
}

func isReadOnlyParsedSQL(sql string) bool {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil || !ast.IsReadOnly(stmt, true) {
		return false
	}
	visitor := &selectIntoFileVisitor{}
	ast.Walk(stmt, visitor)
	return !visitor.found
}

func isReadOnlySelect(sql string, lexer *Lexer) bool {
	previousToken := ""
	for {
		token := lexer.NextToken()
		if token == "" {
			return true
		}
		if previousToken == "FOR" && token == "UPDATE" {
			return false
		}
		if previousToken == "INTO" && (token == "OUTFILE" || token == "DUMPFILE") {
			return isReadOnlyParsedSQL(sql)
		}
		previousToken = token
	}
}

func isReadOnlyWith(sql string, lexer *Lexer) bool {
	previousToken := ""
	for {
		token := lexer.NextToken()
		if token == "" {
			return true
		}
		if previousToken == "INTO" && (token == "OUTFILE" || token == "DUMPFILE") {
			return isReadOnlyParsedSQL(sql)
		}
		switch token {
		case "UPDATE", "DELETE", "INSERT", "REPLACE":
			// The lexer does not distinguish keywords from identifiers. Parse only
			// ambiguous WITH statements so identifiers such as `update` stay allowed.
			return isReadOnlyParsedSQL(sql)
		}
		previousToken = token
	}
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
		return isReadOnlySelect(sql, lexer)
	case "WITH":
		return isReadOnlyWith(sql, lexer)
	case "SHOW", "USE", "DESC", "DESCRIBE", "TABLE", "DO", "BEGIN", "COMMIT", "ROLLBACK":
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
