// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bytes"
	"strings"
)

const (
	sqlTypeSelect   = "select"
	sqlTypeInsert   = "insert"
	sqlTypeUpdate   = "update"
	sqlTypeDelete   = "delete"
	sqlTypeReplace  = "replace"
	sqlTypeBegin    = "begin"
	sqlTypeCommit   = "commit"
	sqlTypeRollback = "rollback"
	sqlTypeSet      = "set"
	sqlTypeUse      = "use"
	sqlTypeOther    = "other"
)

var interactionSQLTypes = []string{
	sqlTypeSelect,
	sqlTypeInsert,
	sqlTypeUpdate,
	sqlTypeDelete,
	sqlTypeReplace,
	sqlTypeBegin,
	sqlTypeCommit,
	sqlTypeRollback,
	sqlTypeSet,
	sqlTypeUse,
	sqlTypeOther,
}

func classifyComQuerySQLType(query []byte) string {
	pos := skipLeadingSQLTokens(query, 0, true)
	if pos >= len(query) {
		return sqlTypeOther
	}
	first, pos := readSQLKeyword(query, pos)
	if first == "" {
		return sqlTypeOther
	}
	switch first {
	case sqlTypeSelect:
		return sqlTypeSelect
	case sqlTypeInsert:
		return sqlTypeInsert
	case sqlTypeUpdate:
		return sqlTypeUpdate
	case sqlTypeDelete:
		return sqlTypeDelete
	case sqlTypeReplace:
		return sqlTypeReplace
	case sqlTypeBegin:
		return sqlTypeBegin
	case sqlTypeCommit:
		return sqlTypeCommit
	case sqlTypeRollback:
		return sqlTypeRollback
	case sqlTypeSet:
		return sqlTypeSet
	case sqlTypeUse:
		return sqlTypeUse
	case "start":
		second, _ := readSQLKeyword(query, skipLeadingSQLTokens(query, pos, false))
		if second == "transaction" {
			return sqlTypeBegin
		}
	}
	return sqlTypeOther
}

func skipLeadingSQLTokens(query []byte, pos int, skipSemicolon bool) int {
	for pos < len(query) {
		switch query[pos] {
		case ' ', '\t', '\n', '\r':
			pos++
		case ';':
			if !skipSemicolon {
				return pos
			}
			pos++
		case '#':
			pos = skipLineComment(query, pos+1)
		default:
			if pos+1 < len(query) && query[pos] == '-' && query[pos+1] == '-' {
				pos = skipLineComment(query, pos+2)
				continue
			}
			if pos+1 < len(query) && query[pos] == '/' && query[pos+1] == '*' {
				end := bytes.Index(query[pos+2:], []byte("*/"))
				if end < 0 {
					return len(query)
				}
				pos += end + 4
				continue
			}
			return pos
		}
	}
	return pos
}

func skipLineComment(query []byte, pos int) int {
	for pos < len(query) && query[pos] != '\n' {
		pos++
	}
	return pos
}

func readSQLKeyword(query []byte, pos int) (string, int) {
	start := pos
	for pos < len(query) {
		ch := query[pos]
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') {
			break
		}
		pos++
	}
	if pos == start {
		return "", pos
	}
	return strings.ToLower(string(query[start:pos])), pos
}
