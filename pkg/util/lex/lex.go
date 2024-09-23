// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package lex

type Lexer struct {
	sql      string
	curToken []byte
	curIdx   int
}

func NewLexer(sql string) *Lexer {
	return &Lexer{
		sql:      sql,
		curToken: make([]byte, 0, 100),
	}
}

// It only returns uppercased identifiers and keywords. It's used to search for some specified keywords.
// It doesn't need to strict but it needs to be fast enough.
func (l *Lexer) NextToken() string {
	l.curToken = l.curToken[:0]
	inSingleLineComment, inMultiLineComment, inSingleQuote, inDoubleQuote := false, false, false, false
	for ; l.curIdx < len(l.sql); l.curIdx++ {
		char := l.sql[l.curIdx]
		switch {
		case inSingleLineComment:
			if char == '\n' {
				inSingleLineComment = false
			}
		case inMultiLineComment:
			if char == '*' {
				if l.curIdx+1 < len(l.sql) && l.sql[l.curIdx+1] == '/' {
					inMultiLineComment = false
					l.curIdx++
				}
			}
		case inSingleQuote:
			if char == '\\' {
				l.curIdx++
			} else if char == '\'' {
				inSingleQuote = false
			}
		case inDoubleQuote:
			if char == '\\' {
				l.curIdx++
			} else if char == '"' {
				inDoubleQuote = false
			}
		case char == '-' && l.curIdx+1 < len(l.sql) && l.sql[l.curIdx+1] == '-':
			l.curIdx++
			inSingleLineComment = true
		case char == '/' && l.curIdx+1 < len(l.sql) && l.sql[l.curIdx+1] == '*':
			l.curIdx++
			inMultiLineComment = true
		case char == '\'':
			inSingleQuote = true
		case char == '"':
			inDoubleQuote = true
		case char >= 'a' && char <= 'z':
			l.curToken = append(l.curToken, char-'a'+'A')
		case char >= 'A' && char <= 'Z' || char == '_':
			l.curToken = append(l.curToken, char)
		default:
			if len(l.curToken) > 0 {
				l.curIdx++
				return string(l.curToken)
			}
		}
	}

	if len(l.curToken) > 0 {
		return string(l.curToken)
	}
	return ""
}
