// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiproxy/lib/config"
)

// NormalizeSQL redacts literals in a SQL statement. Off keeps the text.
// Any mode other than Off or Marker is treated as On.
func NormalizeSQL(sql string, mode config.RedactInfoLogType) string {
	return parser.Normalize(sql, normalize(mode).SQLRedactMode())
}

// ParamsText formats prepared-statement parameters for a report row.
func ParamsText(params []any, mode config.RedactInfoLogType) string {
	mode = normalize(mode)
	if mode == config.RedactInfoLogOff {
		return fmt.Sprintf("%v", params)
	}
	if params == nil {
		return "[]"
	}
	parts := make([]string, len(params))
	for i, param := range params {
		if mode == config.RedactInfoLogMarker {
			parts[i] = wrapMarker(paramString(param))
			continue
		}
		parts[i] = "?"
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// ErrorMessage redacts quoted text in an error sample. The error code and the
// words outside quotes stay, so rows can still be grouped by error shape.
func ErrorMessage(msg string, mode config.RedactInfoLogType) string {
	mode = normalize(mode)
	if mode == config.RedactInfoLogOff || msg == "" || (!strings.Contains(msg, "'") && !strings.Contains(msg, `"`)) {
		return msg
	}

	var b strings.Builder
	b.Grow(len(msg))
	for i := 0; i < len(msg); {
		inner, next, ok := quotedSpan(msg, i)
		if !ok {
			b.WriteByte(msg[i])
			i++
			continue
		}
		b.WriteByte(msg[i])
		if mode == config.RedactInfoLogMarker {
			b.WriteString(wrapMarker(inner))
		} else {
			b.WriteByte('?')
		}
		b.WriteByte(msg[next-1])
		i = next
	}
	return b.String()
}

func normalize(mode config.RedactInfoLogType) config.RedactInfoLogType {
	switch mode {
	case config.RedactInfoLogOff, config.RedactInfoLogMarker:
		return mode
	default:
		return config.RedactInfoLogOn
	}
}

func paramString(param any) string {
	if param == nil {
		return "<nil>"
	}
	return fmt.Sprint(param)
}

// wrapMarker encloses s in TiDB redaction markers and doubles marker runes
// that already appear in the value so the original text can be recovered.
func wrapMarker(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteRune('‹')
	for _, r := range s {
		b.WriteRune(r)
		if r == '‹' || r == '›' {
			b.WriteRune(r)
		}
	}
	b.WriteRune('›')
	return b.String()
}

// quotedSpan returns the raw text inside a quote that starts at open.
// A quote without a closer, such as the apostrophe in "doesn't", is left unchanged.
func quotedSpan(s string, open int) (inner string, next int, ok bool) {
	if open >= len(s) {
		return "", 0, false
	}
	quote := s[open]
	if quote != '\'' && quote != '"' {
		return "", 0, false
	}
	// An apostrophe surrounded by word characters is part of a contraction or
	// possessive, not the start of quoted sensitive text.
	if quote == '\'' && open > 0 && open+1 < len(s) && isASCIIWordByte(s[open-1]) && isASCIIWordByte(s[open+1]) {
		return "", 0, false
	}

	// A quote followed immediately by a word character can be an unescaped
	// nested opening quote. Prefer a later unambiguous closer, but retain the
	// first such quote as a fallback for malformed messages.
	ambiguousCloser := -1
	i := open + 1
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			i += 2
			continue
		}
		if s[i] == quote {
			// MySQL escapes quotes by doubling them. When a doubled pair is
			// followed by a word character or another quote remains to close the
			// surrounding span, it is inside the quoted value. Otherwise, the
			// pair can close nested and outer quoted fragments.
			if i+1 < len(s) && s[i+1] == quote {
				if i+2 < len(s) && (isASCIIWordByte(s[i+2]) || hasLaterUnescapedQuote(s, i+2, quote)) {
					i += 2
					continue
				}
				return s[open+1 : i+1], i + 2, true
			}
			if i+1 < len(s) && isASCIIWordByte(s[i+1]) {
				if ambiguousCloser < 0 {
					ambiguousCloser = i
				}
				i++
				continue
			}
			return s[open+1 : i], i + 1, true
		}
		i++
	}
	if ambiguousCloser >= 0 {
		return s[open+1 : ambiguousCloser], ambiguousCloser + 1, true
	}
	return "", 0, false
}

func hasLaterUnescapedQuote(s string, start int, quote byte) bool {
	for i := start; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			continue
		}
		if s[i] == quote {
			return true
		}
	}
	return false
}

func isASCIIWordByte(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_'
}
