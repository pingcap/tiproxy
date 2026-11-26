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

// ErrorMessage redacts an error sample. For canonical MySQL errors, the numeric
// error code and SQLSTATE stay so rows can still be grouped without exposing
// the unstructured message body.
func ErrorMessage(msg string, mode config.RedactInfoLogType) string {
	mode = normalize(mode)
	if mode == config.RedactInfoLogOff || msg == "" {
		return msg
	}

	prefix, body := splitMySQLError(msg)
	if mode == config.RedactInfoLogMarker {
		return prefix + wrapMarker(body)
	}
	return prefix + "?"
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

// splitMySQLError keeps only the fixed metadata prefix emitted by
// go-mysql's MyError.Error. Everything after the prefix is unstructured and
// may contain values supplied by a client or returned by a backend.
func splitMySQLError(msg string) (prefix, body string) {
	const errorPrefix = "ERROR "
	if !strings.HasPrefix(msg, errorPrefix) {
		return "", msg
	}

	codeStart := len(errorPrefix)
	codeEnd := codeStart
	for codeEnd < len(msg) && msg[codeEnd] >= '0' && msg[codeEnd] <= '9' {
		codeEnd++
	}
	if codeEnd == codeStart || codeEnd-codeStart > 5 || codeEnd+2 > len(msg) || msg[codeEnd:codeEnd+2] != " (" {
		return "", msg
	}

	stateStart := codeEnd + 2
	stateEnd := stateStart + 5
	if stateEnd+3 > len(msg) || msg[stateEnd:stateEnd+3] != "): " {
		return "", msg
	}
	for i := stateStart; i < stateEnd; i++ {
		if (msg[i] < 'A' || msg[i] > 'Z') && (msg[i] < '0' || msg[i] > '9') {
			return "", msg
		}
	}

	bodyStart := stateEnd + 3
	return msg[:bodyStart], msg[bodyStart:]
}
