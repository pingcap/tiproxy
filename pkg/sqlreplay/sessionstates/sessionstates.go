// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sessionstates

import (
	"encoding/json"
	"strings"
)

const (
	setSessionStatesPrefix = "SET SESSION_STATES "
)

// SessionStates mirrors TiDB session states JSON schema partially.
// Currently we only need prepared statements for traffic capture / replay.
type SessionStates struct {
	PreparedStmts map[uint32]*PreparedStmtInfo `json:"prepared-stmts,omitempty"`
}

type PreparedStmtInfo struct {
	StmtText   string `json:"text"`
	ParamTypes []byte `json:"types,omitempty"`
}

// ParseFromSetSessionStatesSQL parses a SQL string like:
//   SET SESSION_STATES '...json...'
// and returns its decoded SessionStates. If sql is not a SET SESSION_STATES statement,
// it returns an empty SessionStates and nil error.
func ParseFromSetSessionStatesSQL(sql string) (SessionStates, error) {
	var ss SessionStates
	if len(sql) <= len(setSessionStatesPrefix) || !strings.EqualFold(sql[:len(setSessionStatesPrefix)], setSessionStatesPrefix) {
		return ss, nil
	}
	// Remove prefix then trim quotes.
	query := strings.TrimSpace(sql[len(setSessionStatesPrefix):])
	query = strings.Trim(query, "'\"")
	// Unescape backslashes and quotes. This matches the escaping done when building SET SESSION_STATES.
	query = strings.ReplaceAll(query, "\\\\", "\\")
	query = strings.ReplaceAll(query, "\\'", "'")
	if err := json.Unmarshal([]byte(query), &ss); err != nil {
		return SessionStates{}, err
	}
	return ss, nil
}

// ExtractPreparedStmtTextsFromSetSessionStatesSQL is a best-effort helper to extract stmtID -> SQL text.
func ExtractPreparedStmtTextsFromSetSessionStatesSQL(sql string) map[uint32]string {
	ss, err := ParseFromSetSessionStatesSQL(sql)
	if err != nil || len(ss.PreparedStmts) == 0 {
		return nil
	}
	m := make(map[uint32]string, len(ss.PreparedStmts))
	for id, info := range ss.PreparedStmts {
		if info == nil || len(info.StmtText) == 0 {
			continue
		}
		m[id] = info.StmtText
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

