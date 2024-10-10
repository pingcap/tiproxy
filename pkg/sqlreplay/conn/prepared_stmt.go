// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

const (
	setSessionStates = "SET SESSION_STATES "
)

type sessionStates struct {
	PreparedStmts map[uint32]*preparedStmtInfo `json:"prepared-stmts,omitempty"`
}

// Prepared stmt in the session states. Used for unmarshal.
type preparedStmtInfo struct {
	StmtText   string `json:"text"`
	ParamTypes []byte `json:"types,omitempty"`
}

// Used for parsing prepared stmt.
type preparedStmt struct {
	text       string
	paramTypes []byte
	paramNum   int
}
