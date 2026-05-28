// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/parser"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"github.com/siddontang/go/hack"
)

const outputTimeFormat = "20060102 15:04:05.999"

// Record is the JSON payload written for each executed SQL.
type Record struct {
	SQL    string `json:"sql"`
	DB     string `json:"db"`
	Cost   string `json:"cost"`
	ExTime string `json:"ex_time"`
}

// NewRecord builds a record from exec info. The second return value is false when the command
// should not be recorded (no SQL text).
func NewRecord(info conn.ExecInfo) (Record, bool) {
	sql := sqlFromCommand(info.Command)
	if len(sql) == 0 {
		return Record{}, false
	}
	return Record{
		SQL:    sql,
		DB:     info.Command.CurDB,
		Cost:   strconv.FormatFloat(float64(info.CostTime)/1000000.0, 'f', 3, 64),
		ExTime: info.StartTime.Format(outputTimeFormat),
	}, true
}

func sqlFromCommand(command *cmd.Command) string {
	switch command.Type {
	case pnet.ComStmtExecute:
		return command.PreparedStmt
	case pnet.ComQuery:
		sql := hack.String(command.Payload[1:])
		return parser.Normalize(sql, "ON")
	default:
		return ""
	}
}
