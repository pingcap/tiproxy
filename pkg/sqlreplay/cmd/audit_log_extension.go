// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

var _ AuditLogDecoder = (*AuditLogExtensionDecoder)(nil)

type AuditLogExtensionDecoder struct {
	connInfo       map[uint64]auditLogPluginConnCtx
	commandEndTime time.Time
	// pendingCmds contains the commands that has not been returned yet.
	pendingCmds     []*Command
	psCloseStrategy PSCloseStrategy
	idAllocator     *ConnIDAllocator
	lg              *zap.Logger
}

func NewAuditLogExtensionDecoder(lg *zap.Logger) AuditLogDecoder {
	return &AuditLogExtensionDecoder{
		connInfo:        make(map[uint64]auditLogPluginConnCtx),
		psCloseStrategy: PSCloseStrategyDirected,
		lg:              lg,
	}
}

// EnableFilterCommandWithRetry implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) EnableFilterCommandWithRetry() {
	// do nothing for extension decoder, it's not supported yet
}

// SetCommandEndTime implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetCommandEndTime(t time.Time) {
	decoder.commandEndTime = t
}

// SetIDAllocator implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetIDAllocator(alloc *ConnIDAllocator) {
	decoder.idAllocator = alloc
}

// SetPSCloseStrategy implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetPSCloseStrategy(s PSCloseStrategy) {
	decoder.psCloseStrategy = s
}

// SetCommandStartTime implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetCommandStartTime(t time.Time) {
	// do nothing for extension decoder
}

func (decoder *AuditLogExtensionDecoder) Decode(reader LineReader) (retCmd *Command, err error) {
	defer func() {
		if retCmd != nil {
			fmt.Println("Decoded command:", retCmd.ConnID, retCmd.Line, retCmd.StartTs, retCmd.EndTs, "error:", err)
		}
	}()
	if len(decoder.pendingCmds) > 0 {
		cmd := decoder.pendingCmds[0]
		decoder.pendingCmds = decoder.pendingCmds[1:]
		return cmd, nil
	}

	kvs := make(map[string]string, 25)
	for {
		line, filename, lineIdx, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		clear(kvs)
		err = parseLog(kvs, hack.String(line))
		if err != nil {
			return nil, errors.Errorf("%s, line %d: %s", filename, lineIdx, err.Error())
		}
		connStr := kvs[auditPluginKeyConnID]
		if len(connStr) == 0 {
			return nil, errors.Errorf("%s, line %d: no connection id in line: %s", filename, lineIdx, line)
		}
		upstreamConnID, err := strconv.ParseUint(connStr, 10, 64)
		if err != nil {
			return nil, errors.Errorf("%s, line %d: parsing connection id failed: %s", filename, lineIdx, connStr)
		}

		// TODO: add both startTs and endTs in extension log. We only have the endTS is the current format.
		endTs, err := time.Parse(timeLayout, kvs[auditPluginKeyLogTime])
		if endTs.Before(decoder.commandEndTime) {
			// Ignore the commands before CommandEndTime.
			continue
		}

		var connID uint64
		if connCtx, ok := decoder.connInfo[upstreamConnID]; ok {
			connID = connCtx.connID
		} else {
			// New connection, allocate a new connection ID.
			if decoder.idAllocator == nil {
				connID = upstreamConnID
			} else {
				connID = decoder.idAllocator.alloc()
			}
			connCtx.connID = connID
			decoder.connInfo[upstreamConnID] = connCtx
		}

		eventStr := kvs[auditPluginKeyEvent]
		if len(eventStr) <= 4 {
			return nil, errors.Errorf("%s, line %d: invalid event field: %s", filename, lineIdx, eventStr)
		}
		// Remove the surrounding quotes and brackets.
		eventStr = eventStr[2 : len(eventStr)-2]
		events := strings.Split(eventStr, ",")
		var cmds []*Command
		switch events[0] {
		case "CONNECTION":
			if len(events) > 1 && events[1] == "DISCONNECT" {
				delete(decoder.connInfo, upstreamConnID)
				cmds = []*Command{{
					Type:    pnet.ComQuit,
					Payload: []byte{pnet.ComQuit.Byte()},
				}}
			}
		case "QUERY":
			cmds, err = decoder.parseQueryEvent(kvs, events, upstreamConnID)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "%s, line %d", filename, lineIdx)
		}
		// The log is ignored, skip.
		if len(cmds) == 0 {
			continue
		}

		db := kvs[auditPluginKeyCurDB]
		for _, cmd := range cmds {
			cmd.Success = true
			cmd.UpstreamConnID = upstreamConnID
			cmd.ConnID = connID
			// We don't have an accurate startTs in extension log.
			cmd.StartTs = endTs
			cmd.CurDB = db
			cmd.FileName = filename
			cmd.Line = lineIdx
			cmd.EndTs = endTs
			cmd.kvs = kvs
		}
		if len(cmds) > 1 {
			decoder.pendingCmds = cmds[1:]
		}
		return cmds[0], nil
	}
}

func (decoder *AuditLogExtensionDecoder) parseQueryEvent(kvs map[string]string, events []string, connID uint64) ([]*Command, error) {
	connInfo := decoder.connInfo[connID]
	if connInfo.preparedStmt == nil {
		connInfo.preparedStmt = make(map[uint32]struct{})
		connInfo.preparedStmtSql = make(map[string]uint32)
	}

	var sql string
	sqlStr := kvs[auditPluginKeySQL]
	if len(sqlStr) > 0 {
		var err error
		sql, err = parseSQL(sqlStr)
		if err != nil {
			return nil, errors.Wrapf(err, "unquote sql failed: %s", sqlStr)
		}
	}
	cmds := make([]*Command, 0, 3)
	// Only handle two events:
	// - QUERY,EXECUTE
	// - QUERY
	if events[0] == "QUERY" && len(events) > 1 && events[1] == "EXECUTE" {
		params, ok := kvs[auditPluginKeyParams]
		if !ok {
			return nil, nil
		}
		args, err := parseExecuteParamsForExtension(params)
		if err != nil {
			return nil, err
		}

		var stmtID uint32
		var shouldPrepare bool

		switch decoder.psCloseStrategy {
		case PSCloseStrategyAlways:
			connInfo.lastPsID++
			decoder.connInfo[connID] = connInfo
			stmtID = connInfo.lastPsID
			shouldPrepare = true
		case PSCloseStrategyNever:
			if id, ok := connInfo.preparedStmtSql[sql]; ok {
				shouldPrepare = false
				stmtID = id
			} else {
				connInfo.lastPsID++
				connInfo.preparedStmtSql[sql] = connInfo.lastPsID
				decoder.connInfo[connID] = connInfo
				stmtID = connInfo.lastPsID
				shouldPrepare = true
			}
		}

		// Append PREPARE command if needed.
		if shouldPrepare {
			cmds = append(cmds, &Command{
				CapturedPsID: stmtID,
				Type:         pnet.ComStmtPrepare,
				StmtType:     kvs[auditPluginKeyStmtType],
				PreparedStmt: sql,
				Payload:      append([]byte{pnet.ComStmtPrepare.Byte()}, hack.Slice(sql)...),
			})
		}

		// Append EXECUTE command
		executeReq, err := pnet.MakeExecuteStmtRequest(stmtID, args, true)
		if err != nil {
			return nil, errors.Wrapf(err, "make execute request failed")
		}
		cmds = append(cmds, &Command{
			CapturedPsID: stmtID,
			Type:         pnet.ComStmtExecute,
			StmtType:     kvs[auditPluginKeyStmtType],
			PreparedStmt: sql,
			Params:       args,
			Payload:      executeReq,
		})
		connInfo.lastCmd = cmds[len(cmds)-1]

		// Append CLOSE command if needed.
		if decoder.psCloseStrategy == PSCloseStrategyAlways {
			// close the prepared statement right after it's executed.
			cmds = append(cmds, &Command{
				CapturedPsID: stmtID,
				Type:         pnet.ComStmtClose,
				StmtType:     kvs[auditPluginKeyStmtType],
				PreparedStmt: sql,
				Payload:      pnet.MakeCloseStmtRequest(stmtID),
			})
		}
	} else if events[0] == "QUERY" {
		cmds = append(cmds, &Command{
			Type:     pnet.ComQuery,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  append([]byte{pnet.ComQuery.Byte()}, hack.Slice(sql)...),
		})
		connInfo.lastCmd = cmds[0]
	}

	decoder.connInfo[connID] = connInfo
	return cmds, nil
}

// parseExecuteParamsForExtension parses the param in audit log extension field like "[1,abc,NULL,\"test bytes\""]"
// This function has the following known limitations:
// - All params are returned as string type. It cannot distinguish int 1 and string "1".
// - It cannot distinguish single empty string and no param.
func parseExecuteParamsForExtension(value string) ([]any, error) {
	v, err := strconv.Unquote(value)
	if err != nil {
		return nil, errors.Wrapf(err, "unquote execute params failed: %s", value)
	}
	if v[0] != '[' || v[len(v)-1] != ']' {
		return nil, errors.Errorf("no brackets in params: %s", value)
	}
	v = v[1 : len(v)-1]
	if len(v) == 0 {
		return nil, nil
	}

	params := make([]any, 0, 10)
	for idx := 0; idx < len(v); idx++ {
		switch v[idx] {
		case '"':
			endIdx := skipQuotes(v[idx+1:], false)
			if endIdx == -1 {
				return nil, errors.Errorf("unterminated quote in params: %s", v[idx+1:])
			}

			unquoted, err := strconv.Unquote(v[idx : idx+endIdx+2])
			if err != nil {
				return nil, errors.Wrapf(err, "unquote param failed: %s", v[idx:idx+endIdx+2])
			}
			params = append(params, unquoted)
			idx += endIdx + 1
		case ',', ' ':
		default:
			endIdx := strings.Index(v[idx:], ",")
			if endIdx == -1 {
				endIdx = len(v) - idx
			}
			params = append(params, v[idx:idx+endIdx])
			idx += endIdx - 1
		}
	}

	return params, nil
}
