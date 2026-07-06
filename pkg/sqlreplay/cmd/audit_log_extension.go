// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
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
	pendingCmds          []*Command
	psCloseStrategy      PSCloseStrategy
	userAllowlist        map[string]struct{}
	tableSuffixAllowlist map[string]struct{}
	idAllocator          *ConnIDAllocator
	lg                   *zap.Logger
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

// SetTableSuffixAllowlist implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetTableSuffixAllowlist(suffixes []string) {
	if len(suffixes) == 0 {
		decoder.tableSuffixAllowlist = nil
		return
	}
	m := make(map[string]struct{})
	for _, s := range suffixes {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		m[s] = struct{}{}
	}
	if len(m) == 0 {
		decoder.tableSuffixAllowlist = nil
	} else {
		decoder.tableSuffixAllowlist = m
	}
}

// SetUserAllowlist implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetUserAllowlist(users []string) {
	if len(users) == 0 {
		decoder.userAllowlist = nil
		return
	}
	m := make(map[string]struct{})
	for _, u := range users {
		u = strings.ToLower(strings.TrimSpace(u))
		if u == "" {
			continue
		}
		m[u] = struct{}{}
	}
	if len(m) == 0 {
		decoder.userAllowlist = nil
	} else {
		decoder.userAllowlist = m
	}
}

// SetCommandEndTime implements [AuditLogDecoder].
func (decoder *AuditLogExtensionDecoder) SetCommandEndTime(t time.Time) {
	if t.IsZero() {
		return
	}
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
	// commandStartTime is not supported for extension decoder, use commandEndTime instead.
	decoder.commandEndTime = t
}

func (decoder *AuditLogExtensionDecoder) Decode(reader LineReader) (retCmd *Command, err error) {
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
		if err != nil {
			return nil, errors.Errorf("%s, line %d: parsing timestamp failed: %s", filename, lineIdx, kvs[auditPluginKeyLogTime])
		}
		if endTs.Before(decoder.commandEndTime) {
			// Ignore the commands before CommandEndTime.
			continue
		}
		if decoder.userAllowlist != nil {
			user := strings.ToLower(strings.TrimSpace(kvs[auditPluginKeyUser]))
			if _, ok := decoder.userAllowlist[user]; !ok {
				decoder.lg.Debug("skipping command because user is not in allowlist", zap.String("user", user))
				continue
			}
		}

		if decoder.tableSuffixAllowlist != nil {
			if !tablesFieldMatchesSuffixAllowlist(kvs[auditPluginKeyTables], decoder.tableSuffixAllowlist) {
				decoder.lg.Debug("skipping command because table suffix is not in allowlist", zap.String("tables", kvs[auditPluginKeyTables]))
				continue
			}
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
			decoder.lg.Debug("skipping command because it is empty", zap.String("event", events[0]))
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
			// Redacted extension logs omit EXECUTE_PARAMS, and the original prepared values can't be recovered.
			return nil, nil
		}
		args, err := parseExecuteParamsForExtension(params)
		if err != nil {
			return nil, err
		}
		args = coerceLimitParams(sql, args)

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
	if len(v) == 0 {
		return nil, errors.Errorf("no brackets in params: %s", value)
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

// coerceLimitParams converts string params bound to LIMIT/OFFSET placeholders to integers.
// TiDB rejects string params in LIMIT while other contexts allow implicit conversion.
func coerceLimitParams(sql string, args []any) []any {
	indices := limitParamIndices(sql)
	if len(indices) == 0 {
		return args
	}
	for _, idx := range indices {
		if idx >= len(args) {
			continue
		}
		s, ok := args[idx].(string)
		if !ok {
			continue
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			args[idx] = i
			continue
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			args[idx] = u
		}
	}
	return args
}

func limitParamIndices(sql string) []int {
	indexSet := make(map[int]struct{})
	paramIdx := 0
	parenDepth := 0
	limitDepth := -1

	for i := 0; i < len(sql); {
		if isSQLSpace(sql[i]) {
			i++
			continue
		}
		if next, ok := skipSQLToken(sql, i); ok {
			i = next
			continue
		}

		switch sql[i] {
		case '(':
			parenDepth++
			i++
		case ')':
			if limitDepth >= 0 && parenDepth == limitDepth {
				limitDepth = -1
			}
			parenDepth--
			i++
		case '?':
			if limitDepth >= 0 && parenDepth == limitDepth {
				indexSet[paramIdx] = struct{}{}
			}
			paramIdx++
			i++
		default:
			if limitDepth >= 0 && parenDepth == limitDepth {
				if matched, kwLen := matchSQLKeyword(sql, i, "for"); matched {
					limitDepth = -1
					i += kwLen
					continue
				}
			}
			if matched, kwLen := matchSQLKeyword(sql, i, "limit"); matched {
				limitDepth = parenDepth
				i += kwLen
				continue
			}
			if matched, kwLen := matchSQLKeyword(sql, i, "offset"); matched {
				limitDepth = parenDepth
				i += kwLen
				continue
			}
			i++
		}
	}

	indices := make([]int, 0, len(indexSet))
	for idx := range indexSet {
		indices = append(indices, idx)
	}
	return indices
}

func isSQLSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isSQLIdentChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func matchSQLKeyword(sql string, pos int, keyword string) (bool, int) {
	if pos+len(keyword) > len(sql) {
		return false, 0
	}
	if !strings.EqualFold(sql[pos:pos+len(keyword)], keyword) {
		return false, 0
	}
	if pos > 0 && isSQLIdentChar(sql[pos-1]) {
		return false, 0
	}
	next := pos + len(keyword)
	if next < len(sql) && isSQLIdentChar(sql[next]) {
		return false, 0
	}
	return true, len(keyword)
}

func skipSQLToken(sql string, pos int) (int, bool) {
	switch sql[pos] {
	case '\'':
		end := skipQuotes(sql[pos+1:], true)
		if end == -1 {
			return len(sql), true
		}
		return pos + end + 2, true
	case '"':
		end := skipQuotes(sql[pos+1:], false)
		if end == -1 {
			return len(sql), true
		}
		return pos + end + 2, true
	case '`':
		end := strings.IndexByte(sql[pos+1:], '`')
		if end == -1 {
			return len(sql), true
		}
		return pos + end + 2, true
	case '-':
		if pos+1 < len(sql) && sql[pos+1] == '-' {
			end := strings.IndexByte(sql[pos:], '\n')
			if end == -1 {
				return len(sql), true
			}
			return pos + end + 1, true
		}
	case '/':
		if pos+1 < len(sql) && sql[pos+1] == '*' {
			end := strings.Index(sql[pos+2:], "*/")
			if end == -1 {
				return len(sql), true
			}
			return pos + end + 4, true
		}
	}
	return pos, false
}
