// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

const (
	auditPluginKeyTimeStamp      = "TIMESTAMP"
	auditPluginKeySQL            = "SQL_TEXT"
	auditPluginKeyConnID         = "CONNECTION_ID"
	auditPluginKeyClass          = "EVENT_CLASS"
	auditPluginKeySubClass       = "EVENT_SUBCLASS"
	auditPluginKeyCommand        = "COMMAND"
	auditPluginKeyStmtType       = "SQL_STATEMENTS"
	auditPluginKeyParams         = "EXECUTE_PARAMS"
	auditPluginKeyCurDB          = "CURRENT_DB"
	auditPluginKeyEvent          = "EVENT"
	auditPluginKeyCostTime       = "COST_TIME"
	auditPluginKeyPreparedStmtID = "PREPARED_STMT_ID"
	auditPluginKeyRetry          = "RETRY"

	auditPluginClassGeneral     = "GENERAL"
	auditPluginClassTableAccess = "TABLE_ACCESS"
	auditPluginClassConnect     = "CONNECTION"

	auditPluginSubClassConnected  = "Connected"
	auditPluginSubClassDisconnect = "Disconnect"

	auditPluginEventEnd = "COMPLETED"

	timeLayout = "2006/01/02 15:04:05.999 -07:00"
)

type DupItem struct {
	Times      int
	MinOverlap time.Duration
	Cost       time.Duration
}

type DeDup struct {
	sync.Mutex
	Items map[string]DupItem
}

func NewDeDup() *DeDup {
	return &DeDup{
		Items: make(map[string]DupItem),
	}
}

type auditLogPluginConnCtx struct {
	connID   uint64
	lastPsID uint32

	// preparedStmt contains the prepared statement IDs that are not closed yet, only used for `ps-close=directed`.
	preparedStmt map[uint32]struct{}
	// preparedStmtSql contains the prepared statement SQLs, only used for `ps-close=never`.
	// It doesn't require the prepared statement IDs to be contained in the audit logs.
	preparedStmtSql map[string]uint32
	lastCmd         *Command
}

func NewAuditLogPluginDecoder(dedup *DeDup, lg *zap.Logger) *AuditLogPluginDecoder {
	return &AuditLogPluginDecoder{
		connInfo:        make(map[uint64]auditLogPluginConnCtx),
		psCloseStrategy: PSCloseStrategyDirected,
		dedup:           dedup,
		lg:              lg,
	}
}

var _ CmdDecoder = (*AuditLogPluginDecoder)(nil)

// PSCloseStrategy defines when to close the prepared statements.
type PSCloseStrategy string

const (
	// PSCloseStrategyAlways means a prepared statement is closed right after it's executed.
	PSCloseStrategyAlways PSCloseStrategy = "always"
	// PSCloseStrategyNever means a prepared statement is never closed. It's re-used if the same statement
	// occurs again in the connection.
	PSCloseStrategyNever PSCloseStrategy = "never"
	// PSCloseStrategyDirected means a prepared statement is closed only when there's close command in the
	// traffic file.
	PSCloseStrategyDirected PSCloseStrategy = "directed"
)

type AuditLogPluginDecoder struct {
	connInfo         map[uint64]auditLogPluginConnCtx
	commandStartTime time.Time
	commandEndTime   time.Time
	// pendingCmds contains the commands that has not been returned yet.
	pendingCmds            []*Command
	psCloseStrategy        PSCloseStrategy
	filterCommandWithRetry bool
	idAllocator            *ConnIDAllocator
	dedup                  *DeDup
	lg                     *zap.Logger
}

// ConnIDAllocator allocates connection IDs for new connections.
// It uses the first 10bits to distinguish different decoders, and the last 54bits are auto-incremented.
type ConnIDAllocator struct {
	nextConnID atomic.Uint64
}

// NewConnIDAllocator creates a new ConnIDAllocator.
func NewConnIDAllocator(decoderID int) (*ConnIDAllocator, error) {
	if decoderID >= 1024 {
		return nil, errors.Errorf("decoderID %d is too large, must be less than 1024", decoderID)
	}
	alloc := &ConnIDAllocator{}
	alloc.nextConnID.Store(uint64(decoderID) << 54)
	return alloc, nil
}

func (c *ConnIDAllocator) alloc() uint64 {
	// TODO: handle the overflow case.
	// However, it may never happen in practice, because 54bits can represent 1.8e16 connections. If the connections
	// are created at a rate of 5k per second, it'll take 114k years to exhaust the IDs.
	return c.nextConnID.Add(1)
}

func (decoder *AuditLogPluginDecoder) Decode(reader LineReader) (*Command, error) {
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

		startTs, endTs, err := parseStartAndEndTs(kvs)
		if err != nil {
			return nil, errors.Wrapf(err, "%s, line %d", filename, lineIdx)
		}
		if startTs.Before(decoder.commandStartTime) {
			// Ignore the commands before CommandStartTime.
			continue
		}
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

		var cmds []*Command
		eventClass := kvs[auditPluginKeyClass]
		switch eventClass {
		case auditPluginClassGeneral, auditPluginClassTableAccess:
			cmds, err = decoder.parseGeneralEvent(kvs, startTs, endTs, upstreamConnID)
		case auditPluginClassConnect:
			var c *Command
			c, err = decoder.parseConnectEvent(kvs, upstreamConnID)
			if c != nil {
				cmds = []*Command{c}
			}
		default:
			return nil, errors.Errorf("%s, line %d: unknown event class: %s", filename, lineIdx, eventClass)
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
			cmd.StartTs = startTs
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

func (decoder *AuditLogPluginDecoder) SetCommandStartTime(t time.Time) {
	decoder.commandStartTime = t
}

func (decoder *AuditLogPluginDecoder) SetCommandEndTime(t time.Time) {
	decoder.commandEndTime = t
}

func (decoder *AuditLogPluginDecoder) SetPSCloseStrategy(s PSCloseStrategy) {
	decoder.psCloseStrategy = s
}

func (decoder *AuditLogPluginDecoder) SetIDAllocator(alloc *ConnIDAllocator) {
	decoder.idAllocator = alloc
}

// All SQL_TEXT are converted into one line in audit log.
func parseLog(kv map[string]string, line string) error {
	for idx := 0; idx < len(line); idx++ {
		switch line[idx] {
		case '[':
			key, value, endIdx, err := parseInBracket(line[idx+1:])
			if err != nil {
				return err
			}
			idx += endIdx + 1
			if len(key) > 0 {
				kv[key] = value
			}
		}
	}
	return nil
}

func parseInBracket(line string) (key, value string, idx int, err error) {
	valueStart := 0
	for ; idx < len(line); idx++ {
		switch line[idx] {
		case ']':
			value = line[valueStart:idx]
			return
		case '"', '\'':
			endIdx := skipQuotes(line[idx+1:], line[idx] == '\'')
			if endIdx == -1 {
				return "", "", len(line), errors.Errorf("unterminated quote in line: %s", line[idx+1:])
			}
			idx += endIdx + 1
		case '=':
			if idx == 0 {
				return "", "", idx, errors.Errorf("empty key in line: %s", line)
			}
			// only care about the first '='
			if len(key) == 0 {
				key = line[:idx]
				valueStart = idx + 1
			}
		}
	}
	return "", "", len(line), errors.Errorf("unterminated bracket in line: %s", line)
}

func skipQuotes(line string, singleQuote bool) (endIdx int) {
	for idx := 0; idx < len(line); idx++ {
		switch line[idx] {
		case '"':
			if !singleQuote {
				return idx
			}
		case '\'':
			if singleQuote {
				return idx
			}
		case '\\':
			idx++
		}
	}
	return -1
}

// [COMMAND="Init DB"], [COMMAND=Query]
func parseCommand(value string) string {
	if len(value) == 0 {
		return ""
	}
	if value[0] == '"' {
		var err error
		value, err = strconv.Unquote(value)
		// impossible
		if err != nil {
			return ""
		}
	}
	return value
}

func parseStartAndEndTs(kvs map[string]string) (time.Time, time.Time, error) {
	endTs, err := time.Parse(timeLayout, kvs[auditPluginKeyTimeStamp])
	if err != nil {
		return time.Time{}, time.Time{}, errors.Errorf("parsing timestamp failed: %s", kvs[auditPluginKeyTimeStamp])
	}
	costTime := kvs[auditPluginKeyCostTime]
	if len(costTime) == 0 {
		return endTs, endTs, nil
	}
	millis, err := strconv.ParseFloat(costTime, 32)
	if err != nil {
		return endTs, endTs, errors.Errorf("parsing cost time failed: %s", costTime)
	}
	return endTs.Add(-time.Duration(millis * 1000)), endTs, nil
}

func parseSQL(value string) (string, error) {
	if len(value) == 0 {
		return "", errors.New("empty sql")
	}
	if value[0] == '"' {
		return strconv.Unquote(value)
	}
	return value, nil
}

// "[\"KindInt64 1\",\"KindInt64 1\"]"
func parseExecuteParams(value string) ([]any, error) {
	v, err := strconv.Unquote(value)
	if err != nil {
		return nil, errors.Errorf("no quotes in params: %s", value)
	}
	if len(v) == 0 {
		return nil, nil
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
		case '"', '\'':
			endIdx := skipQuotes(v[idx+1:], v[idx] == '\'')
			if endIdx == -1 {
				return nil, errors.Errorf("unterminated quote in params: %s", v[idx+1:])
			}
			// The maximum possible value of `endIdx` is len(v[idx+1:]) - 1 = len(v) - idx - 2
			// So `v[idx : idx+endIdx+2]` will never cause out-of-bound error and it is correct
			// to contain the first and last quotes.
			paramEncodedStr, err := strconv.Unquote(v[idx : idx+endIdx+2])
			if err != nil {
				return nil, errors.Errorf("unquote param failed: %s", v[idx:idx+endIdx+2])
			}
			param, err := parseSingleParam(paramEncodedStr)
			idx += endIdx + 1
			if err != nil {
				return nil, err
			}
			params = append(params, param)
		case ',', ' ':
		default:
			return nil, errors.Errorf("expected char in params: %s", v[idx:])
		}
	}
	return params, nil
}

func parseSingleParam(value string) (any, error) {
	idx := strings.IndexByte(value, ' ')
	if idx < 0 {
		return nil, errors.Errorf("no space in param: %s", value)
	}
	tpStr := value[:idx]
	value = value[idx+1:]
	switch tpStr {
	case "KindNull":
		return nil, nil
	case "KindInt64":
		return strconv.ParseInt(value, 10, 64)
	case "KindUint64":
		return strconv.ParseUint(value, 10, 64)
	case "KindFloat32":
		return strconv.ParseFloat(value, 32)
	case "KindFloat64", "KindMysqlDecimal":
		return strconv.ParseFloat(value, 64)
	case "KindString":
		return strconv.Unquote(`"` + value + `"`)
	case "KindBinaryLiteral", "KindMysqlBit", "KindMysqlSet", "KindMysqlTime", "KindMysqlJSON":
		return value, nil
	case "KindBytes":
		str, err := strconv.Unquote(`"` + value + `"`)
		if err != nil {
			return nil, err
		}
		return hack.Slice(str), nil
	case "KindMysqlDuration", "KindMysqlEnum", "KindInterface", "KindMinNotNull", "KindMaxValue", "KindRaw":
		return nil, errors.Errorf("unsupported param type: %s", tpStr)
	}
	return nil, errors.Errorf("unknown param type: %s", tpStr)
}

func (decoder *AuditLogPluginDecoder) parseGeneralEvent(kvs map[string]string, startTs, endTs time.Time, connID uint64) ([]*Command, error) {
	connInfo := decoder.connInfo[connID]
	if connInfo.preparedStmt == nil {
		connInfo.preparedStmt = make(map[uint32]struct{})
		connInfo.preparedStmtSql = make(map[string]uint32)
	}
	event, ok := kvs[auditPluginKeyEvent]
	if !ok || event != auditPluginEventEnd {
		// Old version doesn't have the EVENT key.
		// The STARTING event is wrong, we only care about the COMPLETED event.
		return nil, nil
	}

	var sql string
	cmdStr := parseCommand(kvs[auditPluginKeyCommand])
	if cmdStr == "Query" || cmdStr == "Execute" {
		var err error
		sql, err = parseSQL(kvs[auditPluginKeySQL])
		if err != nil {
			return nil, errors.Wrapf(err, "unquote sql failed: %s", kvs[auditPluginKeySQL])
		}
		if decoder.filterCommandWithRetry {
			if retryStr, ok := kvs[auditPluginKeyRetry]; ok {
				if retryStr == "true" {
					// skip the retried command
					return nil, nil
				}
			}
		} else {
			// deduplicate DML and SELECT FOR UPDATE
			if decoder.isDuplicatedWrite(connInfo.lastCmd, kvs, cmdStr, sql, startTs, endTs) {
				return nil, nil
			}
		}
	}

	cmds := make([]*Command, 0, 3)
	switch cmdStr {
	case "Query":
		cmds = append(cmds, &Command{
			Type:     pnet.ComQuery,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  append([]byte{pnet.ComQuery.Byte()}, hack.Slice(sql)...),
		})
		connInfo.lastCmd = cmds[0]
	case "Close stmt":
		if decoder.psCloseStrategy != PSCloseStrategyDirected {
			break
		}
		stmtID, err := parseStmtID(kvs[auditPluginKeyPreparedStmtID])
		if err != nil {
			return nil, err
		}

		// If the statement was prepared before the command-start-time, do not close it.
		if _, ok := connInfo.preparedStmt[stmtID]; ok {
			delete(connInfo.preparedStmt, stmtID)
			decoder.connInfo[connID] = connInfo
			cmds = append(cmds, &Command{
				CapturedPsID: stmtID,
				Type:         pnet.ComStmtClose,
				StmtType:     kvs[auditPluginKeyStmtType],
				Payload:      pnet.MakeCloseStmtRequest(stmtID),
			})
		}
	case "Execute":
		params, ok := kvs[auditPluginKeyParams]
		if !ok {
			// the old format doesn't output params
			break
		}
		args, err := parseExecuteParams(params)
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
		case PSCloseStrategyDirected:
			stmtID, err = parseStmtID(kvs[auditPluginKeyPreparedStmtID])
			if err != nil {
				return nil, err
			}
			if _, ok := connInfo.preparedStmt[stmtID]; !ok {
				shouldPrepare = true
				connInfo.preparedStmt[stmtID] = struct{}{}
				decoder.connInfo[connID] = connInfo
			}
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
		// Ignore Quit since disconnection is handled in parseConnectEvent.
	}
	decoder.connInfo[connID] = connInfo
	return cmds, nil
}

func (decoder *AuditLogPluginDecoder) parseConnectEvent(kvs map[string]string, connID uint64) (*Command, error) {
	switch kvs[auditPluginKeySubClass] {
	case auditPluginSubClassDisconnect:
		delete(decoder.connInfo, connID)
		return &Command{
			Type:    pnet.ComQuit,
			Payload: []byte{pnet.ComQuit.Byte()},
		}, nil
	}
	return nil, nil
}

func parseStmtID(value string) (uint32, error) {
	if len(value) == 0 {
		return 0, errors.New("empty prepared stmt id")
	}
	id, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.Errorf("parsing prepared stmt id failed: %s", value)
	}
	return uint32(id), nil
}

// Transaction retrials will record the same SQL multiple times in the audit logs, so we need to deduplicate them.
func (decoder *AuditLogPluginDecoder) isDuplicatedWrite(lastCmd *Command, kvs map[string]string, cmdType, sql string, startTs, endTs time.Time) bool {
	if lastCmd == nil {
		return false
	}
	// Does the time overlap?
	if lastCmd.EndTs.Before(startTs) || startTs.Sub(lastCmd.StartTs) > time.Millisecond {
		return false
	}
	// Are the statements equal?
	switch lastCmd.Type {
	case pnet.ComStmtExecute:
		if cmdType != "Execute" || lastCmd.PreparedStmt != sql || lastCmd.kvs[auditPluginKeyParams] != kvs[auditPluginKeyParams] {
			return false
		}
	case pnet.ComQuery:
		if cmdType != "Query" || hack.String(lastCmd.Payload[1:]) != sql {
			return false
		}
	default:
		return false
	}
	if lastCmd.StmtType != kvs[auditPluginKeyStmtType] {
		return false
	}
	// Is it DML?
	switch lastCmd.StmtType {
	case "Insert", "Update", "Delete", "Replace":
	case "Select":
		// The judgment is inaccurate, but just make it simple.
		if !strings.Contains(strings.ToLower(sql), "for update") {
			return false
		}
	default:
		return false
	}
	// Record the deduplication.
	decoder.dedup.Lock()
	dedup := decoder.dedup.Items[lastCmd.StmtType]
	dedup.Times++
	dedup.Cost += endTs.Sub(startTs)
	overlap := lastCmd.EndTs.Sub(startTs)
	if dedup.MinOverlap == 0 {
		dedup.MinOverlap = overlap
	} else if dedup.MinOverlap > overlap {
		dedup.MinOverlap = overlap
	}
	decoder.dedup.Items[lastCmd.StmtType] = dedup
	decoder.dedup.Unlock()
	return true
}

// EnableFilterCommandWithRetry enables filtering out commands that are retries according to the audit log.
func (decoder *AuditLogPluginDecoder) EnableFilterCommandWithRetry() {
	decoder.filterCommandWithRetry = true
}
