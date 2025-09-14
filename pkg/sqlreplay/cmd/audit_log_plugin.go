// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

const (
	auditPluginKeyTimeStamp = "TIMESTAMP"
	auditPluginKeySQL       = "SQL_TEXT"
	auditPluginKeyConnID    = "CONNECTION_ID"
	auditPluginKeyClass     = "EVENT_CLASS"
	auditPluginKeySubClass  = "EVENT_SUBCLASS"
	auditPluginKeyCommand   = "COMMAND"
	auditPluginKeyStmtType  = "SQL_STATEMENTS"
	auditPluginKeyParams    = "EXECUTE_PARAMS"
	auditPluginKeyCurDB     = "CURRENT_DB"
	auditPluginKeyEvent     = "EVENT"
	auditPluginKeyCostTime  = "COST_TIME"

	auditPluginClassGeneral     = "GENERAL"
	auditPluginClassTableAccess = "TABLE_ACCESS"
	auditPluginClassConnect     = "CONNECTION"

	auditPluginSubClassConnected  = "Connected"
	auditPluginSubClassDisconnect = "Disconnect"

	auditPluginEventEnd = "COMPLETED"

	timeLayout = "2006/01/02 15:04:05.999 -07:00"
)

type auditLogPluginConnCtx struct {
	currentDB string
}

func NewAuditLogPluginDecoder() *AuditLogPluginDecoder {
	return &AuditLogPluginDecoder{
		connInfo: make(map[uint64]auditLogPluginConnCtx),
	}
}

var _ CmdDecoder = (*AuditLogPluginDecoder)(nil)

type AuditLogPluginDecoder struct {
	connInfo map[uint64]auditLogPluginConnCtx
	// pendingCmds contains the commands that has not been returned yet.
	pendingCmds []*Command
}

func (decoder *AuditLogPluginDecoder) Decode(reader LineReader) (*Command, error) {
	if len(decoder.pendingCmds) > 0 {
		cmd := decoder.pendingCmds[0]
		decoder.pendingCmds = decoder.pendingCmds[1:]
		return cmd, nil
	}
	for {
		line, filename, lineIdx, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		kvs, err := parseLog(hack.String(line))
		if err != nil {
			return nil, errors.Errorf("%s, line %d: %s", filename, lineIdx, err.Error())
		}
		connStr := kvs[auditPluginKeyConnID]
		if len(connStr) == 0 {
			return nil, errors.Errorf("%s, line %d: no connection id in line: %s", filename, lineIdx, line)
		}
		connID, err := strconv.ParseUint(connStr, 10, 64)
		if err != nil {
			return nil, errors.Errorf("%s, line %d: parsing connection id failed: %s", filename, lineIdx, connStr)
		}

		var cmds []*Command
		eventClass := kvs[auditPluginKeyClass]
		switch eventClass {
		case auditPluginClassGeneral, auditPluginClassTableAccess:
			cmds, err = decoder.parseGeneralEvent(kvs, connID)
		case auditPluginClassConnect:
			var c *Command
			c, err = decoder.parseConnectEvent(kvs, connID)
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

		startTs, err := parseStartTs(kvs)
		if err != nil {
			return nil, errors.Wrapf(err, "%s, line %d", filename, lineIdx)
		}
		for _, cmd := range cmds {
			cmd.Success = true
			cmd.ConnID = connID
			cmd.StartTs = startTs
		}
		if len(cmds) > 1 {
			decoder.pendingCmds = cmds[1:]
		}
		return cmds[0], nil
	}
}

// All SQL_TEXT are converted into one line in audit log.
func parseLog(line string) (map[string]string, error) {
	kv := make(map[string]string)
	for idx := 0; idx < len(line); idx++ {
		switch line[idx] {
		case '[':
			key, value, endIdx, err := parseInBracket(line[idx+1:])
			if err != nil {
				return kv, err
			}
			idx += endIdx + 1
			if len(key) > 0 {
				kv[key] = value
			}
		}
	}
	return kv, nil
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

func parseStartTs(kvs map[string]string) (time.Time, error) {
	endTs, err := time.Parse(timeLayout, kvs[auditPluginKeyTimeStamp])
	if err != nil {
		return time.Time{}, errors.Errorf("parsing timestamp failed: %s", kvs[auditPluginKeyTimeStamp])
	}
	costTime := kvs[auditPluginKeyCostTime]
	if len(costTime) == 0 {
		return endTs, nil
	}
	millis, err := strconv.ParseFloat(costTime, 32)
	if err != nil {
		return endTs, errors.Errorf("parsing cost time failed: %s", costTime)
	}
	return endTs.Add(-time.Duration(millis * 1000)), nil
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
			param, err := parseSingleParam(v[idx+1 : idx+endIdx+1])
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
	case "KindString", "KindBinaryLiteral", "KindMysqlBit", "KindMysqlSet", "KindMysqlTime", "KindMysqlJSON":
		return value, nil
	case "KindBytes":
		return hack.Slice(value), nil
	case "KindMysqlDuration", "KindMysqlEnum", "KindInterface", "KindMinNotNull", "KindMaxValue", "KindRaw":
		return nil, errors.Errorf("unsupported param type: %s", tpStr)
	}
	return nil, errors.Errorf("unknown param type: %s", tpStr)
}

func (decoder *AuditLogPluginDecoder) parseGeneralEvent(kvs map[string]string, connID uint64) ([]*Command, error) {
	connInfo := decoder.connInfo[connID]
	event, ok := kvs[auditPluginKeyEvent]
	if !ok || event != auditPluginEventEnd {
		// Old version doesn't have the EVENT key.
		// The STARTING event is wrong, we only care about the COMPLETED event.
		return nil, nil
	}

	cmdStr := parseCommand(kvs[auditPluginKeyCommand])
	cmds := make([]*Command, 0, 4)
	db := kvs[auditPluginKeyCurDB]
	if len(db) > 0 && db != connInfo.currentDB {
		cmds = append(cmds, &Command{
			Type:    pnet.ComInitDB,
			Payload: pnet.MakeInitDBRequest(db),
		})
		connInfo.currentDB = db
		decoder.connInfo[connID] = connInfo
	}

	switch cmdStr {
	case "Query", "Init DB":
		sql, err := strconv.Unquote(kvs[auditPluginKeySQL])
		if err != nil {
			return nil, errors.Wrapf(err, "unquote sql failed: %s", kvs[auditPluginKeySQL])
		}
		cmds = append(cmds, &Command{
			Type:     pnet.ComQuery,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  append([]byte{pnet.ComQuery.Byte()}, hack.Slice(sql)...),
		})
	case "Execute":
		params, ok := kvs[auditPluginKeyParams]
		if !ok {
			// the old format doesn't output params
			break
		}
		sql, err := strconv.Unquote(kvs[auditPluginKeySQL])
		if err != nil {
			return nil, errors.Wrapf(err, "unquote sql failed: %s", kvs[auditPluginKeySQL])
		}
		args, err := parseExecuteParams(params)
		if err != nil {
			return nil, err
		}
		executeReq, err := pnet.MakeExecuteStmtRequest(0, args, true)
		if err != nil {
			return nil, errors.Wrapf(err, "make execute request failed")
		}
		cmds = append(cmds, &Command{
			Type:     pnet.ComStmtPrepare,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  append([]byte{pnet.ComStmtPrepare.Byte()}, hack.Slice(sql)...),
		}, &Command{
			Type:     pnet.ComStmtExecute,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  executeReq,
		}, &Command{
			Type:     pnet.ComStmtClose,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  pnet.MakeCloseStmtRequest(0),
		})
		// Ignore Quit since disconnection is handled in parseConnectEvent.
	}
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
