// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

const (
	auditPluginKeyTimeStamp = "TIMESTAMP"
	auditPluginKeyDatabase  = "DATABASES"
	auditPluginKeySQL       = "SQL_TEXT"
	auditPluginKeyConnID    = "CONNECTION_ID"
	auditPluginKeyClass     = "EVENT_CLASS"
	auditPluginKeySubClass  = "EVENT_SUBCLASS"
	auditPluginKeyCommand   = "COMMAND"
	auditPluginKeyStmtType  = "SQL_STATEMENTS"

	auditPluginClassGeneral     = "GENERAL"
	auditPluginClassTableAccess = "TABLE_ACCESS"
	auditPluginClassConnect     = "CONNECTION"

	auditPluginSubClassConnected  = "Connected"
	auditPluginSubClassDisconnect = "Disconnect"

	timeLayout = "2006/01/02 15:04:05.999 -07:00"
)

type auditLogPluginConnCtx struct {
	beginCmd *Command
	inited   bool
}

func NewAuditLogPluginDecoder() *AuditLogPluginDecoder {
	return &AuditLogPluginDecoder{
		connInfo: make(map[uint64]auditLogPluginConnCtx),
	}
}

var _ CmdDecoder = (*AuditLogPluginDecoder)(nil)

type AuditLogPluginDecoder struct {
	connInfo map[uint64]auditLogPluginConnCtx
}

func (decoder *AuditLogPluginDecoder) Decode(reader LineReader) (*Command, error) {
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
		tsStr := kvs[auditPluginKeyTimeStamp]
		if len(tsStr) == 0 {
			return nil, errors.Errorf("%s, line %d: no timestamp in line: '%s", filename, lineIdx, line)
		}
		startTs, err := time.Parse(timeLayout, tsStr)
		if err != nil {
			return nil, errors.Errorf("%s, line %d: parsing timestamp failed: %s", filename, lineIdx, tsStr)
		}
		var c *Command
		eventClass := kvs[auditPluginKeyClass]
		switch eventClass {
		case auditPluginClassGeneral, auditPluginClassTableAccess:
			c, err = decoder.parseGeneralEvent(kvs, connID)
		case auditPluginClassConnect:
			c, err = decoder.parseConnectEvent(kvs, connID)
		default:
			return nil, errors.Errorf("%s, line %d: unknown event class: %s", filename, lineIdx, eventClass)
		}
		if err != nil {
			return c, err
		}
		// The log is ignored, skip.
		if c == nil {
			continue
		}
		c.Succeess = true
		c.ConnID = connID
		c.StartTs = startTs
		return c, nil
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

// [DATABASES="[test]"]
func parseDB(value string) []string {
	var err error
	value, err = strconv.Unquote(value)
	if err != nil {
		return nil
	}
	if len(value) == 0 {
		return nil
	}
	if value[0] != '[' || value[len(value)-1] != ']' {
		// impossible
		return nil
	}
	value = value[1 : len(value)-1]
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
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

func (decoder *AuditLogPluginDecoder) parseGeneralEvent(kvs map[string]string, connID uint64) (*Command, error) {
	connInfo := decoder.connInfo[connID]
	var cmd *Command
	cmdStr := parseCommand(kvs[auditPluginKeyCommand])
	switch cmdStr {
	case "Query", "Init DB":
		sql, err := strconv.Unquote(kvs[auditPluginKeySQL])
		if err != nil {
			return nil, errors.Wrapf(err, "unquote sql failed: %s", kvs[auditPluginKeySQL])
			// We also ignore "Quit" since disconnection is handled in parseConnectEvent.
		}
		cmd = &Command{
			Type:     pnet.ComQuery,
			StmtType: kvs[auditPluginKeyStmtType],
			Payload:  append([]byte{pnet.ComQuery.Byte()}, hack.Slice(sql)...),
		}
		// Ignore StmtExecute since the params are not outputted.
		// Ignore Quit since disconnection is handled in parseConnectEvent.
	}
	// Audit logs record both the beginning and end of each statement, but we only need the first one.
	if cmd == nil {
		return nil, nil
	}
	if connInfo.beginCmd != nil && bytes.Equal(cmd.Payload, connInfo.beginCmd.Payload) {
		cmd = nil
	} else if !connInfo.inited && cmd.StmtType == "Use" {
		connInfo.inited = true
	} else if kvs[auditPluginKeyClass] == auditPluginClassTableAccess && !connInfo.inited {
		cmd = nil
	}
	connInfo.beginCmd = cmd
	decoder.connInfo[connID] = connInfo
	return cmd, nil
}

func (decoder *AuditLogPluginDecoder) parseConnectEvent(kvs map[string]string, connID uint64) (*Command, error) {
	switch kvs[auditPluginKeySubClass] {
	case auditPluginSubClassConnected:
		// The connection is treated as initialized no matter the current db is set or not.
		connInfo := decoder.connInfo[connID]
		connInfo.inited = true
		decoder.connInfo[connID] = connInfo

		db := kvs[auditPluginKeyDatabase]
		dbs := parseDB(db)
		if len(dbs) == 1 {
			return &Command{
				Type:    pnet.ComInitDB,
				Payload: append([]byte{pnet.ComInitDB.Byte()}, hack.Slice(dbs[0])...),
			}, nil
		}
		return nil, nil
	case auditPluginSubClassDisconnect:
		return &Command{
			Type:    pnet.ComQuit,
			Payload: []byte{pnet.ComQuit.Byte()},
		}, nil
	}
	return nil, nil
}
