// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

const (
	FormatNative         = "native"
	FormatAuditLogPlugin = "audit_log_plugin"
)

type LineReader interface {
	String() string
	ReadLine() ([]byte, string, int, error)
	Read([]byte) (string, int, error)
	Close()
}

func NewCmdEncoder(_ string) CmdEncoder {
	// Only support writing native format
	return NewNativeEncoder()
}

type CmdEncoder interface {
	Encode(c *Command, writer *bytes.Buffer) error
}

func NewCmdDecoder(format string) CmdDecoder {
	switch format {
	case FormatAuditLogPlugin:
		return NewAuditLogPluginDecoder()
	default:
		return NewNativeDecoder()
	}
}

type CmdDecoder interface {
	Decode(reader LineReader) (c *Command, err error)

	SetCommandStartTime(t time.Time)
}

type Command struct {
	PreparedStmt string
	// CapturedPsID is the prepared statement ID in capture.
	// The Execute command needs to update the prepared statement ID in replay.
	CapturedPsID uint32
	Params       []any
	digest       string
	// Payload starts with command type so that replay can reuse this byte array.
	Payload []byte
	StartTs time.Time
	// For audit log plugin, the decoder will allocate a new id to avoid id collision. To make it easier
	// to debug, we keep the upstream connection id here to store it in the exception report.
	UpstreamConnID uint64
	ConnID         uint64
	Type           pnet.Command
	// When the replay connection disconnects, the replayer reconnects based on the current DB of the next command.
	CurDB string
	// The place in the traffic file, used to report.
	FileName string
	Line     int
	// Logged only in audit log.
	StmtType string
	EndTs    time.Time
	Content  string
	// Logged only in native log.
	Success bool
}

func NewCommand(packet []byte, startTs time.Time, connID uint64) *Command {
	if len(packet) == 0 {
		return nil
	}
	// TODO: handle load infile specially
	return &Command{
		Payload: packet,
		StartTs: startTs,
		ConnID:  connID,
		Type:    pnet.Command(packet[0]),
		Success: true,
	}
}

func (c *Command) Equal(that *Command) bool {
	if that == nil {
		return false
	}
	return c.StartTs.Equal(that.StartTs) &&
		c.ConnID == that.ConnID &&
		c.Type == that.Type &&
		c.Success == that.Success &&
		bytes.Equal(c.Payload, that.Payload)
}

func (c *Command) Validate(filename string, lineIdx int) error {
	if c.StartTs.IsZero() {
		return errors.Errorf("%s, line %d: no start time", filename, lineIdx)
	}
	if c.ConnID == 0 {
		return errors.Errorf("%s, line %d: no connection id", filename, lineIdx)
	}
	if len(c.Payload) == 0 {
		return errors.Errorf("%s, line %d: no payload", filename, lineIdx)
	}
	return nil
}

func (c *Command) Digest() string {
	if len(c.digest) == 0 {
		switch c.Type {
		case pnet.ComQuery, pnet.ComStmtPrepare:
			stmt := hack.String(c.Payload[1:])
			_, digest := parser.NormalizeDigest(stmt)
			c.digest = digest.String()
		case pnet.ComStmtExecute, pnet.ComStmtClose, pnet.ComStmtSendLongData, pnet.ComStmtReset, pnet.ComStmtFetch:
			_, digest := parser.NormalizeDigest(c.PreparedStmt)
			c.digest = digest.String()
		}
	}
	return c.digest
}

func (c *Command) QueryText() string {
	switch c.Type {
	case pnet.ComQuery, pnet.ComStmtPrepare:
		return hack.String(c.Payload[1:])
	case pnet.ComStmtExecute:
		return fmt.Sprintf("%s params=%v", c.PreparedStmt, c.Params)
	case pnet.ComStmtClose, pnet.ComStmtSendLongData, pnet.ComStmtReset, pnet.ComStmtFetch:
		return c.PreparedStmt
	}
	return ""
}
