// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

const (
	nativeCommonKeyPrefix = "# "
	nativeCommonKeySuffix = ": "
	nativeKeyStartTs      = "# Time: "
	nativeKeyConnID       = "# Conn_ID: "
	nativeKeyType         = "# Cmd_type: "
	nativeKeySuccess      = "# Success: "
	nativeKeyCapturedPsID = "# Captured_ps_id: "
	nativeKeyPreparedStmt = "# Prepared_stmt: "
	nativeKeyPayloadLen   = "# Payload_len: "
)

func NewNativeEncoder() *NativeEncoder {
	// TODO: handle load infile specially
	return &NativeEncoder{}
}

var _ CmdEncoder = (*NativeEncoder)(nil)

type NativeEncoder struct {
}

func (rw *NativeEncoder) Encode(c *Command, writer *bytes.Buffer) error {
	var err error
	if err = writeString(nativeKeyStartTs, c.StartTs.Format(time.RFC3339Nano), writer); err != nil {
		return err
	}
	if err = writeString(nativeKeyConnID, strconv.FormatUint(c.ConnID, 10), writer); err != nil {
		return err
	}
	if c.Type != pnet.ComQuery {
		if err = writeString(nativeKeyType, c.Type.String(), writer); err != nil {
			return err
		}
	}
	if !c.Success {
		if err = writeString(nativeKeySuccess, "false", writer); err != nil {
			return err
		}
	}
	if c.CapturedPsID != 0 {
		if err = writeString(nativeKeyCapturedPsID, strconv.FormatUint(uint64(c.CapturedPsID), 10), writer); err != nil {
			return err
		}
	}
	if len(c.PreparedStmt) > 0 {
		// Use quoted string so it can be stored in a single line, e.g. it may contain '\n'.
		if err = writeString(nativeKeyPreparedStmt, strconv.Quote(c.PreparedStmt), writer); err != nil {
			return err
		}
	}
	// `Payload_len` doesn't include the command type.
	if err = writeString(nativeKeyPayloadLen, strconv.Itoa(len(c.Payload[1:])), writer); err != nil {
		return err
	}
	// Unlike TiDB slow log, the payload is binary because StmtExecute can't be transformed to a SQL.
	if len(c.Payload) > 1 {
		if _, err = writer.Write(c.Payload[1:]); err != nil {
			return errors.WithStack(err)
		}
	}
	if err = writer.WriteByte('\n'); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewNativeDecoder() *NativeDecoder {
	// TODO: handle load infile specially
	return &NativeDecoder{}
}

var _ CmdDecoder = (*NativeDecoder)(nil)

type NativeDecoder struct {
	commandStartTime time.Time
}

func (rw *NativeDecoder) Decode(reader LineReader) (c *Command, err error) {
	c = &Command{}
	c.Success = true
	c.Type = pnet.ComQuery

	var skipThisCommand bool
	for {
		line, filename, lineIdx, err := reader.ReadLine()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(hack.String(line), nativeCommonKeyPrefix) {
			return nil, errors.Errorf("%s, line %d: line doesn't start with '%s': %s", filename, lineIdx, nativeCommonKeyPrefix, line)
		}
		idx := strings.Index(hack.String(line), nativeCommonKeySuffix)
		if idx < 0 {
			return nil, errors.Errorf("%s, line %d: '%s' is not found in line: %s", filename, lineIdx, nativeCommonKeySuffix, line)
		}
		idx += len(nativeCommonKeySuffix)
		key := hack.String(line[:idx])
		value := hack.String(line[idx:])
		if len(value) == 0 {
			return nil, errors.Errorf("%s, line %d: value is empty in line: %s", filename, lineIdx, line)
		}
		switch key {
		case nativeKeyStartTs:
			if !c.StartTs.IsZero() {
				return nil, errors.Errorf("%s, line %d: redundant Time: %s, Time was %v", filename, lineIdx, line, c.StartTs)
			}
			c.StartTs, err = time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return nil, errors.Errorf("%s, line %d: parsing Time failed: %s", filename, lineIdx, line)
			}

			if c.StartTs.Before(rw.commandStartTime) || c.StartTs.Equal(rw.commandStartTime) {
				skipThisCommand = true
			}
		case nativeKeyConnID:
			if c.ConnID > 0 {
				return nil, errors.Errorf("%s, line %d: redundant Conn_ID: %s, Conn_ID was %d", filename, lineIdx, line, c.ConnID)
			}
			c.ConnID, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, errors.Errorf("%s, line %d: parsing Conn_ID failed: %s", filename, lineIdx, line)
			}
		case nativeKeyType:
			if c.Type != pnet.ComQuery {
				return nil, errors.Errorf("%s, line %d: redundant Cmd_type: %s, Cmd_type was %v", filename, lineIdx, line, c.Type)
			}
			c.Type = pnet.CommandFromString(value)
		case nativeKeySuccess:
			c.Success = value == "true"
		case nativeKeyCapturedPsID:
			id, parseErr := strconv.ParseUint(value, 10, 32)
			if parseErr != nil {
				return nil, errors.Errorf("%s, line %d: parsing Captured_ps_id failed: %s", filename, lineIdx, line)
			}
			c.CapturedPsID = uint32(id)
		case nativeKeyPreparedStmt:
			stmt, unqErr := strconv.Unquote(value)
			if unqErr != nil {
				return nil, errors.Errorf("%s, line %d: unquoting Prepared_stmt failed: %s", filename, lineIdx, line)
			}
			c.PreparedStmt = stmt
		case nativeKeyPayloadLen:
			var payloadLen int
			if payloadLen, err = strconv.Atoi(value); err != nil {
				return nil, errors.Errorf("parsing Payload_len failed: %s", line)
			}
			c.Payload = make([]byte, payloadLen+1)
			c.Payload[0] = c.Type.Byte()
			if payloadLen > 0 {
				if filename, lineIdx, err = reader.Read(c.Payload[1:]); err != nil {
					return nil, errors.Errorf("%s, line %d: reading Payload failed: %s", filename, lineIdx, err.Error())
				}
			}
			// skip '\n'
			var data [1]byte
			if filename, lineIdx, err = reader.Read(data[:]); err != nil {
				if !errors.Is(err, io.EOF) {
					return nil, errors.Errorf("%s, line %d: skipping new line failed: %s", filename, lineIdx, err.Error())
				}
				return nil, err
			}
			if data[0] != '\n' {
				return nil, errors.Errorf("%s, line %d: expected new line, but got: %s", filename, lineIdx, line)
			}

			if skipThisCommand {
				c = &Command{}
				c.Success = true
				c.Type = pnet.ComQuery
				skipThisCommand = false
				// skip the payload
				continue
			}
			if err = c.Validate(filename, lineIdx); err != nil {
				return nil, err
			}
			c.FileName = filename
			c.Line = lineIdx
			return c, nil
		}
	}
}

func (rw *NativeDecoder) SetCommandStartTime(t time.Time) {
	rw.commandStartTime = t
}

func writeString(key, value string, writer *bytes.Buffer) error {
	var err error
	if _, err = writer.WriteString(key); err != nil {
		return errors.WithStack(err)
	}
	if _, err = writer.WriteString(value); err != nil {
		return errors.WithStack(err)
	}
	if err = writer.WriteByte('\n'); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
