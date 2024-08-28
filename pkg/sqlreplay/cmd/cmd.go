// Copyright 2024 PingCAP, Inc.
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
	commonKeyPrefix = "# "
	commonKeySuffix = ": "
	keyStartTs      = "# Time: "
	keyConnID       = "# Conn_ID: "
	keyType         = "# Cmd_type: "
	keySuccess      = "# Success: "
	keyPayloadLen   = "# Payload_len: "
)

type LineReader interface {
	ReadLine() ([]byte, error)
	Read(n int) ([]byte, error)
}

type Command struct {
	Payload  []byte
	StartTs  time.Time
	ConnID   uint64
	Type     pnet.Command
	Succeess bool
}

func NewCommand(packet []byte, startTs time.Time, connID uint64) *Command {
	if len(packet) == 0 {
		return nil
	}
	// TODO: handle load infile specially
	return &Command{
		Payload:  packet[1:],
		StartTs:  startTs,
		ConnID:   connID,
		Type:     pnet.Command(packet[0]),
		Succeess: true,
	}
}

func (c *Command) Equal(that *Command) bool {
	if that == nil {
		return false
	}
	return c.StartTs.Equal(that.StartTs) &&
		c.ConnID == that.ConnID &&
		c.Type == that.Type &&
		c.Succeess == that.Succeess &&
		bytes.Equal(c.Payload, that.Payload)
}

func (c *Command) Validate() error {
	if c.StartTs.IsZero() {
		return errors.Errorf("no start time")
	}
	if c.ConnID == 0 {
		return errors.Errorf("no connection id")
	}
	if c.Type == pnet.ComQuery && len(c.Payload) == 0 {
		return errors.Errorf("no query")
	}
	return nil
}

func (c *Command) Encode(writer *bytes.Buffer) error {
	var err error
	if err = writeString(keyStartTs, c.StartTs.Format(time.RFC3339Nano), writer); err != nil {
		return nil
	}
	if err = writeString(keyConnID, strconv.FormatUint(c.ConnID, 10), writer); err != nil {
		return nil
	}
	if c.Type != pnet.ComQuery {
		if err = writeByte(keyType, c.Type.Byte(), writer); err != nil {
			return nil
		}
	}
	if !c.Succeess {
		if err = writeString(keySuccess, "false", writer); err != nil {
			return nil
		}
	}
	if err = writeString(keyPayloadLen, strconv.Itoa(len(c.Payload)), writer); err != nil {
		return nil
	}
	// Unlike TiDB slow log, the payload is binary because StmtExecute can't be transformed to a SQL.
	if len(c.Payload) > 0 {
		if _, err = writer.Write(c.Payload); err != nil {
			return err
		}
		if _, err = writer.WriteRune('\n'); err != nil {
			return err
		}
	}
	return nil
}

func (c *Command) Decode(reader LineReader) error {
	var payloadLen int
	c.Succeess = true
	c.Type = pnet.ComQuery
	for {
		line, err := reader.ReadLine()
		if err != nil {
			return errors.WithStack(err)
		}
		if !strings.HasPrefix(hack.String(line), commonKeyPrefix) {
			return errors.Errorf("line doesn't start with '%s': %s", commonKeyPrefix, line)
		}
		idx := strings.Index(hack.String(line), commonKeySuffix)
		if idx < 0 {
			return errors.Errorf("'%s' is not found in line: %s", commonKeySuffix, line)
		}
		idx += len(commonKeySuffix)
		key := hack.String(line[:idx])
		value := hack.String(line[idx:])
		if len(value) == 0 {
			return errors.Errorf("value is empty in line: %s", line)
		}
		switch key {
		case keyStartTs:
			if !c.StartTs.IsZero() {
				return errors.Errorf("redundant Time: %s, Time was %v", line, c.StartTs)
			}
			c.StartTs, err = time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return errors.Errorf("parsing Time failed: %s", line)
			}
		case keyConnID:
			if c.ConnID > 0 {
				return errors.Errorf("redundant Conn_ID: %s, Conn_ID was %d", line, c.ConnID)
			}
			c.ConnID, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return errors.Errorf("parsing Conn_ID failed: %s", line)
			}
		case keyType:
			if c.Type != pnet.ComQuery {
				return errors.Errorf("redundant Cmd_type: %s, Cmd_type was %v", line, c.Type)
			}
			c.Type = pnet.Command(value[0])
		case keySuccess:
			c.Succeess = value == "true"
		case keyPayloadLen:
			payloadLen, err = strconv.Atoi(value)
			if err != nil {
				return errors.Errorf("parsing Payload_len failed: %s", line)
			}
			if payloadLen > 0 {
				if c.Payload, err = reader.Read(payloadLen); err != nil {
					return errors.WithStack(err)
				}
				// skip '\n'
				if _, err = reader.Read(1); err != nil {
					return errors.WithStack(err)
				}
			}
			if err = c.Validate(); err != nil {
				return err
			}
			return nil
		}
	}
}

func writeString(key, value string, writer *bytes.Buffer) error {
	var err error
	if _, err = writer.WriteString(key); err != nil {
		return err
	}
	if _, err = writer.WriteString(value); err != nil {
		return err
	}
	if _, err = writer.WriteRune('\n'); err != nil {
		return err
	}
	return nil
}

func writeByte(key string, value byte, writer *bytes.Buffer) error {
	var err error
	if _, err = writer.WriteString(key); err != nil {
		return err
	}
	if err = writer.WriteByte(value); err != nil {
		return err
	}
	if _, err = writer.WriteRune('\n'); err != nil {
		return err
	}
	return nil
}
