// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	"errors"
	"time"

	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/siddontang/go/hack"
)

type ExceptionType int

const (
	// connect error, handshake error
	Other ExceptionType = iota
	// execute error
	Fail
	// the error type count
	Total
)

func (t ExceptionType) String() string {
	return [...]string{
		"Other",
		"Fail",
	}[t]
}

type Exception interface {
	Type() ExceptionType
	Key() string
	ConnID() uint64
	Time() time.Time
}

type OtherException struct {
	err    error
	connID uint64
	ts     time.Time
}

func NewOtherException(err error, connID uint64) *OtherException {
	return &OtherException{
		err:    err,
		connID: connID,
		ts:     time.Now(),
	}
}

func (oe *OtherException) Type() ExceptionType {
	return Other
}

func (oe *OtherException) Key() string {
	internal := errors.Unwrap(oe.err)
	if internal == nil {
		return oe.err.Error()
	}
	return internal.Error()
}

func (oe *OtherException) ConnID() uint64 {
	return oe.connID
}

func (oe *OtherException) Error() string {
	return oe.err.Error()
}

func (oe *OtherException) Time() time.Time {
	return oe.ts
}

type FailException struct {
	key     string
	err     error
	ts      time.Time
	command *cmd.Command
}

func NewFailException(err error, command *cmd.Command) *FailException {
	fail := &FailException{
		err:     err,
		command: command,
		ts:      time.Now(),
	}
	var b []byte
	switch command.Type {
	case pnet.ComQuery, pnet.ComStmtPrepare, pnet.ComStmtExecute, pnet.ComStmtClose, pnet.ComStmtSendLongData,
		pnet.ComStmtReset, pnet.ComStmtFetch:
		digest := command.Digest()
		b = make([]byte, 1+len(digest))
		b[0] = command.Type.Byte()
		copy(b[1:], hack.Slice(digest))
	default:
		b = []byte{command.Type.Byte()}
	}
	fail.key = hack.String(b)
	return fail
}

func (fe *FailException) Type() ExceptionType {
	return Fail
}

func (fe *FailException) Key() string {
	return fe.key
}

func (fe *FailException) ConnID() uint64 {
	return fe.command.ConnID
}

func (fe *FailException) Time() time.Time {
	return fe.ts
}

func (fe *FailException) Command() *cmd.Command {
	return fe.command
}

func (fe *FailException) Error() string {
	return fe.err.Error()
}
