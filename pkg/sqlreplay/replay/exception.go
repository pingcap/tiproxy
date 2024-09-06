// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

type Exception interface {
	ConnID() uint64
	String() string
}

type otherException struct {
	err    error
	connID uint64
}

func (he otherException) ConnID() uint64 {
	return he.connID
}

func (he otherException) String() string {
	return he.err.Error()
}

type failException struct {
	err     error
	command *cmd.Command
}

func (fe failException) ConnID() uint64 {
	return fe.command.ConnID
}

func (fe failException) String() string {
	return fe.err.Error()
}
