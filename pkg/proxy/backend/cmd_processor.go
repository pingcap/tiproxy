// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"encoding/binary"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
)

const (
	StatusInTrans uint32 = 1 << iota
	StatusQuit
	StatusPrepareWaitExecute
	StatusPrepareWaitFetch
)

// CmdProcessor maintains the transaction and prepared statement status and decides whether the session can be redirected.
type CmdProcessor struct {
	// Each prepared statement has an independent status.
	preparedStmtStatus map[int]uint32
	capability         uint32
	// Only includes in_trans or quit status.
	serverStatus uint32
}

func NewCmdProcessor() *CmdProcessor {
	return &CmdProcessor{
		serverStatus:       0,
		preparedStmtStatus: make(map[int]uint32),
	}
}

func (cp *CmdProcessor) handleOKPacket(request, response []byte) *gomysql.Result {
	r := pnet.ParseOKPacket(response)
	cp.updateServerStatus(request, r.Status)
	return r
}

func (cp *CmdProcessor) handleErrorPacket(data []byte) error {
	return pnet.ParseErrorPacket(data)
}

func (cp *CmdProcessor) handleEOFPacket(request, response []byte) uint16 {
	serverStatus := binary.LittleEndian.Uint16(response[3:])
	cp.updateServerStatus(request, serverStatus)
	return serverStatus
}

func (cp *CmdProcessor) updateServerStatus(request []byte, serverStatus uint16) {
	cp.updateTxnStatus(serverStatus)
	cp.updatePrepStmtStatus(request, serverStatus)
}

func (cp *CmdProcessor) updateTxnStatus(serverStatus uint16) {
	if serverStatus&mysql.ServerStatusInTrans > 0 {
		cp.serverStatus |= StatusInTrans
	} else {
		cp.serverStatus &^= StatusInTrans
	}
}

func (cp *CmdProcessor) updatePrepStmtStatus(request []byte, serverStatus uint16) {
	var (
		stmtID         int
		prepStmtStatus uint32
	)
	cmd := request[0]
	switch cmd {
	case mysql.ComStmtSendLongData, mysql.ComStmtExecute, mysql.ComStmtFetch, mysql.ComStmtReset, mysql.ComStmtClose:
		stmtID = int(binary.LittleEndian.Uint32(request[1:5]))
	case mysql.ComResetConnection, mysql.ComChangeUser:
		cp.preparedStmtStatus = make(map[int]uint32)
		return
	default:
		return
	}
	switch cmd {
	case mysql.ComStmtSendLongData:
		prepStmtStatus = StatusPrepareWaitExecute
	case mysql.ComStmtExecute:
		if serverStatus&mysql.ServerStatusCursorExists > 0 {
			prepStmtStatus = StatusPrepareWaitFetch
		}
	case mysql.ComStmtFetch:
		if serverStatus&mysql.ServerStatusLastRowSend == 0 {
			prepStmtStatus = StatusPrepareWaitFetch
		}
	}
	if prepStmtStatus > 0 {
		cp.preparedStmtStatus[stmtID] = prepStmtStatus
	} else {
		delete(cp.preparedStmtStatus, stmtID)
	}
}

func (cp *CmdProcessor) finishedTxn() bool {
	if cp.serverStatus&(StatusInTrans|StatusQuit) > 0 {
		return false
	}
	// If any result of the prepared statements is not fetched, we should wait.
	return !cp.hasPendingPreparedStmts()
}

func (cp *CmdProcessor) hasPendingPreparedStmts() bool {
	for _, serverStatus := range cp.preparedStmtStatus {
		if serverStatus > 0 {
			return true
		}
	}
	return false
}

// IsMySQLError returns true if the error is a MySQL error.
func IsMySQLError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*gomysql.MyError)
	return ok
}
