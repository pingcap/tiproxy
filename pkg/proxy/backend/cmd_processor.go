// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"encoding/binary"

	"github.com/pingcap/tidb/parser/mysql"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"go.uber.org/zap"
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
	capability         pnet.Capability
	// Only includes in_trans or quit status.
	serverStatus uint32
	logger       *zap.Logger
}

func NewCmdProcessor(logger *zap.Logger) *CmdProcessor {
	return &CmdProcessor{
		serverStatus:       0,
		preparedStmtStatus: make(map[int]uint32),
		logger:             logger,
	}
}

func (cp *CmdProcessor) handleOKPacket(request, response []byte) uint16 {
	status := pnet.ParseOKPacket(response)
	cp.updateServerStatus(request, status)
	return status
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
	cmd := pnet.Command(request[0])
	switch cmd {
	case pnet.ComStmtSendLongData, pnet.ComStmtExecute, pnet.ComStmtFetch, pnet.ComStmtReset, pnet.ComStmtClose:
		stmtID = int(binary.LittleEndian.Uint32(request[1:5]))
	case pnet.ComResetConnection, pnet.ComChangeUser:
		cp.preparedStmtStatus = make(map[int]uint32)
		return
	default:
		return
	}
	switch cmd {
	case pnet.ComStmtSendLongData:
		prepStmtStatus = StatusPrepareWaitExecute
	case pnet.ComStmtExecute:
		if serverStatus&mysql.ServerStatusCursorExists > 0 {
			prepStmtStatus = StatusPrepareWaitFetch
		}
	case pnet.ComStmtFetch:
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
