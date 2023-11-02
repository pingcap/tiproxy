// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"encoding/binary"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

// executeCmd forwards requests and responses between the client and the backend.
// holdRequest: should the proxy send the request to the new backend.
// err: unexpected errors or MySQL errors.
func (cp *CmdProcessor) executeCmd(request []byte, clientIO, backendIO *pnet.PacketIO, waitingRedirect bool) (holdRequest bool, err error) {
	backendIO.ResetSequence()
	if waitingRedirect && cp.needHoldRequest(request) {
		var response []byte
		if _, response, err = cp.query(backendIO, "COMMIT"); err != nil {
			// If commit fails, forward the response to the client.
			if IsMySQLError(err) {
				if writeErr := clientIO.WritePacket(response, true); writeErr != nil {
					return false, writeErr
				}
			}
			// commit txn fails; read packet fails; write packet fails.
			return false, err
		}
		return true, err
	}
	return false, cp.forwardCommand(clientIO, backendIO, request)
}

func (cp *CmdProcessor) forwardCommand(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	cmd := pnet.Command(request[0])
	// ComChangeUser is special: we need to modify the packet before forwarding.
	if cmd != pnet.ComChangeUser {
		if err := backendIO.WritePacket(request, true); err != nil {
			return err
		}
	}
	switch cmd {
	case pnet.ComStmtPrepare:
		return cp.forwardPrepareCmd(clientIO, backendIO)
	case pnet.ComStmtFetch:
		return cp.forwardFetchCmd(clientIO, backendIO, request)
	case pnet.ComQuery, pnet.ComStmtExecute, pnet.ComProcessInfo:
		return cp.forwardQueryCmd(clientIO, backendIO, request)
	case pnet.ComStmtClose:
		return cp.forwardCloseCmd(request)
	case pnet.ComStmtSendLongData:
		return cp.forwardSendLongDataCmd(request)
	case pnet.ComChangeUser:
		return cp.forwardChangeUserCmd(clientIO, backendIO, request)
	case pnet.ComStatistics:
		return cp.forwardStatisticsCmd(clientIO, backendIO)
	case pnet.ComFieldList:
		return cp.forwardFieldListCmd(clientIO, backendIO, request)
	case pnet.ComQuit:
		return cp.forwardQuitCmd()
	}

	// For other commands, an OK / Error / EOF packet is expected.
	response, err := forwardOnePacket(clientIO, backendIO, true)
	if err != nil {
		return err
	}
	switch response[0] {
	case mysql.OKHeader:
		cp.handleOKPacket(request, response)
		return nil
	case mysql.ErrHeader:
		return cp.handleErrorPacket(response)
	case mysql.EOFHeader:
		if cp.capability&pnet.ClientDeprecateEOF == 0 {
			cp.handleEOFPacket(request, response)
		} else {
			cp.handleOKPacket(request, response)
		}
		return nil
	}
	// impossible here
	return errors.Errorf("unexpected response, cmd:%d resp:%d", cmd, response[0])
}

func forwardOnePacket(destIO, srcIO *pnet.PacketIO, flush bool) (data []byte, err error) {
	if data, err = srcIO.ReadPacket(); err != nil {
		return
	}
	return data, destIO.WritePacket(data, flush)
}

func (cp *CmdProcessor) forwardUntilResultEnd(clientIO, backendIO *pnet.PacketIO, request []byte) (uint16, error) {
	var serverStatus uint16
	err := backendIO.ForwardUntil(clientIO, func(firstByte byte, length int) bool {
		switch {
		case pnet.IsErrorPacket(firstByte):
			return true
		case cp.capability&pnet.ClientDeprecateEOF == 0:
			return pnet.IsEOFPacket(firstByte, length)
		default:
			return pnet.IsResultSetOKPacket(firstByte, length)
		}
	}, func(response []byte) error {
		switch {
		case pnet.IsErrorPacket(response[0]):
			if err := clientIO.Flush(); err != nil {
				return err
			}
			return cp.handleErrorPacket(response)
		case cp.capability&pnet.ClientDeprecateEOF == 0:
			serverStatus = cp.handleEOFPacket(request, response)
			return clientIO.Flush()
		default:
			serverStatus = cp.handleOKPacket(request, response).Status
			return clientIO.Flush()
		}
	})
	return serverStatus, err
}

func (cp *CmdProcessor) forwardPrepareCmd(clientIO, backendIO *pnet.PacketIO) error {
	response, err := forwardOnePacket(clientIO, backendIO, false)
	if err != nil {
		return err
	}
	switch response[0] {
	case mysql.OKHeader:
		// The OK packet doesn't contain a server status.
		// See https://mariadb.com/kb/en/com_stmt_prepare/
		numColumns := binary.LittleEndian.Uint16(response[5:])
		numParams := binary.LittleEndian.Uint16(response[7:])
		expectedPackets := int(numColumns) + int(numParams)
		if cp.capability&pnet.ClientDeprecateEOF == 0 {
			if numColumns > 0 {
				expectedPackets++
			}
			if numParams > 0 {
				expectedPackets++
			}
		}
		for i := 0; i < expectedPackets; i++ {
			// Ignore this status because PREPARE doesn't affect status.
			if _, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
				return err
			}
		}
		return clientIO.Flush()
	case mysql.ErrHeader:
		if err := clientIO.Flush(); err != nil {
			return err
		}
		return cp.handleErrorPacket(response)
	}
	// impossible here
	return errors.Errorf("unexpected response, cmd:%d resp:%d", pnet.ComStmtPrepare, response[0])
}

func (cp *CmdProcessor) forwardFetchCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	_, err := cp.forwardUntilResultEnd(clientIO, backendIO, request)
	return err
}

func (cp *CmdProcessor) forwardFieldListCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	_, err := cp.forwardUntilResultEnd(clientIO, backendIO, request)
	return err
}

func (cp *CmdProcessor) forwardQueryCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	for {
		response, err := forwardOnePacket(clientIO, backendIO, false)
		if err != nil {
			return err
		}
		var serverStatus uint16
		switch response[0] {
		case mysql.OKHeader:
			rs := cp.handleOKPacket(request, response)
			serverStatus, err = rs.Status, clientIO.Flush()
		case mysql.ErrHeader:
			if err := clientIO.Flush(); err != nil {
				return err
			}
			// Subsequent statements won't be executed even if it's a multi-statement.
			return cp.handleErrorPacket(response)
		case mysql.LocalInFileHeader:
			serverStatus, err = cp.forwardLoadInFile(clientIO, backendIO, request)
		default:
			serverStatus, err = cp.forwardResultSet(clientIO, backendIO, request)
		}
		if err != nil {
			return err
		}
		// If it's not the last statement in multi-statements, continue.
		if serverStatus&mysql.ServerMoreResultsExists == 0 {
			break
		}
	}
	return nil
}

func (cp *CmdProcessor) forwardLoadInFile(clientIO, backendIO *pnet.PacketIO, request []byte) (serverStatus uint16, err error) {
	if err = clientIO.Flush(); err != nil {
		return
	}
	// The client sends file data until an empty packet.
	for {
		var data []byte
		// The file may be large, so always flush it.
		if data, err = forwardOnePacket(backendIO, clientIO, true); err != nil {
			return
		}
		if len(data) == 0 {
			break
		}
	}
	var response []byte
	if response, err = forwardOnePacket(clientIO, backendIO, true); err != nil {
		return
	}
	switch response[0] {
	case mysql.OKHeader:
		rs := cp.handleOKPacket(request, response)
		return rs.Status, nil
	case mysql.ErrHeader:
		return serverStatus, cp.handleErrorPacket(response)
	}
	// impossible here
	return serverStatus, errors.Errorf("unexpected response, cmd:%d resp:%d", pnet.ComQuery, response[0])
}

func (cp *CmdProcessor) forwardResultSet(clientIO, backendIO *pnet.PacketIO, request []byte) (uint16, error) {
	if cp.capability&pnet.ClientDeprecateEOF == 0 {
		var response []byte
		// read columns
		for {
			var err error
			if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
				return 0, err
			}
			if pnet.IsEOFPacket(response[0], len(response)) {
				break
			}
		}
		serverStatus := binary.LittleEndian.Uint16(response[3:])
		// If a cursor exists, only columns are sent this time. The client will then send COM_STMT_FETCH to fetch rows.
		// Otherwise, columns and rows are both sent once.
		if serverStatus&mysql.ServerStatusCursorExists > 0 {
			return cp.handleEOFPacket(request, response), clientIO.Flush()
		}
	}
	// Deprecate EOF or no cursor.
	return cp.forwardUntilResultEnd(clientIO, backendIO, request)
}

func (cp *CmdProcessor) forwardCloseCmd(request []byte) error {
	// No packet is sent to the client for COM_STMT_CLOSE.
	cp.updatePrepStmtStatus(request, 0)
	return nil
}

func (cp *CmdProcessor) forwardSendLongDataCmd(request []byte) error {
	// No packet is sent to the client for COM_STMT_SEND_LONG_DATA.
	cp.updatePrepStmtStatus(request, 0)
	return nil
}

func (cp *CmdProcessor) forwardChangeUserCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	req, err := pnet.ParseChangeUser(request, cp.capability)
	if err != nil {
		cp.logger.Warn("parse COM_CHANGE_USER packet encounters error", zap.Error(err))
		var warning *errors.Warning
		if !errors.As(err, &warning) {
			return gomysql.ErrMalformPacket
		}
	}
	// The client may use the TiProxy salt to generate the auth data instead of using the TiDB salt,
	// so we need another switch-auth request to pass the TiDB salt to the client.
	// See https://github.com/pingcap/tiproxy/issues/127.
	req.AuthPlugin = unknownAuthPlugin
	req.AuthData = nil
	if err := backendIO.WritePacket(pnet.MakeChangeUser(req, cp.capability), true); err != nil {
		return err
	}

	for {
		response, err := forwardOnePacket(clientIO, backendIO, true)
		if err != nil {
			return err
		}
		switch response[0] {
		case mysql.OKHeader:
			cp.handleOKPacket(request, response)
			return nil
		case mysql.ErrHeader:
			return cp.handleErrorPacket(response)
		default:
			// If the server sends a switch-auth request, the proxy forwards the auth data to the server.
			if _, err = forwardOnePacket(backendIO, clientIO, true); err != nil {
				return err
			}
		}
	}
}

func (cp *CmdProcessor) forwardStatisticsCmd(clientIO, backendIO *pnet.PacketIO) error {
	// It just sends a string.
	_, err := forwardOnePacket(clientIO, backendIO, true)
	return err
}

func (cp *CmdProcessor) forwardQuitCmd() error {
	// No returning, just disconnect.
	cp.serverStatus |= StatusQuit
	return nil
}

// When the following conditions are matched, we can hold the command after redirecting:
// - The proxy has received a redirect signal.
// - The session is in a transaction and waits for it to finish.
// - The incoming statement is `BEGIN` or `START TRANSACTION`, which commits the current transaction implicitly.
// The application may always omit `COMMIT` and thus the session can never be redirected.
// We can send a `COMMIT` statement to the current backend and then forward the `BEGIN` statement to the new backend.
func (cp *CmdProcessor) needHoldRequest(request []byte) bool {
	cmd, data := pnet.Command(request[0]), request[1:]
	// BEGIN/START TRANSACTION statements cannot be prepared.
	if cmd != pnet.ComQuery {
		return false
	}
	// Hold request only when it's waiting for the end of the transaction.
	if cp.serverStatus&StatusInTrans == 0 {
		return false
	}
	// Opening result sets can still be fetched after COMMIT/ROLLBACK, so don't hold.
	if cp.hasPendingPreparedStmts() {
		return false
	}
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	query := hack.String(data)
	return isBeginStmt(query)
}

func isBeginStmt(query string) bool {
	normalized := parser.Normalize(query)
	return strings.HasPrefix(normalized, "begin") || strings.HasPrefix(normalized, "start transaction")
}
