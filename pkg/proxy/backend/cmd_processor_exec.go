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
	"strings"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/siddontang/go/hack"
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

	if err = backendIO.WritePacket(request, true); err != nil {
		return false, err
	}
	return false, cp.forwardCommand(clientIO, backendIO, request)
}

func (cp *CmdProcessor) forwardCommand(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	cmd := request[0]
	switch cmd {
	case mysql.ComStmtPrepare:
		return cp.forwardPrepareCmd(clientIO, backendIO)
	case mysql.ComStmtFetch:
		return cp.forwardFetchCmd(clientIO, backendIO, request)
	case mysql.ComQuery, mysql.ComStmtExecute, mysql.ComProcessInfo:
		return cp.forwardQueryCmd(clientIO, backendIO, request)
	case mysql.ComStmtClose:
		return cp.forwardCloseCmd(request)
	case mysql.ComStmtSendLongData:
		return cp.forwardSendLongDataCmd(request)
	case mysql.ComChangeUser:
		return cp.forwardChangeUserCmd(clientIO, backendIO, request)
	case mysql.ComStatistics:
		return cp.forwardStatisticsCmd(clientIO, backendIO)
	case mysql.ComFieldList:
		return cp.forwardFieldListCmd(clientIO, backendIO, request)
	case mysql.ComQuit:
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
		cp.handleEOFPacket(request, response)
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

func forwardUntilEOF(clientIO, backendIO *pnet.PacketIO) (eofPacket []byte, err error) {
	var response []byte
	for {
		if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
			return
		}
		if pnet.IsEOFPacket(response) {
			return response, nil
		}
	}
}

func (cp *CmdProcessor) forwardPrepareCmd(clientIO, backendIO *pnet.PacketIO) error {
	response, err := forwardOnePacket(clientIO, backendIO, false)
	if err != nil {
		return err
	}
	// The OK packet doesn't contain a server status.
	switch response[0] {
	case mysql.OKHeader:
		expectedEOFNum := 0
		numColumns := binary.LittleEndian.Uint16(response[5:])
		if numColumns > 0 {
			expectedEOFNum++
		}
		numParams := binary.LittleEndian.Uint16(response[7:])
		if numParams > 0 {
			expectedEOFNum++
		}
		for i := 0; i < expectedEOFNum; i++ {
			// Ignore this status because PREPARE doesn't affect status.
			if _, err = forwardUntilEOF(clientIO, backendIO); err != nil {
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
	return errors.Errorf("unexpected response, cmd:%d resp:%d", mysql.ComStmtPrepare, response[0])
}

func (cp *CmdProcessor) forwardFetchCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	response, err := forwardOnePacket(clientIO, backendIO, false)
	if err != nil {
		return err
	}
	if response[0] == mysql.ErrHeader {
		if err := clientIO.Flush(); err != nil {
			return err
		}
		return cp.handleErrorPacket(response)
	}
	if !pnet.IsEOFPacket(response) {
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return err
		}
	}
	cp.handleEOFPacket(request, response)
	return clientIO.Flush()
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
			serverStatus, err = cp.forwardResultSet(clientIO, backendIO, request, response)
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
	return serverStatus, errors.Errorf("unexpected response, cmd:%d resp:%d", mysql.ComQuery, response[0])
}

func (cp *CmdProcessor) forwardResultSet(clientIO, backendIO *pnet.PacketIO, request, response []byte) (serverStatus uint16, err error) {
	if !pnet.IsEOFPacket(response) {
		// read columns
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	serverStatus = binary.LittleEndian.Uint16(response[3:])
	// If a cursor exists, only columns are sent this time. The client will then send COM_STMT_FETCH to fetch rows.
	// Otherwise, columns and rows are both sent once.
	if serverStatus&mysql.ServerStatusCursorExists == 0 {
		// read rows
		for {
			if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
				return
			}
			// An error may occur when the backend writes rows.
			if response[0] == mysql.ErrHeader {
				if err = clientIO.Flush(); err != nil {
					return serverStatus, err
				}
				return serverStatus, cp.handleErrorPacket(response)
			}
			if pnet.IsEOFPacket(response) {
				break
			}
		}
	}
	serverStatus = cp.handleEOFPacket(request, response)
	return serverStatus, clientIO.Flush()
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
	// Currently, TiDB responses with an OK or Err packet. But according to the MySQL doc, the server may send a
	// switch auth request.
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

func (cp *CmdProcessor) forwardFieldListCmd(clientIO, backendIO *pnet.PacketIO, request []byte) error {
	response, err := forwardOnePacket(clientIO, backendIO, false)
	if err != nil {
		return err
	}
	if response[0] == mysql.ErrHeader {
		if err = clientIO.Flush(); err != nil {
			return err
		}
		return cp.handleErrorPacket(response)
	}
	// It sends some columns and an EOF packet.
	if !pnet.IsEOFPacket(response) {
		response, err = forwardUntilEOF(clientIO, backendIO)
	}
	cp.handleEOFPacket(request, response)
	return clientIO.Flush()
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
	cmd, data := request[0], request[1:]
	// BEGIN/START TRANSACTION statements cannot be prepared.
	if cmd != mysql.ComQuery {
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
