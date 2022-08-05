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

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/siddontang/go/hack"
)

// executeCmd forwards requests and responses between the client and the backend.
func (cp *CmdProcessor) executeCmd(request []byte, clientIO, backendIO *pnet.PacketIO, waitingRedirect bool) (holdRequest, succeed bool, err error) {
	backendIO.ResetSequence()
	if waitingRedirect && cp.needHoldRequest(request) {
		var response []byte
		if _, response, err = cp.query(backendIO, "COMMIT"); err != nil {
			// If commit fails, forward the response to the client.
			if _, ok := err.(*gomysql.MyError); ok {
				err = clientIO.WritePacket(response, true)
			}
			// commit txn fails; read packet fails; write packet fails.
			return
		}
		holdRequest = true
		succeed = true
		return
	}

	if err = backendIO.WritePacket(request, true); err != nil {
		return
	}
	succeed, err = cp.forwardCommand(clientIO, backendIO, request)
	return
}

func (cp *CmdProcessor) forwardCommand(clientIO, backendIO *pnet.PacketIO, request []byte) (succeed bool, err error) {
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
		return false, err
	}
	switch response[0] {
	case mysql.OKHeader:
		cp.handleOKPacket(request, response)
		return true, nil
	case mysql.ErrHeader:
		return false, nil
	case mysql.EOFHeader:
		cp.handleEOFPacket(request, response)
		return true, nil
	}
	// impossible here
	return false, errors.Errorf("unexpected response, cmd:%d resp:%d", cmd, response[0])
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

func (cp *CmdProcessor) forwardPrepareCmd(clientIO, backendIO *pnet.PacketIO) (succeed bool, err error) {
	var (
		expectedEOFNum int
		response       []byte
	)
	if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
		return
	}
	// The OK packet doesn't contain a server status.
	if response[0] == mysql.OKHeader {
		numColumns := binary.LittleEndian.Uint16(response[5:])
		if numColumns > 0 {
			expectedEOFNum++
		}
		numParams := binary.LittleEndian.Uint16(response[7:])
		if numParams > 0 {
			expectedEOFNum++
		}
		succeed = true
	}
	for i := 0; i < expectedEOFNum; i++ {
		// The server status in EOF packets is always 0, so ignore it.
		if _, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	return succeed, clientIO.Flush()
}

func (cp *CmdProcessor) forwardFetchCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (succeed bool, err error) {
	var response []byte
	if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
		return
	}
	if response[0] == mysql.ErrHeader {
		return false, clientIO.Flush()
	} else if !pnet.IsEOFPacket(response) {
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	cp.handleEOFPacket(request, response)
	return true, clientIO.Flush()
}

func (cp *CmdProcessor) forwardQueryCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (succeed bool, err error) {
	var response []byte
	for {
		if response, err = forwardOnePacket(clientIO, backendIO, false); err != nil {
			return false, err
		}
		var serverStatus uint16
		switch response[0] {
		case mysql.OKHeader:
			if err = clientIO.Flush(); err != nil {
				return false, err
			}
			rs := cp.handleOKPacket(request, response)
			serverStatus = rs.Status
			succeed = true
		case mysql.ErrHeader:
			// Subsequent statements won't be executed even if it's a multi-statement.
			return false, clientIO.Flush()
		case mysql.LocalInFileHeader:
			serverStatus, succeed, err = cp.forwardLoadInFile(clientIO, backendIO, request)
		default:
			serverStatus, succeed, err = cp.forwardResultSet(clientIO, backendIO, request, response)
		}
		if err != nil || !succeed {
			return succeed, err
		}
		// If it's not the last statement in multi-statements, continue.
		if serverStatus&mysql.ServerMoreResultsExists == 0 {
			break
		}
	}
	return true, nil
}

func (cp *CmdProcessor) forwardLoadInFile(clientIO, backendIO *pnet.PacketIO, request []byte) (serverStatus uint16, succeed bool, err error) {
	if err = clientIO.Flush(); err != nil {
		return
	}
	// The client sends file data until an empty packet.
	for {
		var data []byte
		data, err = forwardOnePacket(backendIO, clientIO, true)
		if err != nil {
			return
		}
		if len(data) == 0 {
			break
		}
	}
	var response []byte
	response, err = forwardOnePacket(clientIO, backendIO, true)
	if err != nil {
		return
	}
	if response[0] == mysql.OKHeader {
		rs := cp.handleOKPacket(request, response)
		return rs.Status, true, nil
	}
	// Error packet
	return
}

func (cp *CmdProcessor) forwardResultSet(clientIO, backendIO *pnet.PacketIO, request, response []byte) (serverStatus uint16, succeed bool, err error) {
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
			if response[0] == mysql.ErrHeader {
				return 0, false, clientIO.Flush()
			}
			if pnet.IsEOFPacket(response) {
				break
			}
		}
	}
	serverStatus = cp.handleEOFPacket(request, response)
	return serverStatus, true, clientIO.Flush()
}

func (cp *CmdProcessor) forwardCloseCmd(request []byte) (succeed bool, err error) {
	// No packet is sent to the client for COM_STMT_CLOSE.
	cp.updatePrepStmtStatus(request, 0)
	return true, nil
}

func (cp *CmdProcessor) forwardChangeUserCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (succeed bool, err error) {
	// Currently, TiDB responses with an OK or Err packet. But according to the MySQL doc, the server may send a
	// switch auth request.
	for {
		response, err := forwardOnePacket(clientIO, backendIO, true)
		if err != nil {
			return false, err
		}
		switch response[0] {
		case mysql.OKHeader:
			cp.handleOKPacket(request, response)
			return true, nil
		case mysql.ErrHeader:
			return false, nil
		default:
			// If the server sends a switch-auth request, the proxy forwards the auth data to the server.
			if _, err = forwardOnePacket(backendIO, clientIO, true); err != nil {
				return false, err
			}
		}
	}
}

func (cp *CmdProcessor) forwardFieldListCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (succeed bool, err error) {
	response, err := forwardOnePacket(clientIO, backendIO, false)
	if err != nil {
		return false, err
	}
	if response[0] == mysql.ErrHeader {
		return false, clientIO.Flush()
	}
	// It sends some columns and an EOF packet.
	if !pnet.IsEOFPacket(response) {
		response, err = forwardUntilEOF(clientIO, backendIO)
	}
	cp.handleEOFPacket(request, response)
	return true, clientIO.Flush()
}

func (cp *CmdProcessor) forwardStatisticsCmd(clientIO, backendIO *pnet.PacketIO) (succeed bool, err error) {
	// It just sends a string.
	_, err = forwardOnePacket(clientIO, backendIO, true)
	return true, err
}

func (cp *CmdProcessor) forwardQuitCmd() (succeed bool, err error) {
	// No returning, just disconnect.
	cp.serverStatus |= StatusQuit
	return true, nil
}

// When the following conditions are matched, we can hold the command after redirecting:
// - The proxy has received a redirect signal.
// - The session is in a transaction and waits for it to finish.
// - The incoming statement is `BEGIN` or `START TRANSACTION`, which commits the current transaction implicitly.
// The application may always omit `COMMIT` and thus the session can never be redirected.
// We can send a `COMMIT` statement to the current backend and then forward the `BEGIN` statement to the new backend.
func (cp *CmdProcessor) needHoldRequest(request []byte) bool {
	cmd, data := request[0], request[1:]
	if cmd != mysql.ComQuery {
		return false
	}
	// Skip checking prepared statements because the cursor will be discarded.
	if cp.serverStatus&StatusInTrans == 0 {
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
	if strings.HasPrefix(normalized, "begin") || strings.HasPrefix(normalized, "start transaction") {
		return true
	}
	return false
}
