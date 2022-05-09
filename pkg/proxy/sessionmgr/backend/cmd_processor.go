package backend

import (
	"encoding/binary"
	"strings"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

const (
	StatusInTrans            uint32 = 0x01
	StatusAutoCommit         uint32 = 0x02
	StatusPrepareWaitExecute uint32 = 0x04
	StatusPrepareWaitFetch   uint32 = 0x08
)

// CmdProcessor maintains the transaction and prepared statement status and decides whether the session can be redirected.
type CmdProcessor struct {
	serverStatus uint32
	// Each prepared statement has an independent status.
	preparedStmtStatus map[int]uint32
	disconnecting      bool
}

func NewCmdProcessor() *CmdProcessor {
	return &CmdProcessor{
		serverStatus:       StatusAutoCommit,
		preparedStmtStatus: make(map[int]uint32),
		disconnecting:      false,
	}
}

func (cp *CmdProcessor) executeCmd(request []byte, clientIO, backendIO *pnet.PacketIO, waitingRedirect bool) (holdRequest bool, err error) {
	backendIO.ResetSequence()
	if waitingRedirect && cp.needHoldRequest(request) {
		var response []byte
		if _, response, err = cp.query(backendIO, "COMMIT"); err != nil {
			// If commit fails, forward the response to the client.
			if _, ok := err.(*gomysql.MyError); ok {
				if err = clientIO.WritePacket(response); err == nil {
					err = clientIO.Flush()
				}
			}
			// commit txn fails; read packet fails; write packet fails.
			return
		}
		holdRequest = true
		return
	}

	if err = backendIO.WritePacket(request); err != nil {
		return
	}
	if err = backendIO.Flush(); err != nil {
		return
	}
	err = cp.forwardCommand(clientIO, backendIO, request)
	return
}

func (cp *CmdProcessor) forwardCommand(clientIO, backendIO *pnet.PacketIO, request []byte) (err error) {
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
	case mysql.ComQuit:
		cp.disconnecting = true
		return
	}

	for {
		response, err := forwardOnePacket(clientIO, backendIO)
		if err != nil {
			return err
		}
		if response[0] == mysql.OKHeader {
			cp.handleOKPacket(request, response)
			break
		} else if response[0] == mysql.ErrHeader {
			break
		} else if isEOFPacket(response) {
			cp.handleEOFPacket(request, response)
			break
		}
	}
	return clientIO.Flush()
}

func forwardOnePacket(clientIO, backendIO *pnet.PacketIO) (response []byte, err error) {
	if response, err = backendIO.ReadPacket(); err != nil {
		return
	}
	err = clientIO.WritePacket(response)
	return
}

func forwardUntilEOF(clientIO, backendIO *pnet.PacketIO) (eofPacket []byte, err error) {
	var response []byte
	for {
		if response, err = forwardOnePacket(clientIO, backendIO); err != nil {
			return
		}
		if isEOFPacket(response) {
			return response, nil
		}
	}
}

func (cp *CmdProcessor) forwardPrepareCmd(clientIO, backendIO *pnet.PacketIO) (err error) {
	var (
		expectedEOFNum int
		response       []byte
	)
	if response, err = forwardOnePacket(clientIO, backendIO); err != nil {
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
	}
	for i := 0; i < expectedEOFNum; i++ {
		// The server status in EOF packets is always 0, so ignore it.
		if _, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	return clientIO.Flush()
}

func (cp *CmdProcessor) forwardFetchCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (err error) {
	var response []byte
	if response, err = forwardOnePacket(clientIO, backendIO); err != nil {
		return
	}
	if response[0] == mysql.ErrHeader {
		return clientIO.Flush()
	} else if !isEOFPacket(response) {
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	cp.handleEOFPacket(request, response)
	return clientIO.Flush()
}

func (cp *CmdProcessor) forwardQueryCmd(clientIO, backendIO *pnet.PacketIO, request []byte) (err error) {
	var response []byte
	// The OK packet doesn't contain a server status, so skip all the OK packets.
	if response, err = forwardOnePacket(clientIO, backendIO); err != nil {
		return
	}
	if response[0] == mysql.OKHeader {
		cp.handleOKPacket(request, response)
		return clientIO.Flush()
	} else if response[0] == mysql.ErrHeader {
		return clientIO.Flush()
	} else if !isEOFPacket(response) {
		// read columns
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	serverStatus := binary.LittleEndian.Uint16(response[3:])
	// read rows
	if serverStatus&mysql.ServerStatusCursorExists == 0 {
		if response, err = forwardUntilEOF(clientIO, backendIO); err != nil {
			return
		}
	}
	cp.handleEOFPacket(request, response)
	return clientIO.Flush()
}

func (cp *CmdProcessor) forwardCloseCmd(request []byte) (err error) {
	// No packet is sent to the client for COM_STMT_CLOSE.
	cp.updatePrepStmtStatus(request, 0)
	return nil
}

func (cp *CmdProcessor) handleOKPacket(request, response []byte) *gomysql.Result {
	var n int
	var pos = 1

	r := new(gomysql.Result)
	r.AffectedRows, _, n = pnet.ParseLengthEncodedInt(response[pos:])
	pos += n
	r.InsertId, _, n = pnet.ParseLengthEncodedInt(response[pos:])
	pos += n
	r.Status = binary.LittleEndian.Uint16(response[pos:])

	cp.updateServerStatus(request, r.Status)
	return r
}

func (cp *CmdProcessor) handleEOFPacket(request, response []byte) {
	serverStatus := binary.LittleEndian.Uint16(response[3:])
	cp.updateServerStatus(request, serverStatus)
}

func (cp *CmdProcessor) updateServerStatus(request []byte, serverStatus uint16) {
	cp.updateTxnStatus(serverStatus)
	cp.updatePrepStmtStatus(request, serverStatus)
}

func (cp *CmdProcessor) updateTxnStatus(serverStatus uint16) {
	if serverStatus&mysql.ServerStatusAutocommit > 0 {
		cp.serverStatus |= StatusAutoCommit
	} else {
		cp.serverStatus &^= StatusAutoCommit
	}
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
	case mysql.ComResetConnection:
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

func (cp *CmdProcessor) canRedirect() bool {
	if cp.disconnecting {
		return false
	}
	if cp.serverStatus&StatusInTrans > 0 {
		return false
	}
	// If any result of the prepared statements is not fetched, we should wait.
	for _, serverStatus := range cp.preparedStmtStatus {
		if serverStatus > 0 {
			return false
		}
	}
	return true
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
	query := string(hack.String(data))
	return isBeginStmt(query)
}

func (cp *CmdProcessor) query(packetIO *pnet.PacketIO, sql string) (result *gomysql.Result, response []byte, err error) {
	// send request
	packetIO.ResetSequence()
	data := hack.Slice(sql)
	request := make([]byte, 0, 1+len(data))
	request = append(request, mysql.ComQuery)
	request = append(request, data...)
	if err = packetIO.WritePacket(request); err != nil {
		return
	}
	if err = packetIO.Flush(); err != nil {
		return
	}

	// read result
	if response, err = packetIO.ReadPacket(); err != nil {
		return
	}
	switch response[0] {
	case mysql.OKHeader:
		result = cp.handleOKPacket(request, response)
	case mysql.ErrHeader:
		err = cp.handleErrorPacket(response)
	case mysql.EOFHeader:
		cp.handleEOFPacket(request, response)
	case mysql.LocalInFileHeader:
		err = mysql.ErrMalformPacket
	default:
		result, err = cp.readResultSet(packetIO, response)
	}
	return
}

func (cp *CmdProcessor) readResultSet(packetIO *pnet.PacketIO, data []byte) (*gomysql.Result, error) {
	columnCount, _, n := pnet.ParseLengthEncodedInt(data)
	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result := &gomysql.Result{
		Resultset: gomysql.NewResultset(int(columnCount)),
	}
	if err := cp.readResultColumns(packetIO, result); err != nil {
		return nil, err
	}
	if err := cp.readResultRows(packetIO, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (cp *CmdProcessor) readResultColumns(packetIO *pnet.PacketIO, result *gomysql.Result) (err error) {
	var fieldIndex int
	var data []byte

	for {
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if isEOFPacket(data) {
			result.Status = binary.LittleEndian.Uint16(data[3:])
			if fieldIndex != len(result.Fields) {
				err = errors.Trace(mysql.ErrMalformPacket)
			}
			return
		}

		if result.Fields[fieldIndex] == nil {
			result.Fields[fieldIndex] = &gomysql.Field{}
		}
		if err = result.Fields[fieldIndex].Parse(data); err != nil {
			return errors.Trace(err)
		}
		fieldName := string(hack.String(result.Fields[fieldIndex].Name))
		result.FieldNames[fieldName] = fieldIndex
		fieldIndex++
	}
}

func (cp *CmdProcessor) readResultRows(packetIO *pnet.PacketIO, result *gomysql.Result) (err error) {
	var data []byte

	for {
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if isEOFPacket(data) {
			result.Status = binary.LittleEndian.Uint16(data[3:])
			break
		}
		if data[0] == mysql.ErrHeader {
			return cp.handleErrorPacket(data)
		}
		result.RowDatas = append(result.RowDatas, data)
	}

	if cap(result.Values) < len(result.RowDatas) {
		result.Values = make([][]gomysql.FieldValue, len(result.RowDatas))
	} else {
		result.Values = result.Values[:len(result.RowDatas)]
	}

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, false, result.Values[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (cp *CmdProcessor) handleErrorPacket(data []byte) error {
	e := new(gomysql.MyError)

	pos := 1
	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	pos++
	e.State = string(hack.String(data[pos : pos+5]))
	pos += 5

	e.Message = string(hack.String(data[pos:]))
	return e
}

func isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOFHeader && len(data) <= 5
}

func isBeginStmt(query string) bool {
	normalized := parser.Normalize(query)
	if strings.HasPrefix(normalized, "begin") || strings.HasPrefix(normalized, "start transaction") {
		return true
	}
	return false
}
