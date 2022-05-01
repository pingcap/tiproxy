package backend

import (
	"encoding/binary"
	"fmt"
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
}

func NewCmdProcessor() *CmdProcessor {
	return &CmdProcessor{
		serverStatus: StatusAutoCommit,
	}
}

func (cp *CmdProcessor) String() string {
	return fmt.Sprintf("in_txn:%t, auto_commit:%t, wait_exec:%t, wait_fetch:%t", cp.serverStatus&StatusInTrans > 0,
		cp.serverStatus&StatusAutoCommit > 0, cp.serverStatus&StatusPrepareWaitExecute > 0, cp.serverStatus&StatusPrepareWaitFetch > 0)
}

func (cp *CmdProcessor) executeCmd(request []byte, clientIO, backendIO *pnet.PacketIO, waitingRedirect bool) (holdRequest bool, err error) {
	cmd := request[0]
	data := request[1:]
	backendIO.ResetSequence()
	if waitingRedirect && cp.needHoldRequest(cmd, data) {
		if err = cp.sendQuery(backendIO, "COMMIT"); err != nil {
			return
		}
		var data []byte
		_, data, err = cp.readResult(backendIO)
		if err != nil {
			// If commit fails, forward the response to the client.
			if _, ok := err.(*gomysql.MyError); ok {
				if err = clientIO.WritePacket(data); err == nil {
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

	switch cmd {
	case mysql.ComQuery:
		err = cp.forwardCommand(clientIO, backendIO, 2)
	default:
		err = cp.forwardCommand(clientIO, backendIO, 1)
	}
	return
}

func (cp *CmdProcessor) forwardCommand(clientIO, backendIO *pnet.PacketIO, expectedEOFNum int) (err error) {
	var (
		eofHeaders int
		okOrErr    bool
	)
	for eofHeaders < expectedEOFNum && !okOrErr {
		var data []byte
		data, err = backendIO.ReadPacket()
		if err != nil {
			return
		}
		switch data[0] {
		case mysql.OKHeader:
			cp.handleOKPacket(data)
			okOrErr = true
		case mysql.ErrHeader:
			okOrErr = true
		case mysql.EOFHeader:
			if isEOFPacket(data) {
				eofHeaders += 1
			}
		}
		err = clientIO.WritePacket(data)
		if err != nil {
			return
		}
	}
	err = clientIO.Flush()
	return
}

func (cp *CmdProcessor) handleOKPacket(data []byte) *gomysql.Result {
	var n int
	var pos = 1

	r := new(gomysql.Result)
	r.AffectedRows, _, n = pnet.ParseLengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = pnet.ParseLengthEncodedInt(data[pos:])
	pos += n
	r.Status = binary.LittleEndian.Uint16(data[pos:])

	serverStatus := uint32(0)
	if r.Status&mysql.ServerStatusAutocommit > 0 {
		serverStatus |= StatusAutoCommit
	} else {
		serverStatus &= ^StatusAutoCommit
	}
	if r.Status&mysql.ServerStatusInTrans > 0 {
		serverStatus |= StatusInTrans
	} else {
		serverStatus &= ^StatusInTrans
	}
	cp.serverStatus = serverStatus
	return r
}

func (cp *CmdProcessor) canRedirect() bool {
	return cp.serverStatus&^StatusAutoCommit == 0
}

// When the following conditions are matched, we can hold the command after redirecting:
// - The proxy has received a redirect signal.
// - The session is in a transaction and waits for it to finish.
// - The incoming statement is `BEGIN` or `START TRANSACTION`, which commits the current transaction implicitly.
// The application may always omit `COMMIT` and thus the session can never be redirected.
// We can send a `COMMIT` statement to the current backend and then forward the `BEGIN` statement to the new backend.
func (cp *CmdProcessor) needHoldRequest(cmd byte, data []byte) bool {
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

func (cp *CmdProcessor) query(packetIO *pnet.PacketIO, sql string) (result *gomysql.Result, err error) {
	packetIO.ResetSequence()
	if err = cp.sendQuery(packetIO, sql); err != nil {
		return
	}
	result, _, err = cp.readResult(packetIO)
	return
}

func (cp *CmdProcessor) sendQuery(packetIO *pnet.PacketIO, sql string) error {
	data := hack.Slice(sql)
	buf := make([]byte, 0, 1+len(data))
	buf = append(buf, mysql.ComQuery)
	buf = append(buf, data...)
	if err := packetIO.WritePacket(buf); err != nil {
		return err
	}
	return packetIO.Flush()
}

func (cp *CmdProcessor) readResult(packetIO *pnet.PacketIO) (result *gomysql.Result, data []byte, err error) {
	data, err = packetIO.ReadPacket()
	if err != nil {
		return
	}
	switch data[0] {
	case mysql.OKHeader:
		result = cp.handleOKPacket(data)
	case mysql.ErrHeader:
		err = cp.handleErrorPacket(data)
	case mysql.EOFHeader:
	case mysql.LocalInFileHeader:
		err = mysql.ErrMalformPacket
	default:
		result, err = cp.readResultSet(packetIO, data)
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
