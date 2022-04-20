package backend

import (
	"encoding/binary"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

type Querier struct {
	capability uint32
}

func (q *Querier) Query(packetIO *pnet.PacketIO, sql string) (result *gomysql.Result, err error) {
	packetIO.ResetSequence()
	if err = q.sendQuery(packetIO, sql); err != nil {
		return nil, err
	}
	return q.readResult(packetIO)
}

func (q *Querier) sendQuery(packetIO *pnet.PacketIO, sql string) error {
	data := hack.Slice(sql)
	buf := make([]byte, 0, 1+len(data))
	buf = append(buf, mysql.ComQuery)
	buf = append(buf, data...)
	if err := packetIO.WritePacket(buf); err != nil {
		return err
	}
	return packetIO.Flush()
}

func (q *Querier) readResult(packetIO *pnet.PacketIO) (*gomysql.Result, error) {
	data, err := packetIO.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch data[0] {
	case mysql.OKHeader:
		return q.handleOKPacket(data)
	case mysql.ErrHeader:
		return nil, q.handleErrorPacket(data)
	case mysql.EOFHeader:
		return nil, nil
	case mysql.LocalInFileHeader:
		return nil, mysql.ErrMalformPacket
	default:
		return q.readResultSet(packetIO, data)
	}
}

func (q *Querier) readResultSet(packetIO *pnet.PacketIO, data []byte) (*gomysql.Result, error) {
	columnCount, _, n := pnet.ParseLengthEncodedInt(data)
	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result := &gomysql.Result{
		Resultset: gomysql.NewResultset(int(columnCount)),
	}
	if err := q.readResultColumns(packetIO, result); err != nil {
		return nil, err
	}
	if err := q.readResultRows(packetIO, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (q *Querier) readResultColumns(packetIO *pnet.PacketIO, result *gomysql.Result) (err error) {
	var fieldIndex int
	var data []byte

	for {
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if isEOFPacket(data) {
			if q.capability&mysql.ClientProtocol41 > 0 {
				result.Status = binary.LittleEndian.Uint16(data[3:])
			}
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

func (q *Querier) readResultRows(packetIO *pnet.PacketIO, result *gomysql.Result) (err error) {
	var data []byte

	for {
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if isEOFPacket(data) {
			if q.capability&mysql.ClientProtocol41 > 0 {
				result.Status = binary.LittleEndian.Uint16(data[3:])
			}
			break
		}
		if data[0] == mysql.ErrHeader {
			return q.handleErrorPacket(data)
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

func (q *Querier) handleErrorPacket(data []byte) error {
	e := new(gomysql.MyError)

	pos := 1
	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if q.capability&mysql.ClientProtocol41 > 0 {
		// skip '#'
		pos++
		e.State = string(hack.String(data[pos : pos+5]))
		pos += 5
	}

	e.Message = string(hack.String(data[pos:]))
	return e
}

func (q *Querier) handleOKPacket(data []byte) (*gomysql.Result, error) {
	var n int
	var pos = 1

	r := new(gomysql.Result)
	r.AffectedRows, _, n = pnet.ParseLengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = pnet.ParseLengthEncodedInt(data[pos:])
	pos += n

	if q.capability&mysql.ClientProtocol41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	} else if q.capability&mysql.ClientTransactions > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		pos += 2
	}
	return r, nil
}

func isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOFHeader && len(data) <= 5
}
