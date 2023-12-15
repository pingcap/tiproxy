// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/siddontang/go/hack"
)

// query is called when the proxy sends requests to the backend by itself,
// such as querying session states, committing the current transaction.
// It only supports limited cases, excluding loading file, cursor fetch, multi-statements, etc.
func (cp *CmdProcessor) query(packetIO *pnet.PacketIO, sql string) (result *mysql.Resultset, response []byte, err error) {
	// send request
	packetIO.ResetSequence()
	data := hack.Slice(sql)
	request := make([]byte, 0, 1+len(data))
	request = append(request, pnet.ComQuery.Byte())
	request = append(request, data...)
	if err = packetIO.WritePacket(request, true); err != nil {
		return
	}

	// read result
	if response, err = packetIO.ReadPacket(); err != nil {
		return
	}
	switch response[0] {
	case pnet.OKHeader.Byte():
		cp.handleOKPacket(request, response)
	case pnet.ErrHeader.Byte():
		err = cp.handleErrorPacket(response)
	case pnet.LocalInFileHeader.Byte():
		err = errors.WithStack(mysql.ErrMalformPacket)
	default:
		var rs *mysql.Result
		rs, err = cp.readResultSet(packetIO, response)
		result = rs.Resultset
	}
	return
}

// readResultSet is only used for reading the results of `show session_states` currently.
func (cp *CmdProcessor) readResultSet(packetIO *pnet.PacketIO, data []byte) (*mysql.Result, error) {
	columnCount, _, n := pnet.ParseLengthEncodedInt(data)
	if n-len(data) != 0 {
		return nil, errors.WithStack(mysql.ErrMalformPacket)
	}

	result := &mysql.Result{
		Resultset: mysql.NewResultset(int(columnCount)),
	}
	if err := cp.readResultColumns(packetIO, result); err != nil {
		return nil, err
	}
	if err := cp.readResultRows(packetIO, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (cp *CmdProcessor) readResultColumns(packetIO *pnet.PacketIO, result *mysql.Result) (err error) {
	var fieldIndex int
	var data []byte

	for {
		if fieldIndex == len(result.Fields) {
			if cp.capability&pnet.ClientDeprecateEOF == 0 {
				if data, err = packetIO.ReadPacket(); err != nil {
					return err
				}
				if !pnet.IsEOFPacket(data[0], len(data)) {
					return errors.WithStack(mysql.ErrMalformPacket)
				}
				result.Status = binary.LittleEndian.Uint16(data[3:])
			}
			return nil
		}
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if result.Fields[fieldIndex] == nil {
			result.Fields[fieldIndex] = &mysql.Field{}
		}
		if err = result.Fields[fieldIndex].Parse(data); err != nil {
			return errors.WithStack(err)
		}
		fieldName := hack.String(result.Fields[fieldIndex].Name)
		result.FieldNames[fieldName] = fieldIndex
		fieldIndex++
	}
}

func (cp *CmdProcessor) readResultRows(packetIO *pnet.PacketIO, result *mysql.Result) (err error) {
	var data []byte

	for {
		if data, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		if cp.capability&pnet.ClientDeprecateEOF == 0 {
			if pnet.IsEOFPacket(data[0], len(data)) {
				result.Status = binary.LittleEndian.Uint16(data[3:])
				break
			}
		} else {
			if pnet.IsResultSetOKPacket(data[0], len(data)) {
				result.Status = pnet.ParseOKPacket(data)
				break
			}
		}
		// An error may occur when the backend writes rows.
		if pnet.IsErrorPacket(data[0]) {
			return cp.handleErrorPacket(data)
		}
		result.RowDatas = append(result.RowDatas, data)
	}

	if cap(result.Values) < len(result.RowDatas) {
		result.Values = make([][]mysql.FieldValue, len(result.RowDatas))
	} else {
		result.Values = result.Values[:len(result.RowDatas)]
	}

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, false, result.Values[i])
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
