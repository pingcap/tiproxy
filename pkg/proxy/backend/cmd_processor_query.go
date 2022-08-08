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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/siddontang/go/hack"
)

// query is called when the proxy sends requests to the backend by itself,
// such as querying session states, committing the current transaction.
// It only supports limited cases, excluding loading file, cursor fetch, multi-statements, etc.
func (cp *CmdProcessor) query(packetIO *pnet.PacketIO, sql string) (result *gomysql.Result, response []byte, err error) {
	// send request
	packetIO.ResetSequence()
	data := hack.Slice(sql)
	request := make([]byte, 0, 1+len(data))
	request = append(request, mysql.ComQuery)
	request = append(request, data...)
	if err = packetIO.WritePacket(request, true); err != nil {
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
		if pnet.IsEOFPacket(data) {
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
		fieldName := hack.String(result.Fields[fieldIndex].Name)
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
		if pnet.IsEOFPacket(data) {
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
