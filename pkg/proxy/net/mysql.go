// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/siddontang/go/hack"
	"go.uber.org/zap"
)

const (
	ShaCommand   = 1
	FastAuthFail = 4
)

var (
	ServerVersion = mysql.ServerVersion
	Collation     = uint8(mysql.DefaultCollationID)
	Status        = ServerStatusAutocommit
)

// WriteSwitchRequest writes a switch request to the client. It's only for testing.
func MakeSwitchRequest(authPlugin string, salt [20]byte) []byte {
	length := 1 + len(authPlugin) + 1 + len(salt) + 1
	data := make([]byte, 0, length)
	// check https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
	data = append(data, byte(AuthSwitchHeader))
	data = append(data, authPlugin...)
	data = append(data, 0x00)
	data = append(data, salt[:]...)
	data = append(data, 0x00)
	return data
}

func MakeShaCommand() []byte {
	return []byte{ShaCommand, FastAuthFail}
}

func ParseSSLRequestOrHandshakeResp(pkt []byte) bool {
	capability := Capability(binary.LittleEndian.Uint32(pkt[:4]))
	return capability&ClientSSL != 0
}

// WriteErrPacket writes an Error packet.
func MakeErrPacket(merr *gomysql.MyError) []byte {
	data := make([]byte, 0, 9+len(merr.Message))
	data = append(data, ErrHeader.Byte())
	data = append(data, byte(merr.Code), byte(merr.Code>>8))
	// ClientProtocol41 is always enabled.
	data = append(data, '#')
	data = append(data, merr.State...)
	data = append(data, merr.Message...)
	return data
}

// WriteOKPacket writes an OK packet. It's only for testing.
func MakeOKPacket(status uint16, header Header) []byte {
	data := make([]byte, 0, 7)
	data = append(data, header.Byte())
	data = append(data, 0, 0)
	// ClientProtocol41 is always enabled.
	data = DumpUint16(data, status)
	data = append(data, 0, 0)
	return data
}

// WriteEOFPacket writes an EOF packet. It's only for testing.
func MakeEOFPacket(status uint16) []byte {
	data := make([]byte, 0, 5)
	data = append(data, EOFHeader.Byte())
	data = append(data, 0, 0)
	// ClientProtocol41 is always enabled.
	data = DumpUint16(data, status)
	return data
}

// WriteUserError writes an unknown error to the client.
func MakeUserError(err error) []byte {
	if err == nil {
		return nil
	}
	myErr := gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, err.Error())
	return MakeErrPacket(myErr)
}

type InitialHandshake struct {
	Salt          [20]byte
	ServerVersion string
	AuthPlugin    string
	ConnID        uint64
	Capability    Capability
}

// MakeInitialHandshake creates an initial handshake as a server.
// It's used for tenant-aware routing and testing.
func MakeInitialHandshake(capability Capability, salt [20]byte, authPlugin string, serverVersion string, connID uint64) []byte {
	saltLen := len(salt)
	data := make([]byte, 0, 128)

	// min version 10
	data = append(data, 10)
	// server version[NUL]
	data = append(data, serverVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(connID), byte(connID>>8), byte(connID>>16), byte(connID>>24))
	// auth-plugin-data-part-1
	data = append(data, salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capability), byte(capability>>8))
	// charset
	data = append(data, Collation)
	// status
	data = DumpUint16(data, Status)
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(capability>>16), byte(capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(saltLen+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, salt[8:saltLen]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte(authPlugin)...)
	data = append(data, 0)
	return data
}

// ParseInitialHandshake parses the initial handshake received from the server.
func ParseInitialHandshake(data []byte) *InitialHandshake {
	hs := &InitialHandshake{}
	// skip min version
	hs.ServerVersion = string(data[1 : 1+bytes.IndexByte(data[1:], 0)])
	pos := 1 + len(hs.ServerVersion) + 1
	hs.ConnID = uint64(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	copy(hs.Salt[:], data[pos:pos+8])
	// skip filter
	pos += 8 + 1
	// capability lower 2 bytes
	capability := uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	if len(data) > pos {
		// skip server charset + status
		pos += 1 + 2
		// capability flags (upper 2 bytes)
		capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | capability
		// skip auth data len or [00]
		// skip reserved (all [00])
		pos += 2 + 1 + 10
		copy(hs.Salt[8:], data[pos:])
		pos += 13
		hs.AuthPlugin = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	}
	hs.Capability = Capability(capability)
	return hs
}

// HandshakeResp indicates the response read from the client.
type HandshakeResp struct {
	Attrs      map[string]string
	User       string
	DB         string
	AuthPlugin string
	AuthData   []byte
	Capability Capability
	ZstdLevel  int
	Collation  uint8
}

func ParseHandshakeResponse(data []byte) (*HandshakeResp, error) {
	resp := new(HandshakeResp)
	pos := 0
	// capability
	resp.Capability = Capability(binary.LittleEndian.Uint32(data[:4]))
	pos += 4
	// skip max packet size
	pos += 4
	// charset
	resp.Collation = data[pos]
	pos++
	// skip reserved 23[00]
	pos += 23

	// user name
	resp.User = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(resp.User) + 1

	// password
	if resp.Capability&ClientPluginAuthLenencClientData > 0 {
		if data[pos] == 0x1 { // No auth data
			pos += 2
		} else {
			num, null, off := ParseLengthEncodedInt(data[pos:])
			pos += off
			if !null {
				resp.AuthData = data[pos : pos+int(num)]
				pos += int(num)
			}
		}
	} else if resp.Capability&ClientSecureConnection > 0 {
		authLen := int(data[pos])
		pos++
		resp.AuthData = data[pos : pos+authLen]
		pos += authLen
	} else {
		resp.AuthData = data[pos : pos+bytes.IndexByte(data[pos:], 0)]
		pos += len(resp.AuthData) + 1
	}

	// dbname
	if resp.Capability&ClientConnectWithDB > 0 {
		if len(data[pos:]) > 0 {
			idx := bytes.IndexByte(data[pos:], 0)
			resp.DB = string(data[pos : pos+idx])
			pos = pos + idx + 1
		}
	}

	// auth plugin
	if resp.Capability&ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[pos:], 0)
		s := pos
		f := pos + idx
		if s < f { // handle unexpected bad packets
			resp.AuthPlugin = string(data[s:f])
		}
		pos += idx + 1
	}

	// attrs
	var err error
	if resp.Capability&ClientConnectAttrs > 0 {
		if num, null, off := ParseLengthEncodedInt(data[pos:]); !null {
			pos += off
			row := data[pos : pos+int(num)]
			resp.Attrs, err = parseAttrs(row)
			// Some clients have known bugs, but we should be compatible with them.
			// E.g. https://bugs.mysql.com/bug.php?id=79612.
			if err != nil {
				err = &errors.Warning{Err: errors.Wrapf(err, "parse attrs failed")}
			}
			pos += int(num)
		}
	}

	// zstd compress level
	if resp.Capability&ClientZstdCompressionAlgorithm > 0 {
		resp.ZstdLevel = int(data[pos])
	}
	return resp, err
}

func parseAttrs(data []byte) (map[string]string, error) {
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		key, _, off, err := ParseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off
		value, _, off, err := ParseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off

		attrs[string(key)] = string(value)
	}
	return attrs, nil
}

func dumpAttrs(attrs map[string]string) []byte {
	var buf bytes.Buffer
	var keyBuf []byte
	for k, v := range attrs {
		keyBuf = keyBuf[0:0]
		keyBuf = DumpLengthEncodedString(keyBuf, []byte(k))
		buf.Write(keyBuf)
		keyBuf = keyBuf[0:0]
		keyBuf = DumpLengthEncodedString(keyBuf, []byte(v))
		buf.Write(keyBuf)
	}
	return buf.Bytes()
}

func MakeHandshakeResponse(resp *HandshakeResp) []byte {
	// encode length of the auth data
	var (
		authRespBuf, attrLenBuf  [9]byte
		authResp, attrs, attrBuf []byte
	)
	authResp = DumpLengthEncodedInt(authRespBuf[:0], uint64(len(resp.AuthData)))
	capability := resp.Capability
	if len(authResp) > 1 {
		capability |= ClientPluginAuthLenencClientData
	} else {
		capability &= ^ClientPluginAuthLenencClientData
	}
	if capability&ClientConnectAttrs > 0 {
		attrs = dumpAttrs(resp.Attrs)
		attrBuf = DumpLengthEncodedInt(attrLenBuf[:0], uint64(len(attrs)))
	}

	length := 4 + 4 + 1 + 23 + len(resp.User) + 1 + len(authResp) + len(resp.AuthData) + len(resp.DB) + 1 + len(resp.AuthPlugin) + 1 + len(attrBuf) + len(attrs) + 1
	data := make([]byte, length)
	pos := 0
	// capability [32 bit]
	DumpUint32(data[:0], capability.Uint32())
	pos += 4
	// MaxPacketSize [32 bit]
	pos += 4
	// Charset [1 byte]
	data[pos] = resp.Collation
	pos++
	// Filler [23 bytes] (all 0x00)
	pos += 23

	// User [null terminated string]
	pos += copy(data[pos:], resp.User)
	data[pos] = 0x00
	pos++

	// auth data
	if capability&ClientPluginAuthLenencClientData > 0 {
		pos += copy(data[pos:], authResp)
		pos += copy(data[pos:], resp.AuthData)
	} else if capability&ClientSecureConnection > 0 {
		data[pos] = byte(len(resp.AuthData))
		pos++
		pos += copy(data[pos:], resp.AuthData)
	} else {
		pos += copy(data[pos:], resp.AuthData)
		data[pos] = 0x00
		pos++
	}

	// db [null terminated string]
	if capability&ClientConnectWithDB > 0 {
		pos += copy(data[pos:], resp.DB)
		data[pos] = 0x00
		pos++
	}

	// auth_plugin [null terminated string]
	if capability&ClientPluginAuth > 0 {
		pos += copy(data[pos:], resp.AuthPlugin)
		data[pos] = 0x00
		pos++
	}

	// attrs
	if capability&ClientConnectAttrs > 0 {
		pos += copy(data[pos:], attrBuf)
		pos += copy(data[pos:], attrs)
	}

	// compress level
	if capability&ClientZstdCompressionAlgorithm > 0 {
		data[pos] = byte(resp.ZstdLevel)
		pos++
	}
	return data[:pos]
}

type ChangeUserReq struct {
	Attrs      map[string]string
	User       string
	DB         string
	AuthPlugin string
	AuthData   []byte
	Charset    []byte
}

// MakeChangeUser creates the data of COM_CHANGE_USER.
func MakeChangeUser(req *ChangeUserReq, capability Capability) []byte {
	var attrLenBuf [9]byte
	var attrs, attrBuf []byte
	length := 1 + len(req.User) + 1 + len(req.AuthData) + 1 + len(req.DB) + 1 + 2 + len(req.AuthPlugin) + 1
	if capability&ClientConnectAttrs > 0 {
		attrs = dumpAttrs(req.Attrs)
		attrBuf = DumpLengthEncodedInt(attrLenBuf[:0], uint64(len(attrs)))
	}
	data := make([]byte, 0, length)
	data = append(data, ComChangeUser.Byte())
	// username
	data = append(data, hack.Slice(req.User)...)
	data = append(data, 0x00)
	// auth data
	if capability&ClientSecureConnection > 0 {
		data = append(data, byte(len(req.AuthData)))
		data = append(data, req.AuthData...)
	} else {
		data = append(data, req.AuthData...)
		data = append(data, 0x00)
	}
	// db
	data = append(data, hack.Slice(req.DB)...)
	data = append(data, 0x00)
	// character set. CLIENT_PROTOCOL_41 is always enabled.
	data = append(data, req.Charset...)
	// auth plugin
	if capability&ClientPluginAuth > 0 {
		data = append(data, hack.Slice(req.AuthPlugin)...)
		data = append(data, 0x00)
	}
	// attrs
	if capability&ClientConnectAttrs > 0 {
		data = append(data, attrBuf...)
		data = append(data, attrs...)
	}
	return data
}

// ParseChangeUser parses the data of COM_CHANGE_USER.
func ParseChangeUser(data []byte, capability Capability) (*ChangeUserReq, error) {
	req := new(ChangeUserReq)
	pos := 1
	// username
	req.User = hack.String(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(req.User) + 1
	// auth data
	if capability&ClientSecureConnection > 0 {
		authLen := int(data[pos])
		pos++
		req.AuthData = data[pos : pos+authLen]
		pos += authLen
	} else {
		req.AuthData = data[pos : pos+bytes.IndexByte(data[pos:], 0)]
		pos += len(req.AuthData) + 1
	}
	// db
	req.DB = hack.String(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(req.DB) + 1
	if pos >= len(data) {
		return req, nil
	}
	// character set. CLIENT_PROTOCOL_41 is always enabled.
	req.Charset = data[pos : pos+2]
	pos += 2
	// auth plugin
	if capability&ClientPluginAuth > 0 {
		req.AuthPlugin = hack.String(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(req.AuthPlugin) + 1
	}
	// attrs
	var err error
	if capability&ClientConnectAttrs > 0 {
		if num, null, off := ParseLengthEncodedInt(data[pos:]); !null {
			pos += off
			row := data[pos : pos+int(num)]
			req.Attrs, err = parseAttrs(row)
			if err != nil {
				err = &errors.Warning{Err: errors.Wrapf(err, "parse attrs failed")}
			}
		}
	}
	return req, err
}

// CheckSqlPort checks whether the SQL port is available.
func CheckSqlPort(conn net.Conn) error {
	c := packet.NewConn(conn)
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}
	if data[0] == ErrHeader.Byte() {
		return errors.New("read initial handshake error")
	}
	return nil
}

// ParseOKPacket parses an OK packet and only returns server status.
func ParseOKPacket(data []byte) uint16 {
	var pos = 1
	// skip affected rows
	pos += SkipLengthEncodedInt(data[pos:])
	// skip insert id
	pos += SkipLengthEncodedInt(data[pos:])
	// return status
	return binary.LittleEndian.Uint16(data[pos:])
}

// ParseErrorPacket transforms an error packet into a MyError object.
func ParseErrorPacket(data []byte) *gomysql.MyError {
	e := new(gomysql.MyError)
	pos := 1
	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2
	pos++
	e.State = hack.String(data[pos : pos+5])
	pos += 5
	e.Message = hack.String(data[pos:])
	return e
}

// IsOKPacket returns true if it's an OK packet (but not ResultSet OK).
func IsOKPacket(firstByte byte) bool {
	return firstByte == OKHeader.Byte()
}

// IsEOFPacket returns true if it's an EOF packet.
func IsEOFPacket(firstByte byte, length int) bool {
	return firstByte == EOFHeader.Byte() && length <= 5
}

// IsResultSetOKPacket returns true if it's an OK packet after the result set when CLIENT_DEPRECATE_EOF is enabled.
// A row packet may also begin with 0xfe, so we need to judge it with the packet length.
// See https://mariadb.com/kb/en/result-set-packets/
func IsResultSetOKPacket(firstByte byte, length int) bool {
	// With CLIENT_PROTOCOL_41 enabled, the least length is 7.
	return firstByte == EOFHeader.Byte() && length >= 7 && length < 0xFFFFFF
}

// IsErrorPacket returns true if it's an error packet.
func IsErrorPacket(firstByte byte) bool {
	return firstByte == ErrHeader.Byte()
}

// IsMySQLError returns true if the error is a MySQL error.
func IsMySQLError(err error) bool {
	var myerr *gomysql.MyError
	return errors.As(err, &myerr)
}

// The connection attribute names that are logged.
// https://dev.mysql.com/doc/mysql-perfschema-excerpt/8.2/en/performance-schema-connection-attribute-tables.html
const (
	AttrNameClientVersion = "_client_version" // libmysqlclient & Connector/C++ & Connector/J & Connector/Net & Connector/Python
	AttrNameClientName1   = "_client_name"    // libmysqlclient & Connector/C++ & Connector/J & Connector/Python & mysqlnd
	AttrNameClientName2   = "_program_name"   // Connector/Net
	AttrNameProgramName   = "program_name"    // MySQL Client & MySQL Shell
)

// Attr2ZapFields converts connection attributes to log fields.
// We only pick some of them because others may be too sensitive to be logged.
func Attr2ZapFields(attrs map[string]string) []zap.Field {
	fields := make([]zap.Field, 0, 3)
	if attrs != nil {
		if version, ok := attrs[AttrNameClientVersion]; ok {
			fields = append(fields, zap.String("client_version", version))
		}
		if name, ok := attrs[AttrNameClientName1]; ok {
			fields = append(fields, zap.String("client_name", name))
		} else if name, ok := attrs[AttrNameClientName2]; ok {
			fields = append(fields, zap.String("client_name", name))
		}
		if name, ok := attrs[AttrNameProgramName]; ok {
			fields = append(fields, zap.String("program_name", name))
		}
	}
	return fields
}

// ParseQueryPacket returns the statement in the CMD_QUERY packet.
// data is the payload after byte CMD_QUERY.
func ParseQueryPacket(data []byte) string {
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}
	return hack.String(data)
}

func MakeQueryPacket(stmt string) []byte {
	request := make([]byte, len(stmt)+1)
	request[0] = ComQuery.Byte()
	copy(request[1:], hack.Slice(stmt))
	return request
}

func MakePrepareStmtPacket(stmt string) []byte {
	request := make([]byte, len(stmt)+1)
	request[0] = ComStmtPrepare.Byte()
	copy(request[1:], hack.Slice(stmt))
	return request
}

func MakeExecuteStmtPacket(stmtID uint32, args []any) ([]byte, error) {
	paramNum := len(args)
	paramTypes := make([]byte, paramNum*2)
	paramValues := make([][]byte, paramNum)
	nullBitmap := make([]byte, (paramNum+7)>>3)
	dataLen := 1 + 4 + 1 + 4 + len(nullBitmap) + 1 + len(paramTypes)
	var newParamBoundFlag byte = 0

	for i := range args {
		if args[i] == nil {
			nullBitmap[i/8] |= 1 << (uint(i) % 8)
			paramTypes[i<<1] = fieldTypeNULL
			continue
		}

		newParamBoundFlag = 1
		switch v := args[i].(type) {
		case int64:
			paramTypes[i<<1] = fieldTypeLongLong
			paramValues[i] = Uint64ToBytes(uint64(v))
		case uint64:
			paramTypes[i<<1] = fieldTypeLongLong
			paramTypes[(i<<1)+1] = 0x80
			paramValues[i] = Uint64ToBytes(v)
		case float64:
			paramTypes[i<<1] = fieldTypeDouble
			paramValues[i] = Uint64ToBytes(math.Float64bits(v))
		case string:
			paramTypes[i<<1] = fieldTypeString
			paramValues[i] = DumpLengthEncodedString(nil, hack.Slice(v))
		default:
			// we don't need other types currently
			return nil, errors.WithStack(errors.Errorf("unsupported type %T", v))
		}

		dataLen += len(paramValues[i])
	}

	request := make([]byte, dataLen)
	pos := 0
	request[pos] = ComStmtExecute.Byte()
	pos += 1
	binary.LittleEndian.PutUint32(request[pos:], uint32(stmtID))
	pos += 4
	// cursor flag
	pos += 1
	// iteration count
	request[pos] = 1
	pos += 4

	if paramNum > 0 {
		copy(request[pos:], nullBitmap)
		pos += len(nullBitmap)
		request[pos] = newParamBoundFlag
		pos++
		if newParamBoundFlag == 1 {
			copy(request[pos:], paramTypes)
			pos += len(paramTypes)
		}
		for _, v := range paramValues {
			copy(request[pos:], v)
			pos += len(v)
		}
	}
	return request, nil
}
