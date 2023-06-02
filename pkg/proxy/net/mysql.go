// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"bytes"
	"encoding/binary"
	"net"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/siddontang/go/hack"
)

const (
	ShaCommand   = 1
	FastAuthFail = 4
)

var (
	ServerVersion = mysql.ServerVersion
	Collation     = uint8(mysql.DefaultCollationID)
	ConnID        = 100
	Status        = mysql.ServerStatusAutocommit
)

// ParseInitialHandshake parses the initial handshake received from the server.
func ParseInitialHandshake(data []byte) (Capability, string) {
	// skip min version
	serverVersion := string(data[1 : 1+bytes.IndexByte(data[1:], 0)])
	pos := 1 + len(serverVersion) + 1
	// skip connection id
	// skip salt first part
	// skip filter
	pos += 4 + 8 + 1

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
		// skip salt second part
		// skip auth plugin
	}
	return Capability(capability), serverVersion
}

// HandshakeResp indicates the response read from the client.
type HandshakeResp struct {
	Attrs      map[string]string
	User       string
	DB         string
	AuthPlugin string
	AuthData   []byte
	Capability uint32
	Collation  uint8
}

func ParseHandshakeResponse(data []byte) (*HandshakeResp, error) {
	resp := new(HandshakeResp)
	pos := 0
	// capability
	resp.Capability = binary.LittleEndian.Uint32(data[:4])
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
	if resp.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
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
	} else if resp.Capability&mysql.ClientSecureConnection > 0 {
		authLen := int(data[pos])
		pos++
		resp.AuthData = data[pos : pos+authLen]
		pos += authLen
	} else {
		resp.AuthData = data[pos : pos+bytes.IndexByte(data[pos:], 0)]
		pos += len(resp.AuthData) + 1
	}

	// dbname
	if resp.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[pos:]) > 0 {
			idx := bytes.IndexByte(data[pos:], 0)
			resp.DB = string(data[pos : pos+idx])
			pos = pos + idx + 1
		}
	}

	// auth plugin
	if resp.Capability&mysql.ClientPluginAuth > 0 {
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
	if resp.Capability&mysql.ClientConnectAtts > 0 {
		if num, null, off := ParseLengthEncodedInt(data[pos:]); !null {
			pos += off
			row := data[pos : pos+int(num)]
			resp.Attrs, err = parseAttrs(row)
			if err != nil {
				err = &errors.Warning{Err: errors.Wrapf(err, "parse attrs failed")}
			}
		}
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
		capability |= mysql.ClientPluginAuthLenencClientData
	} else {
		capability &= ^mysql.ClientPluginAuthLenencClientData
	}
	if capability&mysql.ClientConnectAtts > 0 {
		attrs = dumpAttrs(resp.Attrs)
		attrBuf = DumpLengthEncodedInt(attrLenBuf[:0], uint64(len(attrs)))
	}

	length := 4 + 4 + 1 + 23 + len(resp.User) + 1 + len(authResp) + len(resp.AuthData) + len(resp.DB) + 1 + len(resp.AuthPlugin) + 1 + len(attrBuf) + len(attrs)
	data := make([]byte, length)
	pos := 0
	// capability [32 bit]
	DumpUint32(data[:0], capability)
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
	if capability&mysql.ClientPluginAuthLenencClientData > 0 {
		pos += copy(data[pos:], authResp)
		pos += copy(data[pos:], resp.AuthData)
	} else if capability&mysql.ClientSecureConnection > 0 {
		data[pos] = byte(len(resp.AuthData))
		pos++
		pos += copy(data[pos:], resp.AuthData)
	} else {
		pos += copy(data[pos:], resp.AuthData)
		data[pos] = 0x00
		pos++
	}

	// db [null terminated string]
	if capability&mysql.ClientConnectWithDB > 0 {
		pos += copy(data[pos:], resp.DB)
		data[pos] = 0x00
		pos++
	}

	// auth_plugin [null terminated string]
	if capability&mysql.ClientPluginAuth > 0 {
		pos += copy(data[pos:], resp.AuthPlugin)
		data[pos] = 0x00
		pos++
	}

	// attrs
	if capability&mysql.ClientConnectAtts > 0 {
		pos += copy(data[pos:], attrBuf)
		pos += copy(data[pos:], attrs)
	}
	return data[:pos]
}

// MakeChangeUser creates the data of COM_CHANGE_USER. It's only used for testing.
func MakeChangeUser(username, db, authPlugin string, authData []byte) []byte {
	length := 1 + len(username) + 1 + len(authData) + 1 + len(db) + 1
	data := make([]byte, 0, length)
	data = append(data, mysql.ComChangeUser)
	data = append(data, []byte(username)...)
	data = append(data, 0x00)
	data = append(data, byte(len(authData)))
	data = append(data, authData...)
	data = append(data, []byte(db)...)
	data = append(data, 0x00)

	// character set
	data = append(data, 0x00)
	data = append(data, 0x00)

	data = append(data, []byte(authPlugin)...)
	data = append(data, 0x00)
	return data
}

// ParseChangeUser parses the data of COM_CHANGE_USER.
func ParseChangeUser(data []byte) (username, db string) {
	user, data := ParseNullTermString(data[1:])
	username = string(user)
	passLen := int(data[0])
	data = data[passLen+1:]
	dbName, _ := ParseNullTermString(data)
	db = string(dbName)
	// TODO: attrs
	return
}

// ReadServerVersion only reads server version.
func ReadServerVersion(conn net.Conn) (string, error) {
	c := packet.NewConn(conn)
	data, err := c.ReadPacket()
	if err != nil {
		return "", err
	}
	if data[0] == gomysql.ERR_HEADER {
		return "", errors.New("read initial handshake error")
	}
	pos := 1
	version := data[pos : bytes.IndexByte(data[pos:], 0x00)+1]
	return string(version), nil
}

// WriteServerVersion only writes server version. It's only used for testing.
func WriteServerVersion(conn net.Conn, serverVersion string) error {
	data := make([]byte, 0, 128)
	data = append(data, []byte{0, 0, 0, 0}...)
	// min version 10
	data = append(data, 10)
	// server version[NUL]
	data = append(data, serverVersion...)
	data = append(data, 0)
	c := packet.NewConn(conn)
	return c.WritePacket(data)
}

// ParseOKPacket transforms an OK packet into a Result object.
func ParseOKPacket(data []byte) *gomysql.Result {
	var n int
	var pos = 1
	r := new(gomysql.Result)
	r.AffectedRows, _, n = ParseLengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = ParseLengthEncodedInt(data[pos:])
	pos += n
	r.Status = binary.LittleEndian.Uint16(data[pos:])
	return r
}

// ParseErrorPacket transforms an error packet into a MyError object.
func ParseErrorPacket(data []byte) error {
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
func IsOKPacket(data []byte) bool {
	return data[0] == mysql.OKHeader
}

// IsEOFPacket returns true if it's an EOF packet.
func IsEOFPacket(data []byte) bool {
	return data[0] == mysql.EOFHeader && len(data) <= 5
}

// IsResultSetOKPacket returns true if it's an OK packet after the result set when CLIENT_DEPRECATE_EOF is enabled.
// A row packet may also begin with 0xfe, so we need to judge it with the packet length.
// See https://mariadb.com/kb/en/result-set-packets/
func IsResultSetOKPacket(data []byte) bool {
	// With CLIENT_PROTOCOL_41 enabled, the least length is 7.
	return data[0] == mysql.EOFHeader && len(data) >= 7 && len(data) < 0xFFFFFF
}

// IsErrorPacket returns true if it's an error packet.
func IsErrorPacket(data []byte) bool {
	return data[0] == mysql.ErrHeader
}
