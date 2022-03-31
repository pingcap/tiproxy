package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/djshow832/weir/pkg/util/passwd"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	ShaCommand       = 1
	RequestRsaPubKey = 2
	FastAuthOk       = 3
	FastAuthFail     = 4
)

type Authenticator struct {
	user       string
	authData   []byte // password
	authPlugin string
	dbname     string // default database name
	capability uint32 // client capability
	collation  uint8
	attrs      []byte // no need to parse
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, authData:%s, authPlugin:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, string(auth.authData), auth.authPlugin, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) handshakeWithClient(ctx context.Context, clientIO, backendIO *pnet.PacketIO, serverTLSConfig *tls.Config) (bool, error) {
	backendIO.ResetSequence()
	var serverPkt, clientPkt []byte
	var err error

	// initial handshake packet
	serverPkt, err = forwardMsg(backendIO, clientIO)
	if err != nil {
		return false, err
	}
	if serverPkt[0] == mysql.ErrHeader {
		return false, nil
	}

	// ssl request + response
	clientPkt, err = forwardMsg(clientIO, backendIO)
	if err != nil {
		return false, err
	}
	capability := uint32(binary.LittleEndian.Uint16(clientPkt[:2]))
	// A 2-bytes capability contains the ClientSSL flag, no matter ClientProtocol41 is set or not.
	if capability&mysql.ClientSSL > 0 {
		// Upgrade with the client.
		serverTLSConfig = serverTLSConfig.Clone()
		err = clientIO.UpgradeToServerTLS(serverTLSConfig)
		if err != nil {
			return false, err
		}
		// Upgrade with the server.
		err = backendIO.UpgradeToClientTLS(&tls.Config{InsecureSkipVerify: true})
		if err != nil {
			return false, err
		}
		// The client will send another response.
		clientPkt, err = forwardMsg(clientIO, backendIO)
		if err != nil {
			return false, err
		}
	}
	auth.readHandshakeResponse(clientPkt)

	// verify password
	for {
		serverPkt, err = forwardMsg(backendIO, clientIO)
		if err != nil {
			return false, err
		}
		switch serverPkt[0] {
		case mysql.OKHeader:
			logutil.BgLogger().Info("parse client handshake response finished", zap.String("authInfo", auth.String()))
			return true, nil
		case mysql.ErrHeader:
			return false, nil
		case mysql.AuthSwitchRequest:
			clientPkt, err = forwardMsg(clientIO, backendIO)
			if err != nil {
				return false, err
			}
			pluginEndIndex := bytes.IndexByte(serverPkt, 0x00)
			auth.authPlugin = string(serverPkt[1:pluginEndIndex])
			auth.authData = clientPkt
		case ShaCommand:
			clientPkt, err = forwardMsg(clientIO, backendIO)
			if err != nil {
				return false, err
			}
			auth.authPlugin = mysql.AuthCachingSha2Password
			auth.authData = bytes.Trim(clientPkt, "\x00")
		}
	}
}

func forwardMsg(srcIO, destIO *pnet.PacketIO) (data []byte, err error) {
	data, err = srcIO.ReadPacket()
	if err != nil {
		return
	}
	err = destIO.WritePacket(data)
	if err != nil {
		return
	}
	err = destIO.Flush()
	if err != nil {
		return
	}
	return
}

func (auth *Authenticator) readHandshakeResponse(data []byte) {
	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&mysql.ClientProtocol41 > 0 {
		auth.parseHandshakeResponse(data)
	} else {
		auth.parseOldHandshakeResponse(data)
	}
}

func (auth *Authenticator) parseHandshakeResponse(data []byte) {
	offset := 0

	// capability
	auth.capability = binary.LittleEndian.Uint32(data[:4])

	// collation
	offset += 8
	auth.collation = data[offset]
	offset += 24

	// user name
	auth.user = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(auth.user) + 1

	// password
	if auth.capability&mysql.ClientPluginAuthLenencClientData > 0 {
		num, null, off := pnet.ParseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			auth.authData = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else if auth.capability&mysql.ClientSecureConnection > 0 {
		authLen := int(data[offset])
		offset++
		auth.authData = data[offset : offset+authLen]
		offset += authLen
	} else {
		auth.authData = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(auth.authData) + 1
	}

	// dbname
	if auth.capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			auth.dbname = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	// auth plugin
	if auth.capability&mysql.ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[offset:], 0)
		s := offset
		f := offset + idx
		if s < f {
			auth.authPlugin = string(data[s:f])
		}
		offset += idx + 1
	}

	// attrs
	if auth.capability&mysql.ClientConnectAtts > 0 {
		if num, null, off := pnet.ParseLengthEncodedInt(data[offset:]); !null {
			offset += off
			auth.attrs = data[offset : offset+int(num)]
		}
	}
}

func (auth *Authenticator) parseOldHandshakeResponse(data []byte) {
	offset := 0
	// capability
	auth.capability = uint32(binary.LittleEndian.Uint16(data[:2]))
	auth.capability |= mysql.ClientProtocol41

	//collation
	offset += 5
	auth.collation = mysql.CollationNames["utf8mb4_general_ci"]

	// user name
	auth.user = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(auth.user) + 1

	// db name
	if auth.capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			auth.dbname = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	// password
	if len(data[offset:]) > 0 {
		auth.authData = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
	}
	auth.authPlugin = mysql.AuthNativePassword
}

func (auth *Authenticator) handshakeWithServer(ctx context.Context, backendIO *pnet.PacketIO) error {
	if auth.authPlugin != mysql.AuthCachingSha2Password {
		return errors.New(fmt.Sprintf("only support %s now", mysql.AuthCachingSha2Password))
	}

	salt, capability, authPlugin, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return err
	}

	err = auth.writeAuthHandshake(backendIO, salt, capability, authPlugin)
	if err != nil {
		return err
	}

	return auth.handleAuthResult(backendIO)
}

func (auth *Authenticator) readInitialHandshake(backendIO *pnet.PacketIO) (salt []byte, capability uint32, authPlugin string, err error) {
	data, err := backendIO.ReadPacket()
	if err != nil {
		return nil, 0, "", errors.Trace(err)
	}

	if data[0] == mysql.ErrHeader {
		return nil, 0, "", errors.New("read initial handshake error")
	}

	// Skip mysql version, mysql version end with 0x00.
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1

	// skip connection id
	pos += 4
	salt = append(salt, data[pos:pos+8]...)

	// skip filter
	pos += 8 + 1
	// capability lower 2 bytes
	capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	if len(data) > pos {
		// skip server charset + status
		pos += 1 + 2
		// capability flags (upper 2 bytes)
		capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | capability
		pos += 2

		// skip auth data len or [00]
		// skip reserved (all [00])
		pos += 10 + 1
		salt = append(salt, data[pos:pos+12]...)
		pos += 13

		// auth plugin
		if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
			authPlugin = string(data[pos : pos+end])
		} else {
			authPlugin = string(data[pos:])
		}
	}
	return
}

func (auth *Authenticator) writeAuthHandshake(backendIO *pnet.PacketIO, salt []byte, serverCapability uint32, authPlugin string) error {
	authData := passwd.CalculatePassword(salt, auth.authData, auth.authPlugin)

	// encode length of the auth plugin data
	var authRespBuf [9]byte
	authResp := pnet.DumpLengthEncodedInt(authRespBuf[:0], uint64(len(authData)))
	capability := auth.capability
	if len(authResp) > 1 {
		capability |= mysql.ClientPluginAuthLenencClientData
	} else {
		capability &= ^mysql.ClientPluginAuthLenencClientData
	}

	//packet length
	//capability 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	//username
	//auth
	//mysql_native_password + null-terminated
	length := 4 + 4 + 1 + 23 + len(auth.user) + 1 + len(authResp) + len(authData) + 21 + 1
	// db name
	if len(auth.dbname) > 0 {
		length += len(auth.dbname) + 1
	}

	data := make([]byte, length)
	pos := 0

	// capability [32 bit]
	pnet.DumpUint32(data[:0], capability)
	pos += 4

	// MaxPacketSize [32 bit] (none)
	for i := 0; i < 4; i++ {
		data[pos] = 0x00
		pos++
	}

	// Charset [1 byte]
	data[pos] = auth.collation
	pos++

	// Filler [23 bytes] (all 0x00)
	for i := 0; i < 23; i++ {
		data[pos] = 0
		pos++
	}

	var err error
	// SSL Connection Request Packet
	if serverCapability&mysql.ClientSSL > 0 {
		// Send TLS / SSL request packet
		if err = backendIO.WritePacket(data[:pos]); err != nil {
			return err
		}
		if err = backendIO.Flush(); err != nil {
			return err
		}
		if err = backendIO.UpgradeToClientTLS(&tls.Config{InsecureSkipVerify: true}); err != nil {
			return err
		}
	}

	// User [null terminated string]
	if len(auth.user) > 0 {
		pos += copy(data[pos:], auth.user)
	}
	data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	pos += copy(data[pos:], authResp)
	pos += copy(data[pos:], authData)

	// db [null terminated string]
	if len(auth.dbname) > 0 {
		pos += copy(data[pos:], auth.dbname)
		data[pos] = 0x00
		pos++
	}

	// Assume native client during response
	pos += copy(data[pos:], auth.authPlugin)
	data[pos] = 0x00

	if err = backendIO.WritePacket(data); err != nil {
		return err
	}
	return backendIO.Flush()
}

func (auth *Authenticator) handleAuthResult(backendIO *pnet.PacketIO) error {
	for {
		data, err := backendIO.ReadPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case mysql.OKHeader:
			return nil
		case ShaCommand:
			switch data[1] {
			case FastAuthOk:
			case FastAuthFail:
				if err = auth.writeAuthPacket(backendIO, auth.authData, true); err != nil {
					return err
				}
			default:
				return errors.New(fmt.Sprintf("do not support sha command %d now", data[1]))
			}
		case mysql.AuthSwitchRequest:
			pluginEndIndex := bytes.IndexByte(data, 0x00)
			plugin := string(data[1:pluginEndIndex])
			salt := data[pluginEndIndex+1:]
			authData := passwd.CalculatePassword(salt, auth.authData, plugin)
			if err = auth.writeAuthPacket(backendIO, authData, false); err != nil {
				return err
			}
		default: // mysql.ErrHeader
			return errors.New("read auth result error")
		}
	}
}

func (auth *Authenticator) writeAuthPacket(backendIO *pnet.PacketIO, password []byte, addNul bool) error {
	pktLen := len(password)
	if addNul {
		pktLen++
	}

	data := make([]byte, pktLen)
	copy(data, password)
	if addNul {
		data[pktLen-1] = 0x00
	}
	if err := backendIO.WritePacket(data); err != nil {
		return err
	}
	return backendIO.Flush()
}
