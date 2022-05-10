package backend

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Authenticator handshakes with the client and the backend.
type Authenticator struct {
	user             string
	dbname           string // default database name
	capability       uint32 // client capability
	collation        uint8
	attrs            []byte // no need to parse
	backendTLSConfig *tls.Config
}

func (auth *Authenticator) String() string {
	return fmt.Sprintf("user:%s, dbname:%s, capability:%d, collation:%d",
		auth.user, auth.dbname, auth.capability, auth.collation)
}

func (auth *Authenticator) handshakeFirstTime(clientIO, backendIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) (bool, error) {
	backendIO.ResetSequence()
	var (
		serverPkt, clientPkt []byte
		err                  error
		serverCapability     uint32
	)

	// Read initial handshake packet from the backend.
	serverPkt, serverCapability, err = auth.readInitialHandshake(backendIO)
	if serverPkt != nil {
		writeErr := clientIO.WritePacket(serverPkt)
		if writeErr != nil {
			return false, writeErr
		}
		if writeErr = clientIO.Flush(); writeErr != nil {
			return false, writeErr
		}
		if err != nil {
			return false, nil
		}
	} else {
		return false, err
	}
	if serverCapability&mysql.ClientSSL == 0 {
		return false, errors.New("the TiDB server must enable TLS")
	}

	// Read the response from the client.
	if clientPkt, err = clientIO.ReadPacket(); err != nil {
		return false, err
	}
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	// A 2-bytes capability contains the ClientSSL flag, no matter ClientProtocol41 is set or not.
	sslEnabled := uint32(capability)&mysql.ClientSSL > 0
	if sslEnabled {
		// Upgrade TLS with the client if SSL is enabled.
		if err = clientIO.UpgradeToServerTLS(serverTLSConfig); err != nil {
			return false, err
		}
	} else {
		// Rewrite the packet with ClientSSL enabled because we always connect to TiDB with TLS.
		pktWithSSL := make([]byte, len(clientPkt))
		pnet.DumpUint16(pktWithSSL[:0], capability|uint16(mysql.ClientSSL))
		copy(pktWithSSL[2:], clientPkt[2:])
		clientPkt = pktWithSSL
	}
	if err = backendIO.WritePacket(clientPkt); err != nil {
		return false, err
	}
	if err = backendIO.Flush(); err != nil {
		return false, err
	}
	// Always upgrade TLS with the server.
	auth.backendTLSConfig = backendTLSConfig
	if err = backendIO.UpgradeToClientTLS(backendTLSConfig); err != nil {
		return false, err
	}
	if sslEnabled {
		// Read from the client again, where the capability may not contain ClientSSL this time.
		if clientPkt, err = clientIO.ReadPacket(); err != nil {
			return false, err
		}
	}
	// Send the response again.
	if err = backendIO.WritePacket(clientPkt); err != nil {
		return false, err
	}
	if err = backendIO.Flush(); err != nil {
		return false, err
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
			logutil.BgLogger().Debug("parse client handshake response finished", zap.String("authInfo", auth.String()))
			return true, nil
		case mysql.ErrHeader:
			return false, nil
		default: // mysql.AuthSwitchRequest, ShaCommand
			clientPkt, err = forwardMsg(clientIO, backendIO)
			if err != nil {
				return false, err
			}
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
			offset += int(num)
		}
	} else if auth.capability&mysql.ClientSecureConnection > 0 {
		authLen := int(data[offset])
		offset += authLen + 1
	} else {
		authLen := bytes.IndexByte(data[offset:], 0)
		offset += authLen + 1
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
	// skip auth data
}

func (auth *Authenticator) handshakeSecondTime(backendIO *pnet.PacketIO, sessionToken string) error {
	if len(sessionToken) == 0 {
		return errors.New("session token is empty")
	}

	_, serverCapability, err := auth.readInitialHandshake(backendIO)
	if err != nil {
		return err
	}
	if serverCapability&mysql.ClientSSL == 0 {
		return errors.New("the TiDB server must enable TLS")
	}

	tokenBytes := hack.Slice(sessionToken)
	err = auth.writeAuthHandshake(backendIO, tokenBytes)
	if err != nil {
		return err
	}

	return auth.handleSecondAuthResult(backendIO)
}

func (auth *Authenticator) readInitialHandshake(backendIO *pnet.PacketIO) (serverPkt []byte, capability uint32, err error) {
	if serverPkt, err = backendIO.ReadPacket(); err != nil {
		err = errors.Trace(err)
		return
	}

	if serverPkt[0] == mysql.ErrHeader {
		err = errors.New("read initial handshake error")
		return
	}

	// skip mysql version
	pos := 1 + bytes.IndexByte(serverPkt[1:], 0x00) + 1
	// skip connection id
	// skip salt first part
	// skip filter
	pos += 4 + 8 + 1

	// capability lower 2 bytes
	capability = uint32(binary.LittleEndian.Uint16(serverPkt[pos : pos+2]))
	pos += 2

	if len(serverPkt) > pos {
		// skip server charset + status
		pos += 1 + 2
		// capability flags (upper 2 bytes)
		capability = uint32(binary.LittleEndian.Uint16(serverPkt[pos:pos+2]))<<16 | capability

		// skip auth data len or [00]
		// skip reserved (all [00])
		// skip salt second part
		// skip auth plugin
	}
	return
}

func (auth *Authenticator) writeAuthHandshake(backendIO *pnet.PacketIO, authData []byte) error {
	// encode length of the auth data
	var (
		authRespBuf, attrRespBuf [9]byte
		authResp, attrResp       []byte
	)
	authResp = pnet.DumpLengthEncodedInt(authRespBuf[:0], uint64(len(authData)))
	capability := auth.capability
	if len(authResp) > 1 {
		capability |= mysql.ClientPluginAuthLenencClientData
	} else {
		capability &= ^mysql.ClientPluginAuthLenencClientData
	}
	// Always handshake with SSL enabled.
	capability |= mysql.ClientSSL
	if capability&mysql.ClientConnectAtts > 0 {
		attrResp = pnet.DumpLengthEncodedInt(attrRespBuf[:0], uint64(len(auth.attrs)))
	}

	//packet length
	//capability 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	//username
	//auth
	//mysql_native_password + null-terminated
	//attrs
	length := 4 + 4 + 1 + 23 + len(auth.user) + 1 + len(authResp) + len(authData) + 21 + 1 + len(attrResp) + len(auth.attrs)
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

	// Send TLS / SSL request packet. The server must have supported TLS.
	err := backendIO.WritePacket(data[:pos])
	if err != nil {
		return err
	}
	if err = backendIO.Flush(); err != nil {
		return err
	}
	if err = backendIO.UpgradeToClientTLS(auth.backendTLSConfig); err != nil {
		return err
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

	// auth_plugin
	pos += copy(data[pos:], mysql.AuthTiDBSessionToken)
	data[pos] = 0x00
	pos++

	// attrs
	if auth.capability&mysql.ClientConnectAtts > 0 {
		pos += copy(data[pos:], attrResp)
		pos += copy(data[pos:], auth.attrs)
	}

	if err = backendIO.WritePacket(data); err != nil {
		return err
	}
	return backendIO.Flush()
}

func (auth *Authenticator) handleSecondAuthResult(backendIO *pnet.PacketIO) error {
	data, err := backendIO.ReadPacket()
	if err != nil {
		return err
	}

	switch data[0] {
	case mysql.OKHeader:
		return nil
	case mysql.ErrHeader:
		return errors.New("auth failed")
	default: // mysql.AuthSwitchRequest, ShaCommand:
		return errors.Errorf("read unexpected command: %#x", data[0])
	}
}

func (auth *Authenticator) changeUser(request []byte) {
	user, data := pnet.ParseNullTermString(request)
	auth.user = string(hack.String(user))
	passLen := int(data[0])
	data = data[passLen+1:]
	dbName, _ := pnet.ParseNullTermString(data)
	auth.dbname = string(hack.String(dbName))
	// TODO: attrs
}
