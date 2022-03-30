package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const ShaCommand = 1

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
	logutil.BgLogger().Info("parse client handshake response finished", zap.String("authInfo", auth.String()))
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
	return nil
}
