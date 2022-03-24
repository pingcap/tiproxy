package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
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

func (auth *Authenticator) handshake(ctx context.Context, clientIO, backendIO *pnet.PacketIO) error {
	backendIO.ResetSequence()
	var serverPkt, clientPkt []byte
	var err error
	sequence := 0
	for {
		serverPkt, err = backendIO.ReadPacket()
		if err != nil {
			return err
		}
		err = clientIO.WritePacket(serverPkt)
		if err != nil {
			return err
		}
		err = clientIO.Flush()
		if err != nil {
			return err
		}
		// Authentication finished.
		if len(serverPkt) > 1 {
			finished := false
			switch serverPkt[0] {
			case mysql.OKHeader, mysql.ErrHeader:
				finished = true
			default:
				if sequence == 1 {
					auth.readHandshakeResponse(clientPkt)
				}
			}
			if finished {
				break
			}
		}
		clientPkt, err = clientIO.ReadPacket()
		if err != nil {
			return err
		}
		err = backendIO.WritePacket(clientPkt)
		if err != nil {
			return err
		}
		err = backendIO.Flush()
		if err != nil {
			return err
		}
		sequence++
	}
	return nil
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
	// header
	offset := 0
	auth.capability = binary.LittleEndian.Uint32(data[:4])
	offset += 8
	auth.collation = data[offset]
	offset += 24

	// user name
	auth.user = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(auth.user) + 1

	if auth.capability&mysql.ClientPluginAuthLenencClientData > 0 {
		num, null, off := pnet.ParseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			auth.authData = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else if auth.capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		auth.authData = data[offset : offset+authLen]
		offset += authLen
	} else {
		auth.authData = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(auth.authData) + 1
	}

	if auth.capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			auth.dbname = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
	}

	if auth.capability&mysql.ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[offset:], 0)
		s := offset
		f := offset + idx
		if s < f {
			auth.authPlugin = string(data[s:f])
		}
		offset += idx + 1
	}

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

	if auth.capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			auth.dbname = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
		if len(data[offset:]) > 0 {
			auth.authData = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	} else {
		auth.authData = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
	}

	auth.authPlugin = mysql.AuthNativePassword
}
