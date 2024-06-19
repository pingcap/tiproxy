// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"crypto/tls"
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

type backendConfig struct {
	salt          [20]byte
	tlsConfig     *tls.Config
	authPlugin    string
	sessionStates string
	columns       int
	loops         int
	params        int
	rows          int
	respondType   respondType
	stmtNum       int
	capability    pnet.Capability
	status        uint16
	proxyProtocol bool
	authSucceed   bool
	abnormalExit  bool
	exitInResult  bool
}

func newBackendConfig() *backendConfig {
	return &backendConfig{
		capability:    defaultTestBackendCapability,
		salt:          mockSalt,
		authPlugin:    pnet.AuthCachingSha2Password,
		authSucceed:   true,
		loops:         1,
		stmtNum:       1,
		sessionStates: mockSessionStates,
	}
}

type mockBackend struct {
	err error
	// Inputs that assigned by the test and will be sent to the client.
	*backendConfig
	// Outputs that received from the client and will be checked by the test.
	username  string
	db        string
	attrs     map[string]string
	authData  []byte
	zstdLevel int
}

func newMockBackend(cfg *backendConfig) *mockBackend {
	return &mockBackend{
		backendConfig: cfg,
	}
}

func (mb *mockBackend) authenticate(packetIO *pnet.PacketIO) error {
	if mb.abnormalExit {
		return packetIO.Close()
	}
	if mb.proxyProtocol {
		packetIO.ApplyOpts(pnet.WithProxy)
	}
	var err error
	// write initial handshake
	if err = packetIO.WriteInitialHandshake(mb.capability, mb.salt, mb.authPlugin, pnet.ServerVersion, 100); err != nil {
		return err
	}
	// read the response
	var clientPkt []byte
	// Unlike TiProxy, TiDB always sends an error to the client even if EOF.
	if clientPkt, err = packetIO.ReadPacket(); err != nil {
		return packetIO.WriteErrPacket(mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error()))
	}
	// upgrade to TLS
	capability := binary.LittleEndian.Uint16(clientPkt[:2])
	sslEnabled := pnet.Capability(capability)&pnet.ClientSSL > 0 && mb.capability&pnet.ClientSSL > 0
	if sslEnabled {
		if _, err = packetIO.ServerTLSHandshake(mb.tlsConfig); err != nil {
			return err
		}
		// read the response again
		if clientPkt, err = packetIO.ReadPacket(); err != nil {
			return err
		}
	}
	resp, err := pnet.ParseHandshakeResponse(clientPkt)
	if err != nil {
		return err
	}
	mb.username = resp.User
	mb.db = resp.DB
	mb.authData = resp.AuthData
	mb.attrs = resp.Attrs
	mb.capability = resp.Capability
	mb.zstdLevel = resp.ZstdLevel
	// verify password
	return mb.verifyPassword(packetIO, resp)
}

func (mb *mockBackend) verifyPassword(packetIO *pnet.PacketIO, resp *pnet.HandshakeResp) error {
	if resp.AuthPlugin != pnet.AuthTiDBSessionToken {
		var err error
		if err = packetIO.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if mb.authData, err = packetIO.ReadPacket(); err != nil {
			return err
		}
		switch mb.authPlugin {
		case pnet.AuthCachingSha2Password:
			if err = packetIO.WriteShaCommand(); err != nil {
				return err
			}
			if mb.authData, err = packetIO.ReadPacket(); err != nil {
				return err
			}
		}
	}
	if mb.authSucceed {
		if err := packetIO.WriteOKPacket(mb.status, pnet.OKHeader); err != nil {
			return err
		}
		if err := setCompress(packetIO, mb.capability, mb.zstdLevel); err != nil {
			return err
		}
	} else {
		if err := packetIO.WriteErrPacket(mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR)); err != nil {
			return err
		}
	}
	return nil
}

func (mb *mockBackend) changeUser(pkt []byte) error {
	req, err := pnet.ParseChangeUser(pkt, mb.capability)
	if err != nil {
		return err
	}
	mb.username = req.User
	mb.db = req.DB
	mb.attrs = req.Attrs
	mb.authData = req.AuthData
	mb.authPlugin = req.AuthPlugin
	if mb.authPlugin != unknownAuthPlugin {
		return errors.New("should use different auth plugin")
	}
	return nil
}

func (mb *mockBackend) respond(packetIO *pnet.PacketIO) error {
	if mb.abnormalExit {
		return packetIO.Close()
	}
	for i := 0; i < mb.loops; i++ {
		if err := mb.respondOnce(packetIO); err != nil {
			return err
		}
	}
	return nil
}

func (mb *mockBackend) respondOnce(packetIO *pnet.PacketIO) error {
	packetIO.ResetSequence()
	pkt, err := packetIO.ReadPacket()
	if err != nil {
		return err
	}
	if pnet.Command(pkt[0]) == pnet.ComChangeUser {
		if err := mb.changeUser(pkt); err != nil {
			return err
		}
	}
	switch mb.respondType {
	case responseTypeOK:
		return mb.respondOK(packetIO)
	case responseTypeErr:
		return packetIO.WriteErrPacket(mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR))
	case responseTypeResultSet:
		if pnet.Command(pkt[0]) == pnet.ComQuery && string(pkt[1:]) == sqlQueryState {
			return mb.respondSessionStates(packetIO)
		}
		return mb.respondResultSet(packetIO)
	case responseTypeColumn:
		return mb.respondColumns(packetIO)
	case responseTypeLoadFile:
		return mb.respondLoadFile(packetIO)
	case responseTypeString:
		return packetIO.WritePacket([]byte(mockCmdStr), true)
	case responseTypeEOF:
		return packetIO.WriteEOFPacket(mb.status)
	case responseTypeSwitchRequest:
		if err := packetIO.WriteSwitchRequest(mb.authPlugin, mb.salt); err != nil {
			return err
		}
		if _, err := packetIO.ReadPacket(); err != nil {
			return err
		}
		return packetIO.WriteOKPacket(mb.status, pnet.OKHeader)
	case responseTypePrepareOK:
		return mb.respondPrepare(packetIO)
	case responseTypeRow:
		return mb.respondRows(packetIO)
	case responseTypeNone:
		return nil
	}
	return packetIO.WriteErrPacket(mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR))
}

func (mb *mockBackend) respondOK(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.stmtNum; i++ {
		status := mb.status
		if i < mb.stmtNum-1 {
			status |= mysql.SERVER_MORE_RESULTS_EXISTS
		} else {
			status &= ^mysql.SERVER_MORE_RESULTS_EXISTS
		}
		if err := packetIO.WriteOKPacket(status, pnet.OKHeader); err != nil {
			return err
		}
	}
	return nil
}

// respond to FieldList
func (mb *mockBackend) respondColumns(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.columns; i++ {
		if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
			return err
		}
	}
	return mb.writeResultEndPacket(packetIO, mb.status)
}

func (mb *mockBackend) writeResultEndPacket(packetIO *pnet.PacketIO, status uint16) error {
	if mb.capability&pnet.ClientDeprecateEOF > 0 {
		return packetIO.WriteOKPacket(status, pnet.EOFHeader)
	}
	return packetIO.WriteEOFPacket(status)
}

// respond to Fetch
func (mb *mockBackend) respondRows(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.rows; i++ {
		if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
			return err
		}
	}
	return mb.writeResultEndPacket(packetIO, mb.status)
}

// respond to Query
func (mb *mockBackend) respondResultSet(packetIO *pnet.PacketIO) error {
	names := make([]string, 0, mb.columns)
	values := make([][]any, 0, mb.rows)
	for i := 0; i < mb.columns; i++ {
		names = append(names, mockCmdStr)
	}
	for i := 0; i < mb.rows; i++ {
		row := make([]any, 0, mb.columns)
		for j := 0; j < mb.columns; j++ {
			row = append(row, mockCmdStr)
		}
		values = append(values, row)
	}
	return mb.writeResultSet(packetIO, names, values)
}

func (mb *mockBackend) writeResultSet(packetIO *pnet.PacketIO, names []string, values [][]any) error {
	rs, err := mysql.BuildSimpleTextResultset(names, values)
	if err != nil {
		return err
	}
	for i := 0; i < mb.stmtNum; i++ {
		status := mb.status
		if i < mb.stmtNum-1 {
			status |= mysql.SERVER_MORE_RESULTS_EXISTS
		} else {
			status &= ^mysql.SERVER_MORE_RESULTS_EXISTS
		}
		data := pnet.DumpLengthEncodedInt(nil, uint64(len(names)))
		if err := packetIO.WritePacket(data, false); err != nil {
			return err
		}
		for _, field := range rs.Fields {
			if err := packetIO.WritePacket(field.Dump(), false); err != nil {
				return err
			}
		}
		if mb.exitInResult {
			if err := packetIO.Flush(); err != nil {
				return err
			}
			return packetIO.Close()
		}

		if status&mysql.SERVER_STATUS_CURSOR_EXISTS == 0 {
			if mb.capability&pnet.ClientDeprecateEOF == 0 {
				if err := packetIO.WriteEOFPacket(status); err != nil {
					return err
				}
			}
			for _, row := range values {
				var data []byte
				for _, value := range row {
					data = pnet.DumpLengthEncodedString(data, []byte(value.(string)))
				}
				if err := packetIO.WritePacket(data, false); err != nil {
					return err
				}
			}
		}
		if err := mb.writeResultEndPacket(packetIO, status); err != nil {
			return err
		}
	}
	return nil
}

// respond to LoadInFile
func (mb *mockBackend) respondLoadFile(packetIO *pnet.PacketIO) error {
	for i := 0; i < mb.stmtNum; i++ {
		status := mb.status
		if i < mb.stmtNum-1 {
			status |= mysql.SERVER_MORE_RESULTS_EXISTS
		} else {
			status &= ^mysql.SERVER_MORE_RESULTS_EXISTS
		}
		data := make([]byte, 0, 1+len(mockCmdStr))
		data = append(data, pnet.LocalInFileHeader.Byte())
		data = append(data, []byte(mockCmdStr)...)
		if err := packetIO.WritePacket(data, true); err != nil {
			return err
		}
		for {
			// read file data
			pkt, err := packetIO.ReadPacket()
			if err != nil {
				return err
			}
			// An empty packet indicates the end of file.
			if len(pkt) == 0 {
				break
			}
		}
		if err := packetIO.WriteOKPacket(status, pnet.OKHeader); err != nil {
			return err
		}
	}
	return nil
}

// respond to Prepare
func (mb *mockBackend) respondPrepare(packetIO *pnet.PacketIO) error {
	data := []byte{pnet.OKHeader.Byte()}
	data = pnet.DumpUint32(data, uint32(mockCmdInt))
	data = pnet.DumpUint16(data, uint16(mb.columns))
	data = pnet.DumpUint16(data, uint16(mb.params))
	data = append(data, 0x00)
	data = pnet.DumpUint16(data, uint16(mockCmdInt))
	if err := packetIO.WritePacket(data, true); err != nil {
		return err
	}
	if mb.params > 0 {
		for i := 0; i < mb.params; i++ {
			if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
				return err
			}
		}
		if mb.capability&pnet.ClientDeprecateEOF == 0 {
			if err := packetIO.WriteEOFPacket(mb.status); err != nil {
				return err
			}
		}
	}
	if mb.columns > 0 {
		for i := 0; i < mb.columns; i++ {
			if err := packetIO.WritePacket(mockCmdBytes, false); err != nil {
				return err
			}
		}
		if mb.capability&pnet.ClientDeprecateEOF == 0 {
			if err := packetIO.WriteEOFPacket(mb.status); err != nil {
				return err
			}
		}
	}
	return packetIO.Flush()
}

func (mb *mockBackend) respondSessionStates(packetIO *pnet.PacketIO) error {
	names := []string{sessionStatesCol, sessionTokenCol}
	values := [][]any{
		{
			mb.sessionStates, mockSessionToken,
		},
	}
	return mb.writeResultSet(packetIO, names, values)
}
