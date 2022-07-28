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

package net

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/pkg/util/waitgroup"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func testConn(t *testing.T, a func(*testing.T, *PacketIO), b func(*testing.T, *PacketIO)) {
	var wg waitgroup.WaitGroup
	client, server := net.Pipe()
	cli, srv := NewPacketIO(client), NewPacketIO(server)
	wg.Run(func() {
		a(t, cli)
		require.NoError(t, cli.Close())
	})
	wg.Run(func() {
		b(t, srv)
		require.NoError(t, srv.Close())
	})
	wg.Wait()
}

func TestPacketIO(t *testing.T) {
	expectMsg := []byte("test")
	testConn(t,
		func(t *testing.T, cli *PacketIO) {
			var err error

			// send anything
			require.NoError(t, cli.WritePacket(expectMsg, true))

			// send more than max payload
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen+212), true))
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen), true))
			require.NoError(t, cli.WritePacket(make([]byte, mysql.MaxPayloadLen*2), true))

			// skip handshake
			_, err = cli.ReadPacket()
			require.NoError(t, err)

			// send correct and wrong capability flags
			var hdr [4]byte
			binary.LittleEndian.PutUint16(hdr[:], uint16(mysql.ClientSSL))
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)

			binary.LittleEndian.PutUint16(hdr[:], 0)
			err = cli.WritePacket(hdr[:], true)
			require.NoError(t, err)
		},
		func(t *testing.T, srv *PacketIO) {
			var salt [8]byte
			var msg []byte
			var err error

			// receive "test"
			msg, err = srv.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, msg, expectMsg)

			// receive more than max payload
			_, err = srv.ReadPacket()
			require.NoError(t, err)
			_, err = srv.ReadPacket()
			require.NoError(t, err)
			_, err = srv.ReadPacket()
			require.NoError(t, err)

			// send handshake
			require.NoError(t, srv.WriteInitialHandshake(0, 0, salt[:]))

			// expect correct and wrong capability flags
			_, err = srv.ReadSSLRequest()
			require.NoError(t, err)
			_, err = srv.ReadSSLRequest()
			require.ErrorIs(t, err, ErrExpectSSLRequest)
		},
	)
}

// certsetup is from https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251.
func certsetup() (serverTLSConf *tls.Config, clientTLSConf *tls.Config, err error) {
	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	serverCert, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	if err != nil {
		return nil, nil, err
	}

	serverTLSConf = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM.Bytes())
	clientTLSConf = &tls.Config{
		RootCAs: certpool,
	}

	return
}

func TestTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	var wg waitgroup.WaitGroup
	stls, ctls, err := certsetup()
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		wg.Run(func() {
			srv, err := listener.Accept()
			require.NoError(t, err)

			srvIO := NewPacketIO(srv)
			err = srvIO.WritePacket([]byte("hello"), true)
			require.NoError(t, err)

			_, err = srvIO.UpgradeToServerTLS(stls)
			require.NoError(t, err)

			data, err := srvIO.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte("world"), data)
		})
		wg.Run(func() {
			cli, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)

			cliIO := NewPacketIO(cli)
			data, err := cliIO.ReadPacket()
			require.NoError(t, err)
			require.Equal(t, []byte("hello"), data)

			require.NoError(t, cliIO.UpgradeToClientTLS(ctls))

			err = cliIO.WritePacket([]byte("world"), true)
			require.NoError(t, err)
		})
		wg.Wait()
	}
}
