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

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cert

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"encoding/pem"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/logger"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

// Test that the certs are automatically created and reloaded.
func TestReloadCerts(t *testing.T) {
	dir := t.TempDir()
	lg := logger.CreateLoggerForTest(t)
	sqlCfg := &config.TLSConfig{AutoCerts: true}
	err := security.AutoTLS(lg, sqlCfg, true, dir, "sql", 0)
	require.NoError(t, err)
	clusterCfg := &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 0)
	require.NoError(t, err)

	cfg := &config.Config{
		Workdir: dir,
		Security: config.Security{
			ServerTLS: config.TLSConfig{
				AutoCerts: true,
			},
			PeerTLS: config.TLSConfig{
				AutoCerts: true,
			},
			SQLTLS:     *sqlCfg,
			ClusterTLS: *clusterCfg,
		},
	}
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(100 * time.Millisecond)
	certMgr.SetAutoCertInterval(50 * time.Millisecond)
	err = certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	areCertsDifferent := func(before, after [4][][]byte) bool {
		for i := 0; i < 4; i++ {
			if len(before[i]) != len(after[i]) {
				continue
			}
			if before[i] == nil || after[i] == nil {
				continue
			}
			different := false
			for j := 0; j < len(before[i]); j++ {
				if !bytes.Equal(before[i][j], after[i][j]) {
					different = true
					break
				}
			}
			if !different {
				return false
			}
		}
		return true
	}

	var before = getAllCertificates(t, certMgr)
	sqlCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, sqlCfg, true, dir, "sql", 0)
	require.NoError(t, err)
	clusterCfg = &config.TLSConfig{AutoCerts: true}
	err = security.AutoTLS(lg, clusterCfg, true, dir, "cluster", 0)
	require.NoError(t, err)

	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timer.C:
			t.Fatal("timeout")
		case <-time.After(100 * time.Millisecond):
			var after = getAllCertificates(t, certMgr)
			if areCertsDifferent(before, after) {
				return
			}
		}
	}
}

func getRawCertificate(t *testing.T, ci *certInfo) [][]byte {
	tlsConfig := ci.getTLS()
	if tlsConfig == nil {
		return nil
	}
	var cert *tls.Certificate
	var err error
	if ci.isServer {
		cert, err = tlsConfig.GetCertificate(nil)
	} else {
		cert, err = tlsConfig.GetClientCertificate(nil)
	}
	require.NoError(t, err)
	return cert.Certificate
}

func getAllCertificates(t *testing.T, certMgr *CertManager) [4][][]byte {
	return [4][][]byte{
		getRawCertificate(t, &certMgr.serverTLS),
		getRawCertificate(t, &certMgr.peerTLS),
		getRawCertificate(t, &certMgr.sqlTLS),
		getRawCertificate(t, &certMgr.clusterTLS),
	}
}

// Test that configuring no certs still works.
func TestReloadEmptyCerts(t *testing.T) {
	lg := logger.CreateLoggerForTest(t)
	cfg := &config.Config{}
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(100 * time.Millisecond)
	err := certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	rawCerts := getAllCertificates(t, certMgr)
	for i := 0; i < len(rawCerts); i++ {
		require.Nil(t, rawCerts[i])
	}
}

// Test that rotating CA works.
func TestRotateCACert(t *testing.T) {
	dir := t.TempDir()
	lg := logger.CreateLoggerForTest(t)
	sqlCfg := &config.TLSConfig{AutoCerts: true, SkipCA: true}
	err := security.AutoTLS(lg, sqlCfg, true, dir, "sql", 0)
	require.NoError(t, err)
	serverCfg := &config.TLSConfig{AutoCerts: true, SkipCA: true}
	err = security.AutoTLS(lg, serverCfg, true, dir, "server", 0)
	require.NoError(t, err)

	cfg := &config.Config{
		Workdir: dir,
		Security: config.Security{
			ServerTLS: *serverCfg,
			SQLTLS:    *sqlCfg,
		},
	}
	certMgr := NewCertManager()
	certMgr.SetRetryInterval(100 * time.Millisecond)
	err = certMgr.Init(cfg, lg)
	require.NoError(t, err)
	t.Cleanup(certMgr.Close)

	// Connecting with fake CA should not work.
	stls := certMgr.ServerTLS()
	ctls := certMgr.SQLTLS()
	clientErr, serverErr := connectWithTLS(ctls, stls, "")
	require.ErrorContains(t, clientErr, "certificate signed by unknown authority")
	require.Error(t, serverErr)

	// Overwrite the cert files with right CA and it works with the same tls.Config.
	overrideCerts(t, sqlCfg)
	overrideCerts(t, serverCfg)
	timer := time.NewTimer(3 * time.Second)
	for {
		select {
		case <-timer.C:
			t.Fatal("timeout")
		case <-time.After(100 * time.Millisecond):
			clientErr, serverErr := connectWithTLS(ctls, stls, "example.golang")
			if clientErr == nil && serverErr == nil {
				return
			}
		}
	}
}

func connectWithTLS(ctls, stls *tls.Config, serverName string) (clientErr, serverErr error) {
	client, server := net.Pipe()
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		ctls.ServerName = serverName
		tlsConn := tls.Client(client, ctls)
		clientErr = tlsConn.Handshake()
		_ = client.Close()
	})
	wg.Run(func() {
		tlsConn := tls.Server(server, stls)
		serverErr = tlsConn.Handshake()
		_ = server.Close()
	})
	wg.Wait()
	return
}

func fromHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func overrideCerts(t *testing.T, cfg *config.TLSConfig) {
	certPEM, keyPEM, caPEM := getPEMBytes()
	err := os.WriteFile(cfg.Cert, certPEM, 0600)
	require.NoError(t, err)
	err = os.WriteFile(cfg.Key, keyPEM, 0600)
	require.NoError(t, err)
	err = os.WriteFile(cfg.CA, caPEM, 0600)
	require.NoError(t, err)
}

func getPEMBytes() ([]byte, []byte, []byte) {
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: testRSACertificate,
	})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: testRSAPrivateKey,
	})
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: testRSACertificateIssuer,
	})
	return certPEM, keyPEM, caPEM
}

var testRSACertificate = fromHex("3082024b308201b4a003020102020900e8f09d3fe25beaa6300d06092a864886f70d01010b0500301f310b3009060355040a1302476f3110300e06035504031307476f20526f6f74301e170d3136303130313030303030305a170d3235303130313030303030305a301a310b3009060355040a1302476f310b300906035504031302476f30819f300d06092a864886f70d010101050003818d0030818902818100db467d932e12270648bc062821ab7ec4b6a25dfe1e5245887a3647a5080d92425bc281c0be97799840fb4f6d14fd2b138bc2a52e67d8d4099ed62238b74a0b74732bc234f1d193e596d9747bf3589f6c613cc0b041d4d92b2b2423775b1c3bbd755dce2054cfa163871d1e24c4f31d1a508baab61443ed97a77562f414c852d70203010001a38193308190300e0603551d0f0101ff0404030205a0301d0603551d250416301406082b0601050507030106082b06010505070302300c0603551d130101ff0402300030190603551d0e041204109f91161f43433e49a6de6db680d79f60301b0603551d230414301280104813494d137e1631bba301d5acab6e7b30190603551d1104123010820e6578616d706c652e676f6c616e67300d06092a864886f70d01010b0500038181009d30cc402b5b50a061cbbae55358e1ed8328a9581aa938a495a1ac315a1a84663d43d32dd90bf297dfd320643892243a00bccf9c7db74020015faad3166109a276fd13c3cce10c5ceeb18782f16c04ed73bbb343778d0c1cf10fa1d8408361c94c722b9daedb4606064df4c1b33ec0d1bd42d4dbfe3d1360845c21d33be9fae7")
var testRSACertificateIssuer = fromHex("3082021930820182a003020102020900ca5e4e811a965964300d06092a864886f70d01010b0500301f310b3009060355040a1302476f3110300e06035504031307476f20526f6f74301e170d3136303130313030303030305a170d3235303130313030303030305a301f310b3009060355040a1302476f3110300e06035504031307476f20526f6f7430819f300d06092a864886f70d010101050003818d0030818902818100d667b378bb22f34143b6cd2008236abefaf2852adf3ab05e01329e2c14834f5105df3f3073f99dab5442d45ee5f8f57b0111c8cb682fbb719a86944eebfffef3406206d898b8c1b1887797c9c5006547bb8f00e694b7a063f10839f269f2c34fff7a1f4b21fbcd6bfdfb13ac792d1d11f277b5c5b48600992203059f2a8f8cc50203010001a35d305b300e0603551d0f0101ff040403020204301d0603551d250416301406082b0601050507030106082b06010505070302300f0603551d130101ff040530030101ff30190603551d0e041204104813494d137e1631bba301d5acab6e7b300d06092a864886f70d01010b050003818100c1154b4bab5266221f293766ae4138899bd4c5e36b13cee670ceeaa4cbdf4f6679017e2fe649765af545749fe4249418a56bd38a04b81e261f5ce86b8d5c65413156a50d12449554748c59a30c515bc36a59d38bddf51173e899820b282e40aa78c806526fd184fb6b4cf186ec728edffa585440d2b3225325f7ab580e87dd76")
var testRSAPrivateKey = fromHex("3082025b02010002818100db467d932e12270648bc062821ab7ec4b6a25dfe1e5245887a3647a5080d92425bc281c0be97799840fb4f6d14fd2b138bc2a52e67d8d4099ed62238b74a0b74732bc234f1d193e596d9747bf3589f6c613cc0b041d4d92b2b2423775b1c3bbd755dce2054cfa163871d1e24c4f31d1a508baab61443ed97a77562f414c852d702030100010281800b07fbcf48b50f1388db34b016298b8217f2092a7c9a04f77db6775a3d1279b62ee9951f7e371e9de33f015aea80660760b3951dc589a9f925ed7de13e8f520e1ccbc7498ce78e7fab6d59582c2386cc07ed688212a576ff37833bd5943483b5554d15a0b9b4010ed9bf09f207e7e9805f649240ed6c1256ed75ab7cd56d9671024100fded810da442775f5923debae4ac758390a032a16598d62f059bb2e781a9c2f41bfa015c209f966513fe3bf5a58717cbdb385100de914f88d649b7d15309fa49024100dd10978c623463a1802c52f012cfa72ff5d901f25a2292446552c2568b1840e49a312e127217c2186615aae4fb6602a4f6ebf3f3d160f3b3ad04c592f65ae41f02400c69062ca781841a09de41ed7a6d9f54adc5d693a2c6847949d9e1358555c9ac6a8d9e71653ac77beb2d3abaf7bb1183aa14278956575dbebf525d0482fd72d90240560fe1900ba36dae3022115fd952f2399fb28e2975a1c3e3d0b679660bdcb356cc189d611cfdd6d87cd5aea45aa30a2082e8b51e94c2f3dd5d5c6036a8a615ed0240143993d80ece56f877cb80048335701eb0e608cc0c1ca8c2227b52edf8f1ac99c562f2541b5ce81f0515af1c5b4770dba53383964b4b725ff46fdec3d08907df")
