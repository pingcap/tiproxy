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
	"crypto/tls"
	"net"
	"strings"
	"testing"

	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"github.com/pingcap/TiProxy/pkg/util/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

// TestSuite creates a sql client and a sql server which connect to the proxy with TCP connection.
// Both the mocked client and server act like a real client and server: they parse data and
// respond to the proxy.
// We can test various client capabilities, auth-plugins, and TLS configs using the mocked client
// and server. We create an M^N combinations of the possibilities to test.
// We test the correctness by checking whether the data received by the client equals to the data
// sent from the server and vice versa.

const (
	defaultBackendCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
		mysql.ClientConnectWithDB | mysql.ClientProtocol41 | mysql.ClientSSL |
		mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
		mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
		mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive
	defaultClientCapability = defaultBackendCapability
)

var (
	mockUsername = "test_user"
	mockDBName   = "test_db"
	mockSalt     = []byte("01234567890123456789")
	mockAuthData = []byte("123456")
	mockToken    = strings.Repeat("t", 512)
)

type testConfig struct {
	clientConfig  clientConfig
	proxyConfig   proxyConfig
	backendConfig backendConfig
}

type clientConfig struct {
	tlsConfig  *tls.Config
	capability uint32
	username   string
	dbName     string
	collation  uint8
	authPlugin string
	authData   []byte
	attrs      []byte
}

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	sessionToken      string
}

type backendConfig struct {
	tlsConfig   *tls.Config
	capability  uint32
	salt        []byte
	authPlugin  string
	switchAuth  bool
	authSucceed bool
}

type cfgOverrider func(config *testConfig)

// Get a combination of M^N configurations to test.
func getCfgCombinations(cfgs [][]cfgOverrider) [][]cfgOverrider {
	cfgOverriders := make([][]cfgOverrider, 0)
	for _, cfgList := range cfgs {
		if len(cfgOverriders) == 0 {
			cfgOverriders = make([][]cfgOverrider, 0, len(cfgList))
			for _, cfg := range cfgList {
				cfgOverriders = append(cfgOverriders, []cfgOverrider{cfg})
			}
		} else {
			newOverriders := make([][]cfgOverrider, 0, len(cfgOverriders)*len(cfgList))
			// Append the cfg to each of the existing overrider list.
			for _, cfg := range cfgList {
				for _, o := range cfgOverriders {
					newOverrider := make([]cfgOverrider, 0, len(o)+1)
					newOverrider = append(newOverrider, o...)
					newOverrider = append(newOverrider, cfg)
					newOverriders = append(newOverriders, newOverrider)
				}
			}
			cfgOverriders = newOverriders
		}
	}
	return cfgOverriders
}

func newTestConfig(overriders ...cfgOverrider) *testConfig {
	cfg := &testConfig{
		clientConfig: clientConfig{
			capability: defaultClientCapability,
			username:   mockUsername,
			dbName:     mockDBName,
			authPlugin: mysql.AuthCachingSha2Password,
			authData:   mockAuthData,
			attrs:      make([]byte, 0),
		},
		proxyConfig: proxyConfig{
			sessionToken: mockToken,
		},
		backendConfig: backendConfig{
			capability:  defaultBackendCapability,
			salt:        mockSalt,
			authPlugin:  mysql.AuthCachingSha2Password,
			switchAuth:  true,
			authSucceed: true,
		},
	}
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(cfg)
		}
	}
	return cfg
}

func (cfg *testConfig) setTLSConfig(clientTLSConfig, backendTLSConfig *tls.Config) {
	cfg.clientConfig.tlsConfig = clientTLSConfig
	cfg.proxyConfig.frontendTLSConfig = backendTLSConfig
	cfg.proxyConfig.backendTLSConfig = clientTLSConfig
	cfg.backendConfig.tlsConfig = backendTLSConfig
}

type testSuite struct {
	t  *testing.T
	mb *mockBackend
	mp *mockProxy
	mc *mockClient
}

func newTestSuite(t *testing.T, cfg *testConfig) *testSuite {
	return &testSuite{
		t:  t,
		mb: newMockBackend(&cfg.backendConfig),
		mp: newMockProxy(cfg),
		mc: newMockClient(&cfg.clientConfig),
	}
}

func (ts *testSuite) changeDB(db string) {
	ts.mc.dbName = db
	ts.mp.auth.updateCurrentDB(db)
}

func (ts *testSuite) changeUser(username, db string) {
	ts.mc.username = username
	ts.mc.dbName = db
	ts.mp.auth.changeUser(username, db)
}

func (ts *testSuite) close() {
	require.NoError(ts.t, ts.mc.close())
	require.NoError(ts.t, ts.mp.close())
	require.NoError(ts.t, ts.mb.close())
}

type tcpClient struct {
	*pnet.PacketIO
}

func (c *tcpClient) connect(addr string) error {
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		return errors.New("dial error")
	}
	c.PacketIO = pnet.NewPacketIO(cn)
	return nil
}

func (c *tcpClient) close() error {
	if c.PacketIO != nil {
		err := c.PacketIO.Close()
		c.PacketIO = nil
		return err
	}
	return nil
}

type tcpServer struct {
	*pnet.PacketIO
	listener net.Listener
}

func (s *tcpServer) listen() error {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *tcpServer) accept() error {
	conn, err := s.listener.Accept()
	if err != nil {
		return err
	}
	s.PacketIO = pnet.NewPacketIO(conn)
	return nil
}

func (s *tcpServer) serverAddr() string {
	return s.listener.Addr().String()
}

func (s *tcpServer) close() error {
	var err1, err2 error
	if s.PacketIO != nil {
		err1 = s.PacketIO.Close()
		s.PacketIO = nil
	}
	if s.listener != nil {
		err2 = s.listener.Close()
		s.listener = nil
	}
	if err1 != nil {
		return err1
	}
	return err2
}

type mockClient struct {
	tcpClient
	// Inputs that assigned by the test and will be sent to the server.
	*clientConfig
	// Outputs that received from the server and will be checked by the test.
	authSucceed bool
}

func newMockClient(cfg *clientConfig) *mockClient {
	return &mockClient{
		clientConfig: cfg,
	}
}

type mockProxy struct {
	tcpServer
	tcpClient
	*proxyConfig
	auth *Authenticator
}

func newMockProxy(cfg *testConfig) *mockProxy {
	return &mockProxy{
		proxyConfig: &cfg.proxyConfig,
		auth:        new(Authenticator),
	}
}

func (mp *mockProxy) close() error {
	err1 := mp.tcpClient.close()
	err2 := mp.tcpServer.close()
	if err1 != nil {
		return err1
	}
	return err2
}

type mockBackend struct {
	tcpServer
	// Inputs that assigned by the test and will be sent to the client.
	*backendConfig
	// Outputs that received from the client and will be checked by the test.
	username string
	authData []byte
	db       string
	attrs    []byte
}

func newMockBackend(cfg *backendConfig) *mockBackend {
	return &mockBackend{
		backendConfig: cfg,
	}
}
