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

package sqlserver

import (
	"context"
	"crypto/tls"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/proxy/driver"
	"github.com/pingcap/TiProxy/pkg/util/security"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	errConCount = dbterror.ClassServer.NewStd(errno.ErrConCount)

	timeWheelUnit       = time.Second * 1
	timeWheelBucketsNum = 3600
)

type SQLServer struct {
	cfg              *config.Proxy
	serverTLSConfig  *tls.Config // the TLS used to connect to the client
	clusterTLSConfig *tls.Config // the TLS used to connect to PD and TiDB server
	driver           driver.IDriver
	listener         net.Listener
	rwlock           sync.RWMutex
	clients          map[uint64]driver.ClientConnection
	baseConnID       uint64
}

// NewSQLServer creates a new Server.
func NewSQLServer(cfg *config.Proxy, d driver.IDriver) (*SQLServer, error) {
	var err error

	s := &SQLServer{
		cfg:     cfg,
		driver:  d,
		clients: make(map[uint64]driver.ClientConnection),
	}

	if s.serverTLSConfig, err = security.CreateServerTLSConfig(cfg.Security.SSLCA, cfg.Security.SSLKey, cfg.Security.SSLCert,
		cfg.Security.MinTLSVersion, cfg.ProxyServer.StoragePath, cfg.Security.RSAKeySize); err != nil {
		return nil, err
	}
	if s.clusterTLSConfig, err = security.CreateClientTLSConfig(cfg.Security.ClusterSSLCA, cfg.Security.ClusterSSLKey,
		cfg.Security.ClusterSSLCert); err != nil {
		return nil, err
	}

	if err := s.initListener(); err != nil {
		return nil, err
	}

	// TODO(eastfisher): init status http server

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())

	return s, nil
}

// TODO(eastfisher): support unix socket and proxy protocol
func (s *SQLServer) initListener() error {
	listener, err := net.Listen("tcp", s.cfg.ProxyServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *SQLServer) Run(ctx context.Context) error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// TODO(eastfisher): startStatusHTTP()

	for {
		select {
		case <-ctx.Done():
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok {
					if opErr.Err.Error() == "use of closed network connection" {
						return nil
					}
				}

				// TODO(eastfisher): support PROXY protocol
				logutil.BgLogger().Error("accept failed", zap.Error(err))
				return errors.Trace(err)
			}

			if err = s.checkConnectionCount(); err != nil {
				return err
			}
			clientConn := s.newConn(conn)
			go s.onConn(ctx, clientConn)
		}
	}
}

// ConnectionCount gets current connection count.
func (s *SQLServer) ConnectionCount() int {
	s.rwlock.RLock()
	cnt := len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *SQLServer) onConn(ctx context.Context, conn driver.ClientConnection) {
	ctx = logutil.WithConnID(ctx, conn.ConnectionID())
	logutil.Logger(ctx).Info("new connection", zap.String("remoteAddr", conn.Addr()))

	defer func() {
		logutil.Logger(ctx).Info("connection closed")
	}()

	s.rwlock.Lock()
	s.clients[conn.ConnectionID()] = conn
	connections := len(s.clients)
	s.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))

	conn.Run(ctx)

	s.rwlock.Lock()
	delete(s.clients, conn.ConnectionID())
	connections = len(s.clients)
	s.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))
}

func (s *SQLServer) newConn(conn net.Conn) driver.ClientConnection {
	if s.cfg.ProxyServer.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logutil.BgLogger().Error("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}
	connectionID := atomic.AddUint64(&s.baseConnID, 1)
	return s.driver.CreateClientConnection(conn, connectionID, s.serverTLSConfig, s.clusterTLSConfig)
}

func (s *SQLServer) checkConnectionCount() error {
	// When the value of MaxConnections is 0, the number of connections is unlimited.
	if int(s.cfg.ProxyServer.MaxConnections) == 0 {
		return nil
	}

	s.rwlock.RLock()
	conns := len(s.clients)
	s.rwlock.RUnlock()

	if conns >= int(s.cfg.ProxyServer.MaxConnections) {
		logutil.BgLogger().Error("too many connections",
			zap.Uint32("max connections", s.cfg.ProxyServer.MaxConnections), zap.Error(errConCount))
		return errConCount
	}
	return nil
}

// TODO(eastfisher): implement this function
func (s *SQLServer) isUnixSocket() bool {
	return false
}

// Close closes the server.
// TODO(eastfisher): implement this function, close unix socket, status server, and gRPC server.
func (s *SQLServer) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	for _, conn := range s.clients {
		err := conn.Close()
		terror.Log(errors.Trace(err))
	}
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}
