// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/djshow832/weir/pkg/util/security"
	"github.com/djshow832/weir/pkg/util/timer"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errConCount = dbterror.ClassServer.NewStd(errno.ErrConCount)

	timeWheelUnit       = time.Second * 1
	timeWheelBucketsNum = 3600
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth | mysql.ClientInteractive

type Server struct {
	cfg            *config.Proxy
	tlsConfig      *tls.Config // the tls used to connect to the client
	driver         driver.IDriver
	listener       net.Listener
	rwlock         sync.RWMutex
	clients        map[uint64]driver.ClientConnection
	baseConnID     uint64
	capability     uint32
	sessionTimeout time.Duration
	tw             *timer.TimeWheel
}

// NewServer creates a new Server.
func NewServer(cfg *config.Proxy, d driver.IDriver) (*Server, error) {
	tw, err := timer.NewTimeWheel(timeWheelUnit, timeWheelBucketsNum)
	if err != nil {
		return nil, err
	}

	tw.Start()

	s := &Server{
		cfg:            cfg,
		driver:         d,
		clients:        make(map[uint64]driver.ClientConnection),
		sessionTimeout: time.Duration(cfg.ProxyServer.SessionTimeout) * time.Second,
		tw:             tw,
	}

	s.tlsConfig, err = security.CreateServerTLSConfig(cfg.Security.SSLCA, cfg.Security.SSLKey, cfg.Security.SSLCert,
		cfg.Security.MinTLSVersion, cfg.ProxyServer.StoragePath, cfg.Security.RSAKeySize)
	if err != nil {
		return nil, err
	}

	setSystemTimeZoneVariable()

	s.initCapability()

	if err := s.initListener(); err != nil {
		return nil, err
	}

	// TODO(eastfisher): init status http server

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())

	return s, nil
}

func (s *Server) initCapability() {
	s.capability = defaultCapability
}

// TODO(eastfisher): support unix socket and proxy protocol
func (s *Server) initListener() error {
	listener, err := net.Listen("tcp", s.cfg.ProxyServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *Server) Run() error {
	metrics.ServerEventCounter.WithLabelValues(metrics.EventStart).Inc()

	// TODO(eastfisher): startStatusHTTP()

	for {
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
		go s.onConn(clientConn)
	}
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	s.rwlock.RLock()
	cnt := len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) onConn(conn driver.ClientConnection) {
	ctx := logutil.WithConnID(context.Background(), conn.ConnectionID())
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
}

func (s *Server) newConn(conn net.Conn) driver.ClientConnection {
	if s.cfg.Performance.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logutil.BgLogger().Error("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}
	connectionID := atomic.AddUint64(&s.baseConnID, 1)
	return s.driver.CreateClientConnection(conn, connectionID, s.tlsConfig)
}

func (s *Server) checkConnectionCount() error {
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
func (s *Server) isUnixSocket() bool {
	return false
}

// Close closes the server.
// TODO(eastfisher): implement this function, close unix socket, status server, and gRPC server.
func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	metrics.ServerEventCounter.WithLabelValues(metrics.EventClose).Inc()
}

// TryGracefulDown will try to gracefully close all connection first with timeout. if timeout, will close all connection directly.
func (s *Server) TryGracefulDown() {
	return
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown(ctx context.Context, done chan struct{}) {
	return
}
