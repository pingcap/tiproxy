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

package proxy

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/security"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	mgrns "github.com/pingcap/TiProxy/pkg/manager/namespace"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	"github.com/pingcap/TiProxy/pkg/proxy/client"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type serverState struct {
	sync.RWMutex
	clients        map[uint64]*client.ClientConnection
	connID         uint64
	maxConnections uint64
	tcpKeepAlive   bool
}

type SQLServer struct {
	listener          net.Listener
	logger            *zap.Logger
	nsmgr             *mgrns.NamespaceManager
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	wg                waitgroup.WaitGroup

	mu serverState
}

// NewSQLServer creates a new SQLServer.
func NewSQLServer(logger *zap.Logger, cfg config.ProxyServer, scfg config.Security, nsmgr *mgrns.NamespaceManager) (*SQLServer, error) {
	var err error

	s := &SQLServer{
		logger: logger,
		nsmgr:  nsmgr,
		mu: serverState{
			tcpKeepAlive:   cfg.TCPKeepAlive,
			maxConnections: cfg.MaxConnections,
			connID:         0,
			clients:        make(map[uint64]*client.ClientConnection),
		},
	}

	if s.frontendTLSConfig, err = security.BuildServerTLSConfig(logger, scfg.ServerTLS); err != nil {
		return nil, err
	}
	if s.backendTLSConfig, err = security.BuildClientTLSConfig(logger, scfg.SQLTLS); err != nil {
		return nil, err
	}

	s.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SQLServer) Run(ctx context.Context, onlineProxyConfig <-chan *config.ProxyServerOnline) {
	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			return
		case och := <-onlineProxyConfig:
			s.mu.Lock()
			s.mu.tcpKeepAlive = och.TCPKeepAlive
			s.mu.maxConnections = och.MaxConnections
			s.mu.Unlock()
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}

				s.logger.Error("accept failed", zap.Error(err))
				continue
			}

			s.wg.Run(func() {
				s.onConn(ctx, conn)
			})
		}
	}
}

func (s *SQLServer) onConn(ctx context.Context, conn net.Conn) {
	s.mu.Lock()
	conns := uint64(len(s.mu.clients))
	maxConns := s.mu.maxConnections
	tcpKeepAlive := s.mu.tcpKeepAlive

	// 'maxConns == 0' => unlimited connections
	if maxConns != 0 && conns >= maxConns {
		s.mu.Unlock()
		s.logger.Warn("too many connections", zap.Uint64("max connections", maxConns), zap.String("addr", conn.RemoteAddr().Network()), zap.Error(conn.Close()))
		return
	}

	connID := s.mu.connID
	s.mu.connID++
	logger := s.logger.With(zap.Uint64("connID", connID))
	clientConn := client.NewClientConnection(logger.Named("cliconn"), conn, s.frontendTLSConfig, s.backendTLSConfig, s.nsmgr, backend.NewBackendConnManager(logger.Named("bemgr"), connID))
	s.mu.clients[connID] = clientConn
	s.mu.Unlock()

	logger.Info("new connection", zap.String("remoteAddr", conn.RemoteAddr().String()))
	metrics.ConnGauge.Inc()

	defer func() {
		s.mu.Lock()
		delete(s.mu.clients, connID)
		s.mu.Unlock()

		if err := clientConn.Close(); err != nil && !pnet.IsDisconnectError(err) {
			logger.Error("close connection fails", zap.Error(err))
		} else {
			logger.Info("connection closed")
		}
		metrics.ConnGauge.Dec()
	}()

	if tcpKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				logger.Warn("failed to set tcp keep alive option", zap.Error(err))
			}
		}
	}

	clientConn.Run(ctx)
}

// Close closes the server.
func (s *SQLServer) Close() error {
	var errs []error
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	s.mu.Lock()
	for _, conn := range s.mu.clients {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.mu.Unlock()

	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
