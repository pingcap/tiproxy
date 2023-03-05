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
	"net"
	"sync"
	"time"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
	"github.com/pingcap/TiProxy/lib/util/waitgroup"
	"github.com/pingcap/TiProxy/pkg/manager/cert"
	"github.com/pingcap/TiProxy/pkg/metrics"
	"github.com/pingcap/TiProxy/pkg/proxy/backend"
	"github.com/pingcap/TiProxy/pkg/proxy/client"
	"github.com/pingcap/TiProxy/pkg/proxy/keepalive"
	pnet "github.com/pingcap/TiProxy/pkg/proxy/net"
	"go.uber.org/zap"
)

type serverState struct {
	sync.RWMutex
	healthyKeepAlive   config.KeepAlive
	unhealthyKeepAlive config.KeepAlive
	clients            map[uint64]*client.ClientConnection
	connID             uint64
	maxConnections     uint64
	tcpKeepAlive       bool
	proxyProtocol      bool
	gracefulWait       int
	inShutdown         bool
}

type SQLServer struct {
	listener          net.Listener
	logger            *zap.Logger
	certMgr           *cert.CertManager
	hsHandler         backend.HandshakeHandler
	requireBackendTLS bool
	wg                waitgroup.WaitGroup
	cancelFunc        context.CancelFunc

	mu serverState
}

// NewSQLServer creates a new SQLServer.
func NewSQLServer(logger *zap.Logger, cfg config.ProxyServer, certMgr *cert.CertManager, hsHandler backend.HandshakeHandler) (*SQLServer, error) {
	var err error

	s := &SQLServer{
		logger:            logger,
		certMgr:           certMgr,
		hsHandler:         hsHandler,
		requireBackendTLS: cfg.RequireBackendTLS,
		mu: serverState{
			connID:  0,
			clients: make(map[uint64]*client.ClientConnection),
		},
	}

	s.reset(&cfg.ProxyServerOnline)

	s.listener, err = net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SQLServer) reset(cfg *config.ProxyServerOnline) {
	s.mu.Lock()
	s.mu.tcpKeepAlive = cfg.FrontendKeepalive.Enabled
	s.mu.maxConnections = cfg.MaxConnections
	s.mu.proxyProtocol = cfg.ProxyProtocol != ""
	s.mu.gracefulWait = cfg.GracefulWaitBeforeShutdown
	s.mu.healthyKeepAlive = cfg.BackendHealthyKeepalive
	s.mu.unhealthyKeepAlive = cfg.BackendUnhealthyKeepalive
	s.mu.Unlock()
}

func (s *SQLServer) Run(ctx context.Context, cfgch <-chan *config.Config) {
	// Create another context because it still needs to run after graceful shutdown.
	ctx, s.cancelFunc = context.WithCancel(context.Background())

	s.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ach := <-cfgch:
				if ach == nil {
					// prevent panic on closing chan
					return
				}
				s.reset(&ach.Proxy.ProxyServerOnline)
			}
		}
	})

	s.wg.Run(func() {
		for {
			select {
			case <-ctx.Done():
				return
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
	})
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
	if s.mu.inShutdown {
		s.mu.Unlock()
		s.logger.Warn("in shutdown", zap.String("addr", conn.RemoteAddr().Network()), zap.Error(conn.Close()))
		return
	}

	connID := s.mu.connID
	s.mu.connID++
	logger := s.logger.With(zap.Uint64("connID", connID), zap.String("remoteAddr", conn.RemoteAddr().String()))
	clientConn := client.NewClientConnection(logger.Named("conn"), conn, s.certMgr.ServerTLS(), s.certMgr.SQLTLS(),
		s.hsHandler, connID, &backend.BCConfig{
			ProxyProtocol:      s.mu.proxyProtocol,
			RequireBackendTLS:  s.requireBackendTLS,
			HealthyKeepAlive:   s.mu.healthyKeepAlive,
			UnhealthyKeepAlive: s.mu.unhealthyKeepAlive,
		})
	s.mu.clients[connID] = clientConn
	s.mu.Unlock()

	logger.Info("new connection")
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

	if err := keepalive.SetKeepalive(conn, config.KeepAlive{Enabled: tcpKeepAlive}); err != nil {
		logger.Warn("failed to set tcp keep alive option", zap.Error(err))
	}

	clientConn.Run(ctx)
}

// Graceful shutdown doesn't close the listener but rejects new connections.
// Whether this affects NLB is to be tested.
func (s *SQLServer) gracefulShutdown() {
	s.mu.Lock()
	gracefulWait := s.mu.gracefulWait
	if gracefulWait == 0 {
		s.mu.Unlock()
		return
	}
	s.mu.inShutdown = true
	for _, conn := range s.mu.clients {
		conn.GracefulClose()
	}
	s.mu.Unlock()
	s.logger.Info("SQL server is shutting down", zap.Int("graceful_wait", gracefulWait))

	timer := time.NewTimer(time.Duration(gracefulWait) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case <-time.After(100 * time.Millisecond):
			s.mu.Lock()
			allClosed := len(s.mu.clients) == 0
			s.mu.Unlock()
			if allClosed {
				return
			}
		}
	}
}

// Close closes the server.
func (s *SQLServer) Close() error {
	s.gracefulShutdown()

	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}
	errs := make([]error, 0, 4)
	if s.listener != nil {
		errs = append(errs, s.listener.Close())
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
