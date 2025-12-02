// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/id"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
	"github.com/pingcap/tiproxy/pkg/proxy/client"
	"github.com/pingcap/tiproxy/pkg/proxy/keepalive"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/capture"
	"github.com/pingcap/tiproxy/pkg/util/netutil"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

type serverState struct {
	sync.RWMutex
	healthyKeepAlive   config.KeepAlive
	unhealthyKeepAlive config.KeepAlive
	clients            map[uint64]*client.ClientConnection
	publicEndpoints    []*net.IPNet
	maxConnections     uint64
	connBufferSize     int
	requireBackendTLS  bool
	tcpKeepAlive       bool
	proxyProtocol      bool
	gracefulWait       int // graceful-wait-before-shutdown
	gracefulClose      int // graceful-close-conn-timeout
}

type SQLServer struct {
	listeners  []net.Listener
	addrs      []string
	logger     *zap.Logger
	certMgr    *cert.CertManager
	idMgr      *id.IDManager
	hsHandler  backend.HandshakeHandler
	cpt        capture.Capture
	meter      backend.Meter
	wg         waitgroup.WaitGroup
	cancelFunc context.CancelFunc

	mu serverState
}

// NewSQLServer creates a new SQLServer.
func NewSQLServer(logger *zap.Logger, cfg *config.Config, certMgr *cert.CertManager, idMgr *id.IDManager, cpt capture.Capture,
	meter backend.Meter, hsHandler backend.HandshakeHandler) (*SQLServer, error) {
	var err error
	s := &SQLServer{
		logger:    logger,
		certMgr:   certMgr,
		idMgr:     idMgr,
		hsHandler: hsHandler,
		cpt:       cpt,
		meter:     meter,
		mu: serverState{
			clients: make(map[uint64]*client.ClientConnection),
		},
	}

	s.reset(cfg)

	s.addrs = strings.Split(cfg.Proxy.Addr, ",")
	s.listeners = make([]net.Listener, len(s.addrs))
	for i, addr := range s.addrs {
		s.listeners[i], err = net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *SQLServer) reset(cfg *config.Config) {
	cidrList, parseErr := netutil.ParseCIDRList(cfg.Proxy.PublicEndpoints)
	if parseErr != nil {
		s.logger.Warn("failed to parse public endpoints", zap.Error(parseErr))
	}
	s.mu.Lock()
	s.mu.tcpKeepAlive = cfg.Proxy.FrontendKeepalive.Enabled
	s.mu.maxConnections = cfg.Proxy.MaxConnections
	s.mu.requireBackendTLS = cfg.Security.RequireBackendTLS
	s.mu.proxyProtocol = cfg.Proxy.ProxyProtocol != ""
	s.mu.gracefulWait = cfg.Proxy.GracefulWaitBeforeShutdown
	s.mu.gracefulClose = cfg.Proxy.GracefulCloseConnTimeout
	s.mu.healthyKeepAlive = cfg.Proxy.BackendHealthyKeepalive
	s.mu.unhealthyKeepAlive = cfg.Proxy.BackendUnhealthyKeepalive
	s.mu.connBufferSize = cfg.Proxy.ConnBufferSize
	s.mu.publicEndpoints = cidrList
	s.mu.Unlock()
}

func (s *SQLServer) Run(ctx context.Context, cfgch <-chan *config.Config) {
	// Create another context because it still needs to run after graceful shutdown.
	ctx, s.cancelFunc = context.WithCancel(context.Background())

	s.wg.RunWithRecover(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ach := <-cfgch:
				if ach == nil {
					// prevent panic on closing chan
					return
				}
				s.reset(ach)
			}
		}
	}, nil, s.logger)

	for i := range s.listeners {
		j := i
		s.wg.Run(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					conn, err := s.listeners[j].Accept()
					if err != nil {
						if errors.Is(err, net.ErrClosed) {
							return
						}

						s.logger.Error("accept failed", zap.Error(err))
						continue
					}

					s.wg.RunWithRecover(func() { s.onConn(ctx, conn, s.addrs[j]) }, nil, s.logger)
				}
			}
		}, s.logger)
	}
}

func (s *SQLServer) onConn(ctx context.Context, conn net.Conn, addr string) {
	tcpKeepAlive, logger, connID, clientConn := func() (bool, *zap.Logger, uint64, *client.ClientConnection) {
		s.mu.Lock()
		defer s.mu.Unlock()

		conns := uint64(len(s.mu.clients))
		maxConns := s.mu.maxConnections
		// 'maxConns == 0' => unlimited connections
		if maxConns != 0 && conns >= maxConns {
			s.logger.Warn("too many connections", zap.Uint64("max connections", maxConns), zap.Stringer("client_addr", conn.RemoteAddr()), zap.Error(conn.Close()))
			return false, nil, 0, nil
		}

		connID := s.idMgr.NewID()
		logger := s.logger.With(zap.Uint64("connID", connID), zap.String("client_addr", conn.RemoteAddr().String()),
			zap.String("addr", addr))
		clientConn := client.NewClientConnection(logger.Named("conn"), conn, s.certMgr.ServerSQLTLS(), s.certMgr.SQLTLS(),
			s.hsHandler, s.cpt, connID, addr, &backend.BCConfig{
				ProxyProtocol:      s.mu.proxyProtocol,
				RequireBackendTLS:  s.mu.requireBackendTLS,
				HealthyKeepAlive:   s.mu.healthyKeepAlive,
				UnhealthyKeepAlive: s.mu.unhealthyKeepAlive,
				ConnBufferSize:     s.mu.connBufferSize,
			}, s.meter)
		s.mu.clients[connID] = clientConn
		logger.Debug("new connection", zap.Bool("proxy-protocol", s.mu.proxyProtocol), zap.Bool("require_backend_tls", s.mu.requireBackendTLS))
		return s.mu.tcpKeepAlive, logger, connID, clientConn
	}()

	if clientConn == nil {
		return
	}

	metrics.ConnGauge.Inc()
	metrics.CreateConnCounter.Inc()

	defer func() {
		s.mu.Lock()
		delete(s.mu.clients, connID)
		s.mu.Unlock()

		if err := clientConn.Close(); err != nil && !pnet.IsDisconnectError(err) {
			logger.Error("close connection fails", zap.Error(err))
		} else {
			logger.Debug("connection closed")
		}
		metrics.ConnGauge.Dec()
	}()

	if err := keepalive.SetKeepalive(conn, config.KeepAlive{Enabled: tcpKeepAlive}); err != nil {
		logger.Warn("failed to set tcp keep alive option", zap.Error(err))
	}

	clientConn.Run(ctx)
}

func (s *SQLServer) fromPublicEndpoint(addr net.Addr) bool {
	if addr == nil || reflect.ValueOf(addr).IsNil() {
		return false
	}
	s.mu.RLock()
	publicEndpoints := s.mu.publicEndpoints
	s.mu.RUnlock()
	ip, err := netutil.NetAddr2IP(addr)
	if err != nil {
		s.logger.Warn("failed to check public endpoint", zap.Any("addr", addr), zap.Error(err))
		return false
	}
	contains, err := netutil.CIDRContainsIP(publicEndpoints, ip)
	if err != nil {
		s.logger.Warn("failed to check public endpoint", zap.Any("ip", ip), zap.Error(err))
		return false
	}
	if contains {
		return true
	}
	// The public NLB may enable preserveIP, and the incoming address is the client address, which may be a public address.
	// Even if the private NLB enables preserveIP, the client address is still a private address.
	return !netutil.IsPrivate(ip)
}

func (s *SQLServer) PreClose() {
	// Step 1: HTTP status returns unhealthy so that NLB takes this instance offline and then new connections won't come.
	s.mu.Lock()
	gracefulWait := s.mu.gracefulWait
	s.mu.Unlock()
	s.logger.Info("SQL server prepares for shutdown", zap.Int("graceful_wait", gracefulWait))
	if gracefulWait > 0 {
		time.Sleep(time.Duration(gracefulWait) * time.Second)
	}

	// Step 2: reject new connections
	for i := range s.listeners {
		if err := s.listeners[i].Close(); err != nil {
			s.logger.Warn("closing listener fails", zap.Error(err))
		}
	}

	// Step 3: gracefully waiting for connections to finish the current transactions
	s.mu.Lock()
	gracefulClose := s.mu.gracefulClose
	s.logger.Info("SQL server is shutting down", zap.Int("graceful_close", gracefulClose), zap.Int("conn_count", len(s.mu.clients)))
	if gracefulClose <= 0 {
		s.mu.Unlock()
		return
	}
	for _, conn := range s.mu.clients {
		conn.GracefulClose()
	}
	s.mu.Unlock()

	timer := time.NewTimer(time.Duration(gracefulClose) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case <-time.After(100 * time.Millisecond):
			s.mu.RLock()
			allClosed := len(s.mu.clients) == 0
			s.mu.RUnlock()
			if allClosed {
				return
			}
		}
	}
}

// Close closes the server.
func (s *SQLServer) Close() error {
	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}

	s.mu.RLock()
	s.logger.Info("force closing connections", zap.Int("conn_count", len(s.mu.clients)))
	for _, conn := range s.mu.clients {
		if err := conn.Close(); err != nil {
			s.logger.Warn("close connection error", zap.Error(err))
		}
	}
	s.mu.RUnlock()

	s.wg.Wait()
	return nil
}
