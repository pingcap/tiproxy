// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestReadServerVersion(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hc := NewDefaultHealthCheck(nil, newHealthCheckConfigForTest(), lg)
	backend, info := newBackendServer(t)
	backend.mockHttpHandler.respBodyOK.Store(true)
	backend.serverVersion.Store("1.0")
	health := hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, "1.0", health.ServerVersion)
	backend.stopSQLServer()
	backend.serverVersion.Store("2.0")
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, "2.0", health.ServerVersion)
	backend.stopSQLServer()

	//test for respBody not ok
	backend.mockHttpHandler.respBodyOK.Store(false)
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusCannotConnect, health.Status)
	require.NotNil(t, health.PingErr)
	require.Equal(t, true, strings.Contains(health.PingErr.Error(), "unexpected end of JSON input"))

	backend.close()

}

// Test that the backend status is correct when the backend starts or shuts down.
func TestHealthCheck(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	hc := NewDefaultHealthCheck(nil, cfg, lg)
	backend, info := newBackendServer(t)
	backend.mockHttpHandler.respBodyOK.Store(true)
	health := hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusHealthy, health.Status)

	backend.stopSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusCannotConnect, health.Status)
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusHealthy, health.Status)

	backend.setHTTPResp(false)
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusCannotConnect, health.Status)
	backend.setHTTPResp(true)
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusHealthy, health.Status)

	backend.setHTTPWait(time.Second + cfg.DialTimeout)
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusCannotConnect, health.Status)
	backend.setHTTPWait(time.Duration(0))
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.Equal(t, StatusHealthy, health.Status)

	backend.close()
}

type backendServer struct {
	t            *testing.T
	sqlListener  net.Listener
	sqlAddr      string
	statusServer *http.Server
	statusAddr   string
	*mockHttpHandler
	wg         waitgroup.WaitGroup
	ip         string
	statusPort uint
}

func newBackendServer(t *testing.T) (*backendServer, *BackendInfo) {
	backend := &backendServer{
		t: t,
	}
	backend.startHTTPServer()
	backend.setHTTPResp(true)
	backend.startSQLServer()
	return backend, &BackendInfo{
		IP:         backend.ip,
		StatusPort: backend.statusPort,
	}
}

type mockHttpHandler struct {
	t             *testing.T
	httpOK        atomic.Bool
	respBodyOK    atomic.Bool
	wait          atomic.Int64
	serverVersion atomic.String
}

func (handler *mockHttpHandler) setHTTPResp(succeed bool) {
	handler.httpOK.Store(succeed)
}

func (handler *mockHttpHandler) setHTTPWait(wait time.Duration) {
	handler.wait.Store(int64(wait))
}

func (handler *mockHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wait := handler.wait.Load()
	if wait > 0 {
		time.Sleep(time.Duration(wait))
	}
	if handler.httpOK.Load() {
		w.WriteHeader(http.StatusOK)
		if handler.respBodyOK.Load() {
			resp := backendHttpStatusRespBody{
				Connections: 0,
				Version:     handler.serverVersion.Load(),
				GitHash:     "",
			}
			body, _ := json.Marshal(resp)
			_, _ = w.Write(body)
		}
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (srv *backendServer) startHTTPServer() {
	if srv.mockHttpHandler == nil {
		srv.mockHttpHandler = &mockHttpHandler{
			t: srv.t,
		}
	}
	var statusListener net.Listener
	statusListener, srv.statusAddr = startListener(srv.t, srv.statusAddr)
	srv.ip, srv.statusPort = parseHostPort(srv.t, srv.statusAddr)
	srv.statusServer = &http.Server{Addr: srv.statusAddr, Handler: srv.mockHttpHandler}
	srv.wg.Run(func() {
		_ = srv.statusServer.Serve(statusListener)
	})
}

func (srv *backendServer) stopHTTPServer() {
	err := srv.statusServer.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) startSQLServer() {
	srv.sqlListener, srv.sqlAddr = startListener(srv.t, srv.sqlAddr)
	srv.wg.Run(func() {
		for {
			conn, err := srv.sqlListener.Accept()
			if err != nil {
				// listener is closed
				break
			}
			if err = pnet.WriteServerVersion(conn, srv.serverVersion.Load()); err != nil {
				break
			}
			_ = conn.Close()
		}
	})
}

func (srv *backendServer) stopSQLServer() {
	err := srv.sqlListener.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) close() {
	srv.stopHTTPServer()
	srv.stopSQLServer()
	srv.wg.Wait()
}

func startListener(t *testing.T, addr string) (net.Listener, string) {
	if len(addr) == 0 {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return listener, listener.Addr().String()
}

func parseHostPort(t *testing.T, addr string) (string, uint) {
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	p, err := strconv.ParseUint(port, 10, 32)
	require.NoError(t, err)
	return host, uint(p)
}
