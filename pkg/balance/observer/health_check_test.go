// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestReadServerVersion(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hc := NewDefaultHealthCheck(nil, newHealthCheckConfigForTest(), lg)
	backend, info := newBackendServer(t)
	backend.setServerVersion("1.0")
	health := hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.Equal(t, "1.0", health.ServerVersion)
	backend.stopSQLServer()
	backend.setServerVersion("2.0")
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.Equal(t, "2.0", health.ServerVersion)
	backend.stopSQLServer()

	//test for respBody not ok
	backend.mockHttpHandler.setStatusRespBody("")
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.False(t, health.Healthy)
	require.NotNil(t, health.PingErr)
	require.Equal(t, true, strings.Contains(health.PingErr.Error(), "unexpected end of JSON input"))

	backend.close()
}

// Test that the backend status is correct when the backend starts or shuts down.
func TestHealthCheck(t *testing.T) {
	lg, text := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	hc := NewDefaultHealthCheck(nil, cfg, lg)
	backend, info := newBackendServer(t)
	backend.setServerVersion("1.0")
	backend.setHasSigningCert(true)
	health := hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.True(t, health.Healthy)

	backend.stopSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.False(t, health.Healthy)
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.True(t, health.Healthy)
	backend.setHTTPResp(false)

	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.False(t, health.Healthy)
	require.NotContains(t, text.String(), "unmarshal body")
	require.True(t, health.SupportRedirection)
	backend.setHTTPResp(true)
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.True(t, health.Healthy)

	backend.setHTTPWait(time.Second + cfg.DialTimeout)
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.False(t, health.Healthy)
	backend.setHTTPWait(time.Duration(0))
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.True(t, health.Healthy)

	backend.setSqlResp(false)
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.False(t, health.Healthy)
	require.True(t, health.SupportRedirection)
	backend.setSqlResp(true)
	health = hc.Check(context.Background(), backend.sqlAddr, info, nil)
	require.True(t, health.Healthy)

	require.True(t, health.SupportRedirection)
	health.lastCheckSigningCertTime = time.Time{}
	health = hc.Check(context.Background(), backend.sqlAddr, info, health)
	require.True(t, health.SupportRedirection)
	backend.setHasSigningCert(false)
	health.lastCheckSigningCertTime = time.Time{}
	health = hc.Check(context.Background(), backend.sqlAddr, info, health)
	require.False(t, health.SupportRedirection)
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
	sqlResp    atomic.Bool
}

func newBackendServer(t *testing.T) (*backendServer, *BackendInfo) {
	backend := &backendServer{
		t: t,
	}
	backend.startHTTPServer()
	backend.setHTTPResp(true)
	backend.setStatusRespBody("")
	backend.setSqlResp(true)
	backend.startSQLServer()
	return backend, &BackendInfo{
		IP:         backend.ip,
		StatusPort: backend.statusPort,
	}
}
func (srv *backendServer) setServerVersion(version string) {
	resp := backendHttpStatusRespBody{
		Connections: 0,
		Version:     version,
		GitHash:     "",
	}
	body, _ := json.Marshal(resp)
	srv.mockHttpHandler.setStatusRespBody(string(body))
}

func (srv *backendServer) startHTTPServer() {
	if srv.mockHttpHandler == nil {
		srv.mockHttpHandler = &mockHttpHandler{
			t: srv.t,
		}
	}
	var statusListener net.Listener
	statusListener, srv.statusAddr = testkit.StartListener(srv.t, srv.statusAddr)
	srv.ip, srv.statusPort = testkit.ParseHostPort(srv.t, srv.statusAddr)
	srv.statusServer = &http.Server{Addr: srv.statusAddr, Handler: srv.mockHttpHandler}
	srv.wg.Run(func() {
		_ = srv.statusServer.Serve(statusListener)
	})
}

func (srv *backendServer) stopHTTPServer() {
	err := srv.statusServer.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) setSqlResp(sqlResp bool) {
	srv.sqlResp.Store(sqlResp)
}

func (srv *backendServer) startSQLServer() {
	srv.sqlListener, srv.sqlAddr = testkit.StartListener(srv.t, srv.sqlAddr)
	srv.wg.Run(func() {
		for {
			conn, err := srv.sqlListener.Accept()
			if err != nil {
				// listener is closed
				break
			}
			if srv.sqlResp.Load() {
				data := []byte{0, 0, 0, 0, 0}
				c := packet.NewConn(conn)
				require.NoError(srv.t, c.WritePacket(data))
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
