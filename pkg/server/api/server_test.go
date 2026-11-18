// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/manager/health"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type httpOpts struct {
	reader io.Reader
	header map[string]string
}

type doHTTPFunc func(t *testing.T, method string, path string, opts httpOpts, f func(*testing.T, *http.Response))

func createServer(t *testing.T) (*Server, doHTTPFunc) {
	srv, doHTTP, _ := createServerWithConfig(t, "")
	return srv, doHTTP
}

func createServerWithConfig(t *testing.T, tomlConfig string) (*Server, doHTTPFunc, doHTTPFunc) {
	lg, _ := logger.CreateLoggerForTest(t)
	ready := atomic.NewBool(true)
	cfgmgr := mgrcfg.NewConfigManager()
	require.NoError(t, cfgmgr.Init(context.Background(), "", ""))
	if tomlConfig != "" {
		require.NoError(t, cfgmgr.SetTOMLConfig([]byte(tomlConfig)))
	}
	crtmgr := mgrcrt.NewCertManager()
	require.NoError(t, crtmgr.Init(cfgmgr.GetConfig(), lg, cfgmgr.WatchConfig()))
	nsMgr := newMockNamespaceManager()
	healthMgr := health.NewManager(nsMgr.Ready, func() (bool, string) { return false, "" })
	srv, err := NewServer(config.API{
		Addr: "0.0.0.0:0",
	}, lg, Managers{
		CfgMgr:        cfgmgr,
		NsMgr:         nsMgr,
		CertMgr:       crtmgr,
		BackendReader: &mockBackendReader{},
		ReplayJobMgr:  &mockReplayJobManager{},
		Health:        healthMgr,
	}, nil, ready)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})

	addr := srv.listener.Addr().String()
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	do := func(scheme string, client *http.Client) doHTTPFunc {
		return func(t *testing.T, method, pa string, opts httpOpts, f func(*testing.T, *http.Response)) {
			if pa[0] != '/' {
				pa = "/" + pa
			}
			req, err := http.NewRequest(method, fmt.Sprintf("%s://%s%s", scheme, addr, pa), opts.reader)
			require.NoError(t, err)
			for key, value := range opts.header {
				req.Header.Set(key, value)
			}
			resp, err := client.Do(req)
			require.NoError(t, err)
			f(t, resp)
			require.NoError(t, resp.Body.Close())
		}
	}
	return srv, do("http", http.DefaultClient), do("https", httpsClient)
}

func TestGrpc(t *testing.T) {
	srv, _ := createServer(t)
	addr := srv.listener.Addr().String()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, cc.Close())
}

func TestAttachLogger(t *testing.T) {
	tests := []struct {
		name          string
		method        string
		path          string
		handlerError  bool
		status        int
		expectedLevel *zapcore.Level
	}{
		{
			name:   "get health",
			method: http.MethodGet,
			path:   "/api/debug/health",
		},
		{
			name:   "head",
			method: http.MethodHead,
			path:   "/api/test",
		},
		{
			name:   "options",
			method: http.MethodOptions,
			path:   "/api/test",
		},
		{
			name:          "post",
			method:        http.MethodPost,
			path:          "/api/debug/redirect",
			expectedLevel: levelPtr(zap.InfoLevel),
		},
		{
			name:          "put config",
			method:        http.MethodPut,
			path:          "/api/admin/config",
			expectedLevel: levelPtr(zap.InfoLevel),
		},
		{
			name:          "patch",
			method:        http.MethodPatch,
			path:          "/api/test",
			expectedLevel: levelPtr(zap.InfoLevel),
		},
		{
			name:          "delete",
			method:        http.MethodDelete,
			path:          "/api/admin/namespace/test",
			expectedLevel: levelPtr(zap.InfoLevel),
		},
		{
			name:          "read error",
			method:        http.MethodGet,
			path:          "/api/admin/namespace/test",
			handlerError:  true,
			expectedLevel: levelPtr(zap.WarnLevel),
		},
		{
			name:          "write error",
			method:        http.MethodPost,
			path:          "/api/test",
			handlerError:  true,
			expectedLevel: levelPtr(zap.WarnLevel),
		},
		{
			name:          "read error status",
			method:        http.MethodGet,
			path:          "/api/debug/health",
			status:        http.StatusBadGateway,
			expectedLevel: levelPtr(zap.WarnLevel),
		},
		{
			name:          "write error status",
			method:        http.MethodPost,
			path:          "/api/traffic/replay",
			status:        http.StatusBadRequest,
			expectedLevel: levelPtr(zap.WarnLevel),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.DebugLevel)
			h := &Server{lg: zap.New(core)}
			engine := gin.New()
			engine.Use(h.attachLogger)
			engine.Handle(tt.method, tt.path, func(c *gin.Context) {
				if tt.handlerError {
					_ = c.Error(errors.New("test error"))
				}
				status := tt.status
				if status == 0 {
					status = http.StatusOK
				}
				c.Status(status)
			})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, nil)
			request.RemoteAddr = "192.0.2.1:1234"
			request.Header.Set("X-Forwarded-For", "203.0.113.1")
			engine.ServeHTTP(recorder, request)

			entries := logs.All()
			if tt.expectedLevel == nil {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, 1)
			require.Equal(t, *tt.expectedLevel, entries[0].Level)
			require.Equal(t, tt.path, entries[0].Message)
			require.Equal(t, tt.method, entries[0].ContextMap()["method"])
			require.Equal(t, "192.0.2.1:1234", entries[0].ContextMap()["remote-addr"])
		})
	}
}

func levelPtr(level zapcore.Level) *zapcore.Level {
	return &level
}
